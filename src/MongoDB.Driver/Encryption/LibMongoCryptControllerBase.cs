/* Copyright 2019-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Libmongocrypt;

namespace MongoDB.Driver.Encryption
{
    internal abstract class LibMongoCryptControllerBase
    {
        // protected fields
        protected readonly CryptClient _cryptClient;
        protected readonly IMongoClient _keyVaultClient;
        protected readonly Lazy<IMongoCollection<BsonDocument>> _keyVaultCollection;
        protected readonly CollectionNamespace _keyVaultNamespace;

        // constructors
        protected LibMongoCryptControllerBase(
             CryptClient cryptClient,
             IMongoClient keyVaultClient,
             CollectionNamespace keyVaultNamespace)
        {
            _cryptClient = cryptClient;
            _keyVaultClient = keyVaultClient; // _keyVaultClient might not be fully constructed at this point, don't call any instance methods on it yet
            _keyVaultNamespace = keyVaultNamespace;
            _keyVaultCollection = new Lazy<IMongoCollection<BsonDocument>>(GetKeyVaultCollection); // delay use _keyVaultClient
        }

        // protected methods
        protected void FeedResult(CryptContext context, BsonDocument document)
        {
#pragma warning disable 618
            var writerSettings = new BsonBinaryWriterSettings();
            if (BsonDefaults.GuidRepresentationMode == GuidRepresentationMode.V2)
            {
                writerSettings.GuidRepresentation = GuidRepresentation.Unspecified;
            }
#pragma warning restore 618
            var documentBytes = document.ToBson(writerSettings: writerSettings);
            context.Feed(documentBytes);
            context.MarkDone();
        }

        protected void FeedResults(CryptContext context, IEnumerable<BsonDocument> documents)
        {
#pragma warning disable 618
            var writerSettings = new BsonBinaryWriterSettings();
            if (BsonDefaults.GuidRepresentationMode == GuidRepresentationMode.V2)
            {
                writerSettings.GuidRepresentation = GuidRepresentation.Unspecified;
            }
#pragma warning restore 618
            foreach (var document in documents)
            {
                var documentBytes = document.ToBson(writerSettings: writerSettings);
                context.Feed(documentBytes);
            }
            context.MarkDone();
        }

        protected virtual void ProcessState(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            switch (context.State)
            {
                case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_KMS:
                    ProcessNeedKmsState(context, cancellationToken);
                    break;
                case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_KEYS:
                    ProcessNeedMongoKeysState(context, cancellationToken);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected context state: {context.State}.");
            }
        }

        protected virtual async Task ProcessStateAsync(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            switch (context.State)
            {
                case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_KMS:
                    await ProcessNeedKmsStateAsync(context, cancellationToken).ConfigureAwait(false);
                    break;
                case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_KEYS:
                    await ProcessNeedMongoKeysStateAsync(context, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected context state: {context.State}.");
            }
        }

        protected byte[] ProcessStates(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            byte[] result = null;
            while (context.State != CryptContext.StateCode.MONGOCRYPT_CTX_DONE)
            {
                if (context.State == CryptContext.StateCode.MONGOCRYPT_CTX_READY)
                {
                    result = ProcessReadyState(context);
                }
                else
                {
                    ProcessState(context, databaseName, cancellationToken);
                }
            }
            return result;
        }

        protected async Task<byte[]> ProcessStatesAsync(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            byte[] result = null;
            while (context.State != CryptContext.StateCode.MONGOCRYPT_CTX_DONE)
            {
                if (context.State == CryptContext.StateCode.MONGOCRYPT_CTX_READY)
                {
                    result = ProcessReadyState(context);
                }
                else
                {
                    await ProcessStateAsync(context, databaseName, cancellationToken).ConfigureAwait(false);
                }
            }
            return result;
        }

        // private methods
        private IMongoCollection<BsonDocument> GetKeyVaultCollection()
        {
            var keyVaultDatabase = _keyVaultClient.GetDatabase(_keyVaultNamespace.DatabaseNamespace.DatabaseName);

            var collectionSettings = new MongoCollectionSettings
            {
                ReadConcern = ReadConcern.Majority,
                WriteConcern = WriteConcern.WMajority
            };
            return keyVaultDatabase.GetCollection<BsonDocument>(_keyVaultNamespace.CollectionName, collectionSettings);
        }

        private void ParseKmsEndPoint(string value, out string host, out int port)
        {
            var match = Regex.Match(value, @"^(?<host>.*):(?<port>\d+)$");
            if (match.Success)
            {
                host = match.Groups["host"].Value;
                port = int.Parse(match.Groups["port"].Value);
            }
            else
            {
                host = value;
                port = 443;
            }
        }

        private void ProcessNeedKmsState(CryptContext context, CancellationToken cancellationToken)
        {
            var requests = context.GetKmsMessageRequests();
            foreach (var request in requests)
            {
                SendKmsRequest(request, cancellationToken);
            }
            requests.MarkDone();
        }

        private async Task ProcessNeedKmsStateAsync(CryptContext context, CancellationToken cancellationToken)
        {
            var requests = context.GetKmsMessageRequests();
            foreach (var request in requests)
            {
                await SendKmsRequestAsync(request, cancellationToken).ConfigureAwait(false);
            }
            requests.MarkDone();
        }

        private void ProcessNeedMongoKeysState(CryptContext context, CancellationToken cancellationToken)
        {
            var filterBytes = context.GetOperation().ToArray();
            var filterDocument = new RawBsonDocument(filterBytes);
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
            var cursor = _keyVaultCollection.Value.FindSync(filter, cancellationToken: cancellationToken);
            var results = cursor.ToList(cancellationToken);
            FeedResults(context, results);
        }

        private async Task ProcessNeedMongoKeysStateAsync(CryptContext context, CancellationToken cancellationToken)
        {
            var filterBytes = context.GetOperation().ToArray();
            var filterDocument = new RawBsonDocument(filterBytes);
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
            var cursor = await _keyVaultCollection.Value.FindAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
            var results = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);
            FeedResults(context, results);
        }

        private byte[] ProcessReadyState(CryptContext context)
        {
            return context.FinalizeForEncryption().ToArray();
        }

        private void SendKmsRequest(KmsRequest request, CancellationToken cancellation)
        {
            ParseKmsEndPoint(request.Endpoint, out var host, out var port);

            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            SocketHelper.ResolvedAndConnect(socket, new DnsEndPoint(host, port));

            using (var networkStream = new NetworkStream(socket, ownsSocket: true))
            using (var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false))
            {
#if NETSTANDARD1_5
                sslStream.AuthenticateAsClientAsync(host).ConfigureAwait(false).GetAwaiter().GetResult();
#else
                sslStream.AuthenticateAsClient(host);
#endif

                var requestBytes = request.Message.ToArray();
                sslStream.Write(requestBytes);

                while (request.BytesNeeded > 0)
                {
                    var buffer = new byte[request.BytesNeeded]; // BytesNeeded is the maximum number of bytes that libmongocrypt wants to receive.
                    var count = sslStream.Read(buffer, 0, buffer.Length);
                    var responseBytes = new byte[count];
                    Buffer.BlockCopy(buffer, 0, responseBytes, 0, count);
                    request.Feed(responseBytes);
                }
            }
            socket.Dispose();
        }

        private async Task SendKmsRequestAsync(KmsRequest request, CancellationToken cancellation)
        {
            ParseKmsEndPoint(request.Endpoint, out var host, out var port);

            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await SocketHelper.ResolveAndConnectAsync(socket, new DnsEndPoint(host, port)).ConfigureAwait(false);

            using (var networkStream = new NetworkStream(socket, ownsSocket: true))
            using (var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false))
            {
                await sslStream.AuthenticateAsClientAsync(host).ConfigureAwait(false);

                var requestBytes = request.Message.ToArray();
                await sslStream.WriteAsync(requestBytes, 0, requestBytes.Length).ConfigureAwait(false);

                while (request.BytesNeeded > 0)
                {
                    var buffer = new byte[request.BytesNeeded]; // BytesNeeded is the maximum number of bytes that libmongocrypt wants to receive.
                    var count = await sslStream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
                    var responseBytes = new byte[count];
                    Buffer.BlockCopy(buffer, 0, responseBytes, 0, count);
                    request.Feed(responseBytes);
                }
            }
            socket.Dispose();
        }

        // nested type
        private static class SocketHelper
        {
            public static void ResolvedAndConnect(Socket socket, EndPoint endPoint)
            {
#if NETSTANDARD1_5
                var resolved = ResolveEndPointsAsync(endPoint).GetAwaiter().GetResult();
                for (int i = 0; i < resolved.Length; i++)
                {
                    try
                    {
                        Connect(socket, resolved[i]);
                        return;
                    }
                    catch
                    {
                        // if we have tried all of them and still failed,
                        // then blow up.
                        if (i == resolved.Length - 1)
                        {
                            throw;
                        }
                    }
                }
#else
                Connect(socket, endPoint);
#endif
            }

            public static async Task ResolveAndConnectAsync(Socket socket, EndPoint endPoint)
            {
#if NETSTANDARD1_5
                var resolved = await ResolveEndPointsAsync(endPoint).ConfigureAwait(false);
                for (int i = 0; i < resolved.Length; i++)
                {
                    try
                    {
                        await ConnectAsync(socket, resolved[i]).ConfigureAwait(false);
                        return;
                    }
                    catch
                    {
                        // if we have tried all of them and still failed,
                        // then blow up.
                        if (i == resolved.Length - 1)
                        {
                            throw;
                        }
                    }
                }
#else
                await ConnectAsync(socket, endPoint).ConfigureAwait(false);
#endif
            }

            // private methods
            private static void Connect(Socket socket, EndPoint endPoint)
            {
                var dnsEndPoint = endPoint as DnsEndPoint;
                if (dnsEndPoint != null)
                {
                    // mono doesn't support DnsEndPoint in its BeginConnect method.
                    socket.Connect(dnsEndPoint.Host, dnsEndPoint.Port);
                }
                else
                {
                    var ip = (IPEndPoint)endPoint;
                    socket.Connect(ip.Address, ip.Port);
                }
            }

            private static async Task ConnectAsync(Socket socket, EndPoint endPoint)
            {
                var dnsEndPoint = endPoint as DnsEndPoint;
#if NET452
                if (dnsEndPoint != null)
                {
                    // mono doesn't support DnsEndPoint in its BeginConnect method.
                    await Task.Factory.FromAsync(socket.BeginConnect(dnsEndPoint.Host, dnsEndPoint.Port, null, null), socket.EndConnect).ConfigureAwait(false);
                }
                else
                {
                    await Task.Factory.FromAsync(socket.BeginConnect(endPoint, null, null), socket.EndConnect).ConfigureAwait(false);
                }
#else
                await socket.ConnectAsync(endPoint).ConfigureAwait(false);
#endif
            }

            private static async Task<EndPoint[]> ResolveEndPointsAsync(EndPoint initial)
            {
                var dnsInitial = initial as DnsEndPoint;
                if (dnsInitial == null)
                {
                    return new[] { initial };
                }

                IPAddress address;
                if (IPAddress.TryParse(dnsInitial.Host, out address))
                {
                    return new[] { new IPEndPoint(address, dnsInitial.Port) };
                }

                var preferred = initial.AddressFamily;
                return (await Dns.GetHostAddressesAsync(dnsInitial.Host).ConfigureAwait(false))
                    .Select(x => new IPEndPoint(x, dnsInitial.Port))
                    .OrderBy(x => x, new PreferredAddressFamilyComparer(preferred))
                    .ToArray();
            }

            // nested types
            private class PreferredAddressFamilyComparer : IComparer<EndPoint>
            {
                private readonly AddressFamily _preferred;

                public PreferredAddressFamilyComparer(AddressFamily preferred)
                {
                    _preferred = preferred;
                }

                public int Compare(EndPoint x, EndPoint y)
                {
                    if (x.AddressFamily == y.AddressFamily)
                    {
                        return 0;
                    }
                    if (x.AddressFamily == _preferred)
                    {
                        return -1;
                    }
                    else if (y.AddressFamily == _preferred)
                    {
                        return 1;
                    }

                    return 0;
                }
            }
        }
    }
}
