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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Crypt;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol;

namespace MongoDB.Driver.LibMongoCrypt
{
    /// <summary>
    /// TODO
    /// </summary>
    internal interface IEncryptionSource
    {
        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="options">TODO</param>
        CryptClient Get(CryptOptions options);
    }

    internal class EncryptionSource : IEncryptionSource
    {
        private readonly ConcurrentDictionary<CryptOptions, CryptClient> _cryptClientCache;

        public EncryptionSource()
        {
            _cryptClientCache = new ConcurrentDictionary<CryptOptions, CryptClient>(new CryptClientComparer());
        }

        public CryptClient Get(CryptOptions options)
        {
            return _cryptClientCache.GetOrAdd(options, CreateCryptClient);
        }

        // private methods
        private CryptClient CreateCryptClient(CryptOptions options)
        {
            return CryptClientFactory.Create(options);
        }

        private class CryptClientComparer : IEqualityComparer<CryptOptions>
        {
            public bool Equals(CryptOptions x, CryptOptions y)
            {
                if (x == null && y == null)
                {
                    return true;
                }
                else if (x == null || y == null)
                {
                    return false;
                }
                else
                {
                    // todo
                    var res = (x.Schema ?? new byte[0]).SequenceEqual(y.Schema ?? new byte[0]) && x.KmsCredentials.KmsType == y.KmsCredentials.KmsType;
                    return res;
                }
            }

            public int GetHashCode(CryptOptions obj)
            {
                // todo: check that it's enough
                //return obj.GetHashCode();
                return 1;
            }
        }
    }

    internal class LibMongoCryptController : IBinaryDocumentFieldDecryptor, IBinaryDocumentFieldEncryptor
    {
        private static LibMongoCryptController __instance;

        // private fields
        private readonly AutoEncryptionOptions _autoEncryptionOptions;
        private readonly MongoClient _client;
        private readonly IMongoClient _mongocryptdClient;
        private readonly IMongoCollection<BsonDocument> _keyVaultCollection;

        // constructors
        private LibMongoCryptController(
            IMongoClient client,
            AutoEncryptionOptions autoEncryptionOptions)
        {
            _client = Ensure.IsNotNull(client, nameof(client)) as MongoClient;
            _autoEncryptionOptions = Ensure.IsNotNull(autoEncryptionOptions, nameof(autoEncryptionOptions));
            _mongocryptdClient = client.CryptClientD; // TODO: launch mongocryptd and create a client for it

            var keyVaultClient = autoEncryptionOptions.KeyVaultClient ?? client;
            var keyVaultNamespace = autoEncryptionOptions.KeyVaultNamespace;
            var keyVaultDatabase = keyVaultClient.GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName);
            _keyVaultCollection = keyVaultDatabase.GetCollection<BsonDocument>(keyVaultNamespace.CollectionName);
        }

        //todo: refactor
        public static LibMongoCryptController GetInstance(IMongoClient client, AutoEncryptionOptions options)
        {
            if (__instance != null)
            {
                return __instance;
            }
            else
            {
                return __instance = new LibMongoCryptController(client, options);
            }
        }

        //public LibMongoCryptController(
        //    AutoEncryptionOptions autoEncryptionOptions)
        //{
        //    _autoEncryptionOptions = Ensure.IsNotNull(autoEncryptionOptions, nameof(autoEncryptionOptions));

        //    var keyVaultClient = Ensure.IsNotNull(autoEncryptionOptions.KeyVaultClient, $"{nameof(autoEncryptionOptions)}.{nameof(autoEncryptionOptions.KeyVaultClient)}");
        //    var keyVaultNamespace = autoEncryptionOptions.KeyVaultNamespace;
        //    var keyVaultDatabase = keyVaultClient.GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName);
        //    _keyVaultCollection = keyVaultDatabase.GetCollection<BsonDocument>(keyVaultNamespace.CollectionName);
        //}

        // public methods
        // todo: move from here
        public Guid GenerateKey(IKmsKeyId kmsKeyId, CancellationToken cancellationToken)
        {
            byte[] keyBytes = null;

            var cryptOptions = CreateCryptOptions(databaseName: null);
            using (var context = _client.EncryptionSource.Get(cryptOptions).StartCreateDataKeyContext(kmsKeyId))
            {
                keyBytes = ProcessStates(context, _keyVaultCollection.Database.DatabaseNamespace.DatabaseName, CancellationToken.None);
            }

            var rawBsonDocument = new RawBsonDocument(keyBytes);
            _keyVaultCollection.InsertOne(rawBsonDocument, cancellationToken: cancellationToken);
            var guid = rawBsonDocument.GetValue("_id").AsGuid;
            return guid;
        }

        public async Task<Guid> GenerateKeyAsync(IKmsKeyId kmsKeyId, CancellationToken cancellationToken)
        {
            byte[] keyBytes = null;

            var cryptOptions = CreateCryptOptions(databaseName: null);
            using (var context = _client.EncryptionSource.Get(cryptOptions).StartCreateDataKeyContext(kmsKeyId))
            {
                keyBytes = await ProcessStatesAsync(context, _keyVaultCollection.Database.DatabaseNamespace.DatabaseName, cancellationToken).ConfigureAwait(false);
            }

            var rawBsonDocument = new RawBsonDocument(keyBytes);
            await _keyVaultCollection.InsertOneAsync(rawBsonDocument, cancellationToken: cancellationToken).ConfigureAwait(false);
            var guid = rawBsonDocument.GetValue("_id").AsGuid;
            return guid;
        }

        public byte[] DecryptFields(byte[] encryptedDocument, CancellationToken cancellationToken)
        {
            var cryptOptions = CreateCryptOptions(databaseName: null);
            using (var context = _client.EncryptionSource.Get(cryptOptions).StartDecryptionContext(encryptedDocument))
            {
                return ProcessStates(context, databaseName: null, cancellationToken) ?? encryptedDocument;
            }
        }

        public async Task<byte[]> DecryptFieldsAsync(byte[] encryptedDocument, CancellationToken cancellationToken)
        {
            var cryptOptions = CreateCryptOptions(databaseName: null);
            using (var context = _client.EncryptionSource.Get(cryptOptions).StartDecryptionContext(encryptedDocument))
            {
                return await ProcessStatesAsync(context, databaseName: null, cancellationToken).ConfigureAwait(false);
            }
        }

        public byte[] EncryptFields(string databaseName, byte[] encryptedDocument, CancellationToken cancellationToken)
        {
            var cryptOptions = CreateCryptOptions(databaseName);

            using (var context = _client.EncryptionSource.Get(cryptOptions).StartEncryptionContext(databaseName, encryptedDocument))
            {
                return ProcessStates(context, databaseName, cancellationToken);
            }
        }

        public async Task<byte[]> EncryptFieldsAsync(string databaseName, byte[] encryptedDocument, CancellationToken cancellationToken)
        {
            var cryptOptions = CreateCryptOptions(databaseName);
            using (var context = _client.EncryptionSource.Get(cryptOptions).StartEncryptionContext(databaseName, encryptedDocument))
            {
                return await ProcessStatesAsync(context, databaseName, cancellationToken).ConfigureAwait(false);
            }
        }

        // private methods
        public static bool AcceptAnyCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        private CryptOptions CreateCryptOptions(string databaseName)
        {
            IKmsCredentials kmsCredentials = null;
            var kmsProviders = _autoEncryptionOptions.KmsProviders;
            if (kmsProviders != null)
            {
                if (kmsProviders.TryGetValue("aws", out var awsProvider))
                {
                    //todo: region
                    if (awsProvider.TryGetValue("accessKeyId", out var accessKeyIdObject) && accessKeyIdObject is string accessKeyId &&
                        awsProvider.TryGetValue("secretAccessKey", out var secretAccessKeyObject) && secretAccessKeyObject is string secretAccessKey)
                    {
                        kmsCredentials = new AwsKmsCredentials(secretAccessKey, accessKeyId);
                    }
                }
                else if (kmsProviders.TryGetValue("local", out var localProvider))
                {
                    if (localProvider.TryGetValue("key", out var keyObject) && keyObject is byte[] key)
                    {
                        kmsCredentials = new LocalKmsCredentials(key);
                    }
                }
            }

            byte[] schemaBytes = null;
            var schemaMap = _autoEncryptionOptions.SchemaMap;
            if (databaseName != null && schemaMap != null && schemaMap.TryGetValue(databaseName, out var schemaObject) && schemaObject is BsonDocument schemaDocument)
            {
                var writeSettings = new BsonBinaryWriterSettings { GuidRepresentation = GuidRepresentation.Unspecified };
                schemaBytes = schemaDocument.ToBson(writerSettings: writeSettings);
            }

            return new CryptOptions(kmsCredentials, schemaBytes);
        }

        private void FeedResult(CryptContext context, BsonDocument document)
        {
            var writerSettings = new BsonBinaryWriterSettings { GuidRepresentation = GuidRepresentation.Unspecified };
            var documentBytes = document.ToBson(writerSettings: writerSettings);
            context.Feed(documentBytes);
            context.MarkDone();
        }

        private void FeedResults(CryptContext context, IEnumerable<BsonDocument> documents)
        {
            var writerSettings = new BsonBinaryWriterSettings { GuidRepresentation = GuidRepresentation.Unspecified }; // todo: why do we need it?
            foreach (var document in documents)
            {
                var documentBytes = document.ToBson(writerSettings: writerSettings);
                context.Feed(documentBytes);
            }
            context.MarkDone();
        }

        private void ProcessErrorState(CryptContext context)
        {
            throw new NotImplementedException();
        }

        private void ProcessNeedCollectionInfoState(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            var database = _client.GetDatabase(databaseName); //todo: cache?
            var filterBytes = context.GetOperation().ToArray();
            var filterDocument = new RawBsonDocument(filterBytes);
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
            var options = new ListCollectionsOptions { Filter = filter };
            var cursor = database.ListCollections(options, cancellationToken); //todo: async version?
            var results = cursor.ToList(cancellationToken);
            FeedResults(context, results);
        }

        private async Task ProcessNeedCollectionInfoStateAsync(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            var database = _client.GetDatabase(databaseName);
            var filterBytes = context.GetOperation().ToArray();
            var filterDocument = new RawBsonDocument(filterBytes);
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
            var options = new ListCollectionsOptions { Filter = filter };
            var cursor = await database.ListCollectionsAsync(options, cancellationToken).ConfigureAwait(false);
            var results = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);
            FeedResults(context, results);
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

            //filter = new BsonDocumentFilterDefinition<BsonDocument>(BsonDocument.Parse(@"{ ""$or"" : [{ ""_id"" : { ""$in"" : [UUID(""00000000-0000-0000-0000-000000000000"")] } }, { ""keyAltNames"" : { ""$in"" : [] } }] }"));

            var cursor = _keyVaultCollection.FindSync(filter, cancellationToken: cancellationToken);


            var results = cursor.ToList(cancellationToken);
            FeedResults(context, results);
        }

        private async Task ProcessNeedMongoKeysStateAsync(CryptContext context, CancellationToken cancellationToken)
        {
            var filterBytes = context.GetOperation().ToArray();
            var filterDocument = new RawBsonDocument(filterBytes);
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
            var cursor = await _keyVaultCollection.FindAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
            var results = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);
            FeedResults(context, results);
        }

        private void ProcessNeedMongoMarkingsState(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            var database = _mongocryptdClient.GetDatabase(databaseName);
            var commandBytes = context.GetOperation().ToArray();
            var commandDocument = new RawBsonDocument(commandBytes);
            var command = new BsonDocumentCommand<BsonDocument>(commandDocument);
            var result = database.RunCommand(command, cancellationToken: cancellationToken);
            FeedResult(context, result);
        }

        private async Task ProcessNeedMongoMarkingsStateAsync(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            var database = _mongocryptdClient.GetDatabase(databaseName);
            var commandBytes = context.GetOperation().ToArray();
            var commandDocument = new RawBsonDocument(commandBytes);
            var command = new BsonDocumentCommand<BsonDocument>(commandDocument);
            var result = await database.RunCommandAsync(command, cancellationToken: cancellationToken).ConfigureAwait(false);
            FeedResult(context, result);
        }

        private byte[] ProcessReadyState(CryptContext context)
        {
            return context.FinalizeForEncryption().ToArray();
        }

        private byte[] ProcessStates(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            byte[] result = null;
            while (true)
            {
                switch (context.State)
                {
                    case CryptContext.StateCode.MONGOCRYPT_CTX_DONE:
                        return result;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_ERROR:
                        ProcessErrorState(context);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_KMS:
                        ProcessNeedKmsState(context, cancellationToken);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_COLLINFO:
                        ProcessNeedCollectionInfoState(context, databaseName, cancellationToken);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_KEYS:
                        ProcessNeedMongoKeysState(context, cancellationToken);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_MARKINGS:
                        ProcessNeedMongoMarkingsState(context, databaseName, cancellationToken);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_READY:
                        result = ProcessReadyState(context);
                        break;
                    default:
                        throw new InvalidOperationException($"Unexpected context state: {context.State}.");
                }
            }
        }

        private async Task<byte[]> ProcessStatesAsync(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            byte[] result = null;
            while (true)
            {
                switch (context.State)
                {
                    case CryptContext.StateCode.MONGOCRYPT_CTX_DONE:
                        return result;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_ERROR:
                        ProcessErrorState(context); // no async version needed
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_KMS:
                        await ProcessNeedKmsStateAsync(context, cancellationToken).ConfigureAwait(false);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_COLLINFO:
                        await ProcessNeedCollectionInfoStateAsync(context, databaseName, cancellationToken).ConfigureAwait(false);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_KEYS:
                        await ProcessNeedMongoKeysStateAsync(context, cancellationToken).ConfigureAwait(false);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_NEED_MONGO_MARKINGS:
                        await ProcessNeedMongoMarkingsStateAsync(context, databaseName, cancellationToken).ConfigureAwait(false);
                        break;
                    case CryptContext.StateCode.MONGOCRYPT_CTX_READY:
                        result = ProcessReadyState(context); // no async version needed
                        break;
                    default:
                        throw new InvalidOperationException($"Unexpected context state: {context.State}.");
                }
            }
        }

        private void SendKmsRequest(KmsRequest request, CancellationToken cancellation)
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(request.Endpoint, 443);

            var validationCallback = new RemoteCertificateValidationCallback(AcceptAnyCertificate); // TODO: what validation needs to be done?

            using (var networkStream = new NetworkStream(socket, ownsSocket: true))
            using (var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false, validationCallback))
            {
#if NETSTANDARD1_5
                sslStream.AuthenticateAsClientAsync(request.Endpoint).ConfigureAwait(false).GetAwaiter().GetResult();
#else
                sslStream.AuthenticateAsClient(request.Endpoint);
#endif

                var requestBytes = request.Message.ToArray();
                sslStream.Write(requestBytes);

                var buffer = new byte[4096];
                while (request.BytesNeeded > 0)
                {
                    var count = sslStream.Read(buffer, 0, buffer.Length);
                    var responseBytes = new byte[count];
                    Buffer.BlockCopy(buffer, 0, responseBytes, 0, count);
                    request.Feed(responseBytes);
                }
            }
        }

        private async Task SendKmsRequestAsync(KmsRequest request, CancellationToken cancellation)
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
#if NETSTANDARD1_5
            await socket.ConnectAsync(request.Endpoint, 443).ConfigureAwait(false);
#else
            await Task.Factory.FromAsync(socket.BeginConnect(request.Endpoint, 443, null, null), socket.EndConnect).ConfigureAwait(false);
#endif

            var validationCallback = new RemoteCertificateValidationCallback(AcceptAnyCertificate); // TODO: what validation needs to be done?

            using (var networkStream = new NetworkStream(socket, ownsSocket: true))
            using (var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false, validationCallback))
            {
                await sslStream.AuthenticateAsClientAsync(request.Endpoint).ConfigureAwait(false);

                var requestBytes = request.Message.ToArray();
                await sslStream.WriteAsync(requestBytes, 0, requestBytes.Length).ConfigureAwait(false);

                var buffer = new byte[4096];
                while (request.BytesNeeded > 0)
                {
                    var count = await sslStream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
                    var responseBytes = new byte[count];
                    Buffer.BlockCopy(buffer, 0, responseBytes, 0, count);
                    request.Feed(responseBytes);
                }
            }
        }
    }
}
