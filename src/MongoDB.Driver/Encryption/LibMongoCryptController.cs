﻿/* Copyright 2019-present MongoDB Inc.
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
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol;
using MongoDB.Libmongocrypt;

namespace MongoDB.Driver
{
    internal class LibMongoCryptController : IBinaryDocumentFieldDecryptor, IBinaryDocumentFieldEncryptor
    {
        // private fields
        private readonly MongoClient _client;
        private readonly CryptClient _cryptClient;
        private readonly IMongoCollection<BsonDocument> _keyVaultCollection;
        private readonly MongoClient _mongocryptdClient;

        // constructors
        public LibMongoCryptController(
            MongoClient client,
            AutoEncryptionOptions autoEncryptionOptions)
        {
            _client = Ensure.IsNotNull(client, nameof(client));
            Ensure.IsNotNull(autoEncryptionOptions, nameof(autoEncryptionOptions));
            _cryptClient = client.CryptClient;_mongocryptdClient = client.MongoCryptDClient;
            _keyVaultCollection = GetKeyVaultCollection(autoEncryptionOptions, client);
        }

        // public methods
        public Guid CreateDataKey(IKmsKeyId kmsKeyId, CancellationToken cancellationToken)
        {
            byte[] keyDocumentBytes;
            try
            {
                using (var context = _cryptClient.StartCreateDataKeyContext(kmsKeyId))
                {
                    keyDocumentBytes = ProcessStates(context, _keyVaultCollection.Database.DatabaseNamespace.DatabaseName, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }

            var keyDocument = new RawBsonDocument(keyDocumentBytes);
            _keyVaultCollection.InsertOne(keyDocument, cancellationToken: cancellationToken);
            var guid = keyDocument["_id"].AsGuid;
            return guid;
        }

        public async Task<Guid> CreateDataKeyAsync(IKmsKeyId kmsKeyId, CancellationToken cancellationToken)
        {
            byte[] keyDocumentBytes;

            try
            {
                using (var context = _cryptClient.StartCreateDataKeyContext(kmsKeyId))
                {
                    keyDocumentBytes = await ProcessStatesAsync(context, _keyVaultCollection.Database.DatabaseNamespace.DatabaseName, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }

            var keyDocument = new RawBsonDocument(keyDocumentBytes);
            await _keyVaultCollection.InsertOneAsync(keyDocument, cancellationToken: cancellationToken).ConfigureAwait(false);
            var guid = keyDocument["_id"].AsGuid;
            return guid;
        }

        public byte[] DecryptField(byte[] wrappedValueBytes, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = _cryptClient.StartExplicitDecryptionContext(wrappedValueBytes))
                {
                    return ProcessStates(context, databaseName: null, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public async Task<byte[]> DecryptFieldAsync(byte[] wrappedValueBytes, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = _cryptClient.StartExplicitDecryptionContext(wrappedValueBytes))
                {
                    return await ProcessStatesAsync(context, databaseName: null, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public byte[] DecryptFields(byte[] encryptedDocumentBytes, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = _cryptClient.StartDecryptionContext(encryptedDocumentBytes))
                {
                    return ProcessStates(context, databaseName: null, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public async Task<byte[]> DecryptFieldsAsync(byte[] encryptedDocumentBytes, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = _cryptClient.StartDecryptionContext(encryptedDocumentBytes))
                {
                    return await ProcessStatesAsync(context, databaseName: null, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public byte[] EncryptField(byte[] wrappedValueBytes, Guid? keyId, byte[] keyAltName, EncryptionAlgorithm encryptionAlgorithm, CancellationToken cancellationToken)
        {
            try
            {
                CryptContext context;
                if (keyId.HasValue)
                {
                    context = _cryptClient.StartExplicitEncryptionContext(keyId.Value, encryptionAlgorithm, wrappedValueBytes);
                }
                else if (keyAltName != null)
                {
                    context = _cryptClient.StartExplicitEncryptionContext(keyAltName, encryptionAlgorithm, wrappedValueBytes);
                }
                else
                {
                    throw new Exception("Key Id and Alt name cannot both be set.");
                }

                using (context)
                {
                    return ProcessStates(context, databaseName: null, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public async Task<byte[]> EncryptFieldAsync(byte[] wrappedValueBytes, Guid? keyId, byte[] keyAltName, EncryptionAlgorithm encryptionAlgorithm, CancellationToken cancellationToken)
        {
            try
            {
                CryptContext context;
                if (keyId.HasValue)
                {
                    context = _cryptClient.StartExplicitEncryptionContext(keyId.Value, encryptionAlgorithm, wrappedValueBytes);
                }
                else if (keyAltName != null)
                {
                    context = _cryptClient.StartExplicitEncryptionContext(keyAltName, encryptionAlgorithm, wrappedValueBytes);
                }
                else
                {
                    throw new Exception("Key Id and Alt name cannot both be set.");
                }

                using (context)
                {
                    return await ProcessStatesAsync(context, databaseName: null, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public byte[] EncryptFields(string databaseName, byte[] unencryptedCommandBytes, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = _cryptClient.StartEncryptionContext(databaseName, unencryptedCommandBytes))
                {
                    return ProcessStates(context, databaseName, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        public async Task<byte[]> EncryptFieldsAsync(string databaseName, byte[] unencryptedCommandBytes, CancellationToken cancellationToken)
        {
            try
            {
                using (var context = _cryptClient.StartEncryptionContext(databaseName, unencryptedCommandBytes))
                {
                    return await ProcessStatesAsync(context, databaseName, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new MongoClientException(ex.Message, ex);
            }
        }

        // private methods
        private static bool AcceptAnyCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        private static IMongoCollection<BsonDocument> GetKeyVaultCollection(AutoEncryptionOptions autoEncryptionOptions, IMongoClient client)
        {
            var keyVaultClient = autoEncryptionOptions.KeyVaultClient ?? client;
            var keyVaultNamespace = autoEncryptionOptions.KeyVaultNamespace;
            var keyVaultDatabase = keyVaultClient.GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName);
            return keyVaultDatabase.GetCollection<BsonDocument>(keyVaultNamespace.CollectionName);
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
            var writerSettings = new BsonBinaryWriterSettings { GuidRepresentation = GuidRepresentation.Unspecified };
            foreach (var document in documents)
            {
                var documentBytes = document.ToBson(writerSettings: writerSettings);
                context.Feed(documentBytes);
            }
            context.MarkDone();
        }

        private void ProcessErrorState(CryptContext context)
        {
            // todo: this stage doesn't work.
            // Libmongocrypt throws an error together with stage.
            throw new NotImplementedException();
        }

        private void ProcessNeedCollectionInfoState(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            var database = _client.GetDatabase(databaseName);
            var filterBytes = context.GetOperation().ToArray();
            var filterDocument = new RawBsonDocument(filterBytes);
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
            var options = new ListCollectionsOptions { Filter = filter };
            var cursor = database.ListCollections(options, cancellationToken);
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
            var response = database.RunCommand(command, cancellationToken: cancellationToken);
            RestoreDbNodeInResponse(commandDocument, response);
            FeedResult(context, response);
        }

        private async Task ProcessNeedMongoMarkingsStateAsync(CryptContext context, string databaseName, CancellationToken cancellationToken)
        {
            var database = _mongocryptdClient.GetDatabase(databaseName);
            var commandBytes = context.GetOperation().ToArray();
            var commandDocument = new RawBsonDocument(commandBytes);
            var command = new BsonDocumentCommand<BsonDocument>(commandDocument);
            var response = await database.RunCommandAsync(command, cancellationToken: cancellationToken).ConfigureAwait(false);
            RestoreDbNodeInResponse(commandDocument, response);
            FeedResult(context, response);
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

        private void RestoreDbNodeInResponse(BsonDocument request, BsonDocument response)
        {
            if (request.TryGetElement("$db", out var db))
            {
                var result = response["result"].AsBsonDocument;
                if (!result.Contains("$db"))
                {
                    result.Add(db);
                }
            }
        }
    }
}
