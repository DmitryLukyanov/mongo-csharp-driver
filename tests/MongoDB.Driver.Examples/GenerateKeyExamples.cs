/* Copyright 2020-present MongoDB Inc.
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
using System.IO;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver.Encryption;
using Xunit;

namespace MongoDB.Driver.Examples
{
    public class GenerateKeyExamples
    {
        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-localkmsprovider-cs
        [Fact]
        public void LocalKmsProvider()
        {
            // 2. Code full example: generate a 96-byte master key and save to a file called master-key.txt.
            string localMasterKeyBase64;
            using (var randomNumberGenerator = System.Security.Cryptography.RandomNumberGenerator.Create())
            {
                var bytes = new byte[96];
                randomNumberGenerator.GetBytes(bytes);
                localMasterKeyBase64 = Convert.ToBase64String(bytes);
                Console.WriteLine(localMasterKeyBase64);
                File.WriteAllText("master-key.txt", localMasterKeyBase64);
            }

            // 3. generate a data key using the master key read from the file master-key.txt
            localMasterKeyBase64 = File.ReadAllText("master-key.txt");
            var localMasterKeyBytes = Convert.FromBase64String(localMasterKeyBase64);

            var kmsProviders = new Dictionary<string, IReadOnlyDictionary<string, object>>();
            var localOptions = new Dictionary<string, object>
            {
                { "key", localMasterKeyBytes }
            };
            kmsProviders.Add("local", localOptions);

            var connectionString = "mongodb://localhost:27017";
            var keyVaultNamespace = CollectionNamespace.FromFullName("encryption.__keyVault");
            var keyVaultClient = new MongoClient(connectionString);
            var clientEncryptionOptions = new ClientEncryptionOptions(
                keyVaultClient: keyVaultClient,
                keyVaultNamespace: keyVaultNamespace,
                kmsProviders: kmsProviders);

            var clientEncryption = new ClientEncryption(clientEncryptionOptions);
            var dataKeyId = clientEncryption.CreateDataKey("local", new DataKeyOptions(), CancellationToken.None);
            Console.WriteLine($"DataKeyId [UUID]: {dataKeyId}");
            var dataKeyIdBase64 = Convert.ToBase64String(GuidConverter.ToBytes(dataKeyId, GuidRepresentation.Standard));
            Console.WriteLine($"DataKeyId [base64]: {dataKeyIdBase64}");

            // Verify that the key document was created
            var client = new MongoClient(connectionString);
            var collection = client
                .GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(
                    keyVaultNamespace.CollectionName,
                    new MongoCollectionSettings
                    {
#pragma warning disable CS0618
                        GuidRepresentation = GuidRepresentation.Standard
#pragma warning restore CS0618
                    });
            var query = Builders<BsonDocument>.Filter.Eq("_id", new BsonBinaryData(dataKeyId, GuidRepresentation.Standard));
            var keyDocument = collection
                .Find(query)
                .Single();

            Console.WriteLine(keyDocument);
        }

        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-awskmsprovider-cs
        [Fact]
        public void AwsKmsProvider()
        {
            var kmsProviders = new Dictionary<string, IReadOnlyDictionary<string, object>>();

            var awsAccessKey = Environment.GetEnvironmentVariable("FLE_AWS_ACCESS_KEY_ID");
            var awsSecretAccessKey = Environment.GetEnvironmentVariable("FLE_AWS_SECRET_ACCESS_KEY");
            var awsKmsOptions = new Dictionary<string, object>
            {
                { "accessKeyId", awsAccessKey },
                { "secretAccessKey", awsSecretAccessKey }
            };
            kmsProviders.Add("aws", awsKmsOptions);

            var connectionString = "mongodb://localhost:27017";
            var keyVaultNamespace = CollectionNamespace.FromFullName("encryption.__keyVault");
            var keyVaultClient = new MongoClient(connectionString);
            var clientEncryptionOptions = new ClientEncryptionOptions(
                keyVaultClient: keyVaultClient,
                keyVaultNamespace: keyVaultNamespace,
                kmsProviders: kmsProviders);

            var clientEncryption = new ClientEncryption(clientEncryptionOptions);
            var awsDataKeyKey = "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"; // e.g. "arn:aws:kms:us-east-2:111122223333:alias/test-key"
            var awsDataKeyRegion = "us-east-1"; 
            var dataKeyOptions = new DataKeyOptions(
                masterKey: new BsonDocument
                {
                    { "region", awsDataKeyRegion },
                    { "key", awsDataKeyKey }
                });

            var dataKeyId = clientEncryption.CreateDataKey("aws", dataKeyOptions, CancellationToken.None);
            Console.WriteLine($"DataKeyId [UUID]: {dataKeyId}");
            var dataKeyIdBase64 = Convert.ToBase64String(GuidConverter.ToBytes(dataKeyId, GuidRepresentation.Standard));
            Console.WriteLine($"DataKeyId [base64]: {dataKeyIdBase64}");

            // Verify that the key document was created
            var client = new MongoClient(connectionString);
            var collection = client
                .GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(
                    keyVaultNamespace.CollectionName,
                    new MongoCollectionSettings
                    {
#pragma warning disable CS0618
                        GuidRepresentation = GuidRepresentation.Standard
#pragma warning restore CS0618
                    });
            var query = Builders<BsonDocument>.Filter.Eq("_id", new BsonBinaryData(dataKeyId, GuidRepresentation.Standard));
            var keyDocument = collection
                .Find(query)
                .Single();

            Console.WriteLine(keyDocument);
        }

        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-azurekmsprovider-cs
        [Fact]
        public void AzureKmsProvider()
        {
            var kmsProviders = new Dictionary<string, IReadOnlyDictionary<string, object>>();

            var azureTenantId = Environment.GetEnvironmentVariable("FLE_AZURE_TENANT_ID");
            var azureClientId = Environment.GetEnvironmentVariable("FLE_AZURE_CLIENT_ID");
            var azureClientSecret = Environment.GetEnvironmentVariable("FLE_AZURE_CLIENT_SECRET");
            var azureKmsOptions = new Dictionary<string, object>
            {
                { "tenantId", azureTenantId },
                { "clientId", azureClientId },
                { "clientSecret", azureClientSecret },

            };
            kmsProviders.Add("azure", azureKmsOptions);

            var connectionString = "mongodb://localhost:27017";
            var keyVaultNamespace = CollectionNamespace.FromFullName("encryption.__keyVault");
            var keyVaultClient = new MongoClient(connectionString);
            var clientEncryptionOptions = new ClientEncryptionOptions(
                keyVaultClient: keyVaultClient,
                keyVaultNamespace: keyVaultNamespace,
                kmsProviders: kmsProviders);

            var clientEncryption = new ClientEncryption(clientEncryptionOptions);
            var azureDataKeyKeyName = "key-name-csfle";
            var azureDataKeyKeyVaultEndpoint = "key-vault-csfle.vault.azure.net"; // typically <azureKeyName>.vault.azure.net
            var dataKeyOptions = new DataKeyOptions(
                masterKey: new BsonDocument
                {
                    { "keyName", azureDataKeyKeyName },
                    { "keyVaultEndpoint", azureDataKeyKeyVaultEndpoint }
                });

            var dataKeyId = clientEncryption.CreateDataKey("azure", dataKeyOptions, CancellationToken.None);
            Console.WriteLine($"DataKeyId [UUID]: {dataKeyId}");
            var dataKeyIdBase64 = Convert.ToBase64String(GuidConverter.ToBytes(dataKeyId, GuidRepresentation.Standard));
            Console.WriteLine($"DataKeyId [base64]: {dataKeyIdBase64}");

            // Verify that the key document was created
            var client = new MongoClient(connectionString);
            var collection = client
                .GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(
                    keyVaultNamespace.CollectionName,
                    new MongoCollectionSettings
                    {
#pragma warning disable CS0618
                        GuidRepresentation = GuidRepresentation.Standard
#pragma warning restore CS0618
                    });
            var query = Builders<BsonDocument>.Filter.Eq("_id", new BsonBinaryData(dataKeyId, GuidRepresentation.Standard));
            var keyDocument = collection
                .Find(query)
                .Single();

            Console.WriteLine(keyDocument);
        }

        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-gcpkmsprovider-cs
        [Fact]
        public void GcpKmsProvider()
        {
            var kmsProviders = new Dictionary<string, IReadOnlyDictionary<string, object>>();

            var gcpPrivateKey = Environment.GetEnvironmentVariable("FLE_GCP_PRIVATE_KEY");
            var gcpEmail = Environment.GetEnvironmentVariable("FLE_GCP_EMAIL");
            var gcpKmsOptions = new Dictionary<string, object>
            {
                { "privateKey", gcpPrivateKey },
                { "email", gcpEmail },
            };
            kmsProviders.Add("gcp", gcpKmsOptions);

            var connectionString = "mongodb://localhost:27017";
            var keyVaultNamespace = CollectionNamespace.FromFullName("encryption.__keyVault");
            var keyVaultClient = new MongoClient(connectionString);
            var clientEncryptionOptions = new ClientEncryptionOptions(
                keyVaultClient: keyVaultClient,
                keyVaultNamespace: keyVaultNamespace,
                kmsProviders: kmsProviders);

            var clientEncryption = new ClientEncryption(clientEncryptionOptions);
            var gcpDataKeyProjectId = "devprod-drivers";
            var gcpDataKeyLocation = "global"; // Optional. e.g. "global"
            var gcpDataKeyKeyRing = "key-ring-csfle";
            var gcpDataKeyKeyName = "key-name-csfle";
            var gcpDataKeyEndpoint = "cloudkms.googleapis.com:443"; // Optional, KMS URL, defaults to https://www.googleapis.com/auth/cloudkms

            var dataKeyOptions = new DataKeyOptions(
                masterKey: new BsonDocument
                {
                    { "projectId", gcpDataKeyProjectId },
                    { "location", gcpDataKeyLocation } ,
                    { "keyRing", gcpDataKeyKeyRing },
                    { "keyName", gcpDataKeyKeyName },
                    { "endpoint", gcpDataKeyEndpoint }
                });

            var dataKeyId = clientEncryption.CreateDataKey("gcp", dataKeyOptions, CancellationToken.None);
            Console.WriteLine($"DataKeyId [UUID]: {dataKeyId}");
            var dataKeyIdBase64 = Convert.ToBase64String(GuidConverter.ToBytes(dataKeyId, GuidRepresentation.Standard));
            Console.WriteLine($"DataKeyId [base64]: {dataKeyIdBase64}");

            // Verify that the key document was created
            var client = new MongoClient(connectionString);
            var collection = client
                .GetDatabase(keyVaultNamespace.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(
                    keyVaultNamespace.CollectionName,
                    new MongoCollectionSettings
                    {
#pragma warning disable CS0618
                        GuidRepresentation = GuidRepresentation.Standard
#pragma warning restore CS0618
                    });
            var query = Builders<BsonDocument>.Filter.Eq("_id", new BsonBinaryData(dataKeyId, GuidRepresentation.Standard));
            var keyDocument = collection
                .Find(query)
                .Single();

            Console.WriteLine(keyDocument);
        }
    }
}
