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
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Encryption;
using MongoDB.Driver.TestHelpers;
using MongoDB.Libmongocrypt;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.client_side_encryption.prose_tests
{
    // TODO: move into ClientEncryptionProseTests.cs
    public class ClientSideEncryptionSmokeProseTests
    {
        #region static
        private static readonly CollectionNamespace __collCollectionNamespace = CollectionNamespace.FromFullName("db.coll");
        private static readonly CollectionNamespace __keyVaultCollectionNamespace = CollectionNamespace.FromFullName("keyvault.datakey");

        private static readonly string __autoEncryptionSchemaMap = @"{
    ""db.coll"": {
        ""bsonType"": ""object"",
        ""properties"": {
            ""secret_azure"": {
                ""encrypt"": {
                    ""keyId"": [{
                        ""$binary"": {
                            ""base64"": ""As3URE1jRcyHOPjaLWHOXA=="",
                            ""subType"": ""04""
                        }
                    }],
                    ""algorithm"": ""AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"",
                    ""bsonType"": ""string""
                }
            },
            ""secret_gcp"": {
                ""encrypt"": {
                    ""keyId"": [{
                        ""$binary"": {
                            ""base64"": ""osU8SLxJRHONbl8Oh5o+eg=="",
                            ""subType"": ""04""
                        }
                    }],
                    ""algorithm"": ""AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"",
                    ""bsonType"": ""string""
                }
            }
        }
    }
}";

        private static BsonDocument GetDekDocument(string kmsType)
        {
            string dekContent;
            switch (kmsType)
            {
                case "azure":
                    dekContent = @"
{
    ""_id"": {
        ""$binary"": {
            ""base64"": ""As3URE1jRcyHOPjaLWHOXA=="",
            ""subType"": ""04""
        }
    },
    ""keyMaterial"": {
        ""$binary"": {
            ""base64"": ""df6fFLZqBsZSnQz2SnTYWNBtznIHktVSDMaidAdL7yVVgxBJQ0DyPZUR2HDQB4hdYym3w4C+VGqzcyTZNJOXn6nJzpGrGlIQMcjv93HE4sP2d245ShQCi1nTkLmMaXN63E2fzltOY3jW7ojf5Z4+r8kxmzyfymmSRgo0w8AF7lUWvFhnBYoE4tE322L31vtAK3Zj8pTPvw8/TcUdMSI9Y669IIzxbMy5yMPmdzpnb8nceUv6/CJoeiLhbt5GgaHqIAv7tHFOY8ZX8ztowMLa3GeAjd9clvzraDTqrfMFYco/kDKAW5iPQQ+Xuy1fP8tyFp0ZwaL/7Ed2sc819j8FTQ=="",
            ""subType"": ""00""
        }
    },
    ""creationDate"": {
        ""$date"": {
            ""$numberLong"": ""1601573901680""
        }
    },
    ""updateDate"": {
        ""$date"": {
            ""$numberLong"": ""1601573901680""
        }
    },
    ""status"": {
        ""$numberInt"": ""0""
    },
    ""masterKey"": {
        ""provider"": ""azure"",
        ""keyVaultEndpoint"": ""key-vault-kevinalbs.vault.azure.net"",
        ""keyName"": ""test-key""
    }
}";
                    break;
                case "gcp":
                    dekContent = @"{
    ""_id"": {
        ""$binary"": {
            ""base64"": ""osU8SLxJRHONbl8Oh5o+eg=="",
            ""subType"": ""04""
        }
    },
    ""keyMaterial"": {
        ""$binary"": {
            ""base64"": ""CiQAg4LDql74hjYPZ957Z7YpCrD6yTVVXKegflJDstQ/xngTyx0SiQEAkWNo/fjPj6jMNSvEop07/29Fu72QHFDRYM3e/KFHfnMQjKzfxb1yX1dC6MbO5FZG/UNBkXlJgPqbHNVuizea3QC24kV5iOiEb4nTM7+RW+8TfVb6QerWWe6MjC+kNpj4LMVcc1lFfVDeGgpJLyMLNGitrjR16qH8qQTNbGNy0toTL69JUmgS8Q=="",
            ""subType"": ""00""
        }
    },
    ""creationDate"": {
        ""$date"": {
            ""$numberLong"": ""1601574333107""
        }
    },
    ""updateDate"": {
        ""$date"": {
            ""$numberLong"": ""1601574333107""
        }
    },
    ""status"": {
        ""$numberInt"": ""0""
    },
    ""masterKey"": {
        ""provider"": ""gcp"",
        ""projectId"": ""csfle-poc"",
        ""location"": ""global"",
        ""keyRing"": ""test"",
        ""keyName"": ""quickstart""
    }
}";
                    break;
                default:
                    throw new InvalidOperationException("DEK content has not been provided.");
            }

            return BsonDocument.Parse(dekContent);
        }
        #endregion

        [Theory]
        [ParameterAttributeData]
        public void Auto_encryption_and_decryption_should_return_expected_result([Values(false, true)] bool async)
        {
            var schemaMapDocument = BsonDocument.Parse(__autoEncryptionSchemaMap);

            using (var client = ConfigureClient())
            {
                // initialize keys
                var collection = GetCollection(client, __keyVaultCollectionNamespace);
                Insert(
                    collection,
                    async,
                    GetDekDocument("azure"),
                    GetDekDocument("gcp"));

                using (var clientEncrypted = ConfigureClientEncrypted(schemaMap: schemaMapDocument))
                {
                    var coll = GetCollection(clientEncrypted, __collCollectionNamespace);
                    var testDocument = BsonDocument.Parse("{ secret_azure : 'test', secret_gcp : 'test' }");
                    Insert(coll, async, (BsonDocument)testDocument.DeepClone());

                    var regularCollection = GetCollection(client, __collCollectionNamespace);
                    var encryptedDocument = Find(regularCollection, new BsonDocument(), async).Single();
                    var expectedDocument = BsonDocument.Parse(@"
                    {
                        ""secret_azure"" : {
                            ""$binary"" : {
                                ""base64"" : ""AQLN1ERNY0XMhzj42i1hzlwC8/OSU9bHfaQRmmRF5l7d5ZpqJX13qF5zSyExo8N9c1b6uS/LoKrHNzcEMKNrkpi3jf2HiShTFRF0xi8AOD9yfw=="",
                                ""subType"" : ""06""
                            }
                        },
                        ""secret_gcp"" : {
                            ""$binary"" : {
                                ""base64"" : ""AaLFPEi8SURzjW5fDoeaPnoCGcOFAmFOPpn5584VPJJ8iXIgml3YDxMRZD9IWv5otyoft8fBzL1LsDEp0lTeB32cV1gOj0IYeAKHhGIleuHZtA=="",
                                ""subType"" : ""06""
                            }
                        }
                    }");
                    encryptedDocument.Remove("_id");
                    encryptedDocument.Should().Be(expectedDocument);

                    var decryptedDocument = Find(coll, new BsonDocument(), async).Single();
                    decryptedDocument.Remove("_id");
                    decryptedDocument.Should().Be(testDocument);
                }
            }
        }

        [Theory]
        [ParameterAttributeData]
        public void Explicit_encryption_and_decryption_should_return_expected_result(
            [Values("gcp", "azure")] string kmsType,
            [Values(false, true)] bool async)
        {
            var dekDocument = GetDekDocument(kmsType);
            using (var client = ConfigureClient())
            {
                var collection = GetCollection(client, __keyVaultCollectionNamespace);
                Insert(collection, async, dekDocument);

                var testString = "test";

                using (var clientEncryption = ConfigureClientEncryption(client.Wrapped as MongoClient))
                {
                    var encryptOptions = CreateEncryptOptions(
                          EncryptionAlgorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                          "id",
                          kmsType);
                    var encrypted = ExplicitEncrypt(clientEncryption, encryptOptions, testString, async);
                    AssertEncriptedText(encriptedBase64String: Convert.ToBase64String(encrypted.Bytes));

                    var decrypted = ExplicitDecrypt(clientEncryption, encrypted, async);
                    decrypted.ToString().Should().Be(testString);
                }
            }

            void AssertEncriptedText(string encriptedBase64String)
            {
                string textToAssert;
                switch (kmsType)
                {
                    case "azure":
                        textToAssert = "AQLN1ERNY0XMhzj42i1hzlwC8/OSU9bHfaQRmmRF5l7d5ZpqJX13qF5zSyExo8N9c1b6uS/LoKrHNzcEMKNrkpi3jf2HiShTFRF0xi8AOD9yfw==";
                        break;
                    case "gcp":
                        textToAssert = "AaLFPEi8SURzjW5fDoeaPnoCGcOFAmFOPpn5584VPJJ8iXIgml3YDxMRZD9IWv5otyoft8fBzL1LsDEp0lTeB32cV1gOj0IYeAKHhGIleuHZtA==";
                        break;
                    default:
                        throw new ArgumentException("Unexpected kmsType.");
                }
                encriptedBase64String.Should().Be(textToAssert);
            }
        }

        // private methods
        private DisposableMongoClient ConfigureClient(bool clearCollections = true)
        {
            var client = CreateMongoClient();
            if (clearCollections)
            {
                var clientKeyVaultDatabase = client.GetDatabase(__keyVaultCollectionNamespace.DatabaseNamespace.DatabaseName);
                clientKeyVaultDatabase.DropCollection(__keyVaultCollectionNamespace.CollectionName);
                var clientDbDatabase = client.GetDatabase(__collCollectionNamespace.DatabaseNamespace.DatabaseName);
                clientDbDatabase.DropCollection(__collCollectionNamespace.CollectionName);
            }
            return client;
        }

        private DisposableMongoClient ConfigureClientEncrypted(
            BsonDocument schemaMap = null,
            string kmsProviderFilter = null,
            bool bypassAutoEncryption = false)
        {
            var kmsProviders = GetKmsProviders();

            var clientEncrypted =
                CreateMongoClient(
                    keyVaultNamespace: __keyVaultCollectionNamespace,
                    schemaMapDocument: schemaMap,
                    kmsProviders:
                        kmsProviderFilter == null
                            ? kmsProviders
                            : kmsProviders
                                .Where(c => c.Key == kmsProviderFilter)
                                .ToDictionary(key => key.Key, value => value.Value),
                    bypassAutoEncryption: bypassAutoEncryption);
            return clientEncrypted;
        }

        private ClientEncryption ConfigureClientEncryption(MongoClient client)
        {
            var clientEncryptionOptions = new ClientEncryptionOptions(
                keyVaultClient: client.Settings.AutoEncryptionOptions?.KeyVaultClient ?? client,
                keyVaultNamespace: __keyVaultCollectionNamespace,
                kmsProviders: GetKmsProviders());

            return new ClientEncryption(clientEncryptionOptions);
        }

        private EncryptOptions CreateEncryptOptions(EncryptionAlgorithm algorithm, string identifier, string kms)
        {
            Guid? keyId = null;
            string alternateName = null;
            if (identifier == "id")
            {
                switch (kms)
                {
                    case "azure":
                        keyId = GuidConverter.FromBytes(Convert.FromBase64String("As3URE1jRcyHOPjaLWHOXA=="), GuidRepresentation.Standard);
                        break;
                    case "gcp":
                        keyId = GuidConverter.FromBytes(Convert.FromBase64String("osU8SLxJRHONbl8Oh5o+eg=="), GuidRepresentation.Standard);
                        break;
                    default:
                        throw new ArgumentException($"Unsupported kms type {kms}.");
                }
            }
            else if (identifier == "altname")
            {
                alternateName = kms;
            }
            else
            {
                throw new ArgumentException($"Unsupported identifier {identifier}.", nameof(identifier));
            }

            return new EncryptOptions(algorithm.ToString(), alternateName, keyId);
        }

        private DisposableMongoClient CreateMongoClient(
            CollectionNamespace keyVaultNamespace = null,
            BsonDocument schemaMapDocument = null,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> kmsProviders = null,
            bool bypassAutoEncryption = false)
        {
            var mongoClientSettings = DriverTestConfiguration.GetClientSettings().Clone();
#pragma warning disable 618
            if (BsonDefaults.GuidRepresentationMode == GuidRepresentationMode.V2)
            {
                mongoClientSettings.GuidRepresentation = GuidRepresentation.Unspecified;
            }
#pragma warning restore 618

            if (keyVaultNamespace != null || schemaMapDocument != null || kmsProviders != null)
            {
                var schemaMap = GetSchemaMapIfNotNull(schemaMapDocument);

                if (kmsProviders == null)
                {
                    kmsProviders = new ReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>(new Dictionary<string, IReadOnlyDictionary<string, object>>());
                }

                var autoEncryptionOptions = new AutoEncryptionOptions(
                    keyVaultNamespace: keyVaultNamespace,
                    kmsProviders: kmsProviders,
                    schemaMap: schemaMap,
                    extraOptions: null,
                    bypassAutoEncryption: bypassAutoEncryption);

                mongoClientSettings.AutoEncryptionOptions = autoEncryptionOptions;
            }

            return new DisposableMongoClient(new MongoClient(mongoClientSettings));
        }

        private IMongoCollection<BsonDocument> GetCollection(IMongoClient client, CollectionNamespace collectionNamespace)
        {
            var collectionSettings = new MongoCollectionSettings
            {
                ReadConcern = ReadConcern.Majority,
                WriteConcern = WriteConcern.WMajority
            };
            return client
                .GetDatabase(collectionNamespace.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(collectionNamespace.CollectionName, collectionSettings);
        }

        private IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> GetKmsProviders()
        {
            var kmsProviders = new Dictionary<string, IReadOnlyDictionary<string, object>>();

            var azureTenantId = Environment.GetEnvironmentVariable("FLE_AZURE_TENANT_ID");
            var azureClientId = Environment.GetEnvironmentVariable("FLE_AZURE_CLIENT_ID") ?? throw new Exception("FLE_AZURE_CLIENT_ID system variable should be configured on the machine.");
            var azureClientSecret = Environment.GetEnvironmentVariable("FLE_AZURE_CLIENT_SECRET") ?? throw new Exception("The FLE_AZURE_CLIENT_SECRET system variable should be configured on the machine.");
            var azureKmsOptions = new Dictionary<string, object>
            {
                { "tenantId", azureTenantId },
                { "clientId", azureClientId },
                { "clientSecret", azureClientSecret }
            };
            kmsProviders.Add("azure", azureKmsOptions);

            var gcpEmail = Environment.GetEnvironmentVariable("FLE_GCP_EMAIL") ?? throw new Exception("FLE_GCP_EMAIL system variable should be configured on the machine.");
            var gcpPRivateKey = Environment.GetEnvironmentVariable("FLE_GCP_PRIVATE_KEY") ?? throw new Exception("FLE_GCP_PRIVATE_KEY system variable should be configured on the machine.");
            var gcpKmsOptions = new Dictionary<string, object>
            {
                { "email", gcpEmail },
                { "privateKey", gcpPRivateKey }
            };
            kmsProviders.Add("gcp", gcpKmsOptions);

            return new ReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>(kmsProviders);
        }

        private Dictionary<string, BsonDocument> GetSchemaMapIfNotNull(BsonDocument schemaMapDocument)
        {
            Dictionary<string, BsonDocument> schemaMap = null;
            if (schemaMapDocument != null)
            {
                var element = schemaMapDocument.Single();
                schemaMap = new Dictionary<string, BsonDocument>
                    {
                        { element.Name, element.Value.AsBsonDocument }
                    };
            }
            return schemaMap;
        }

        private BsonValue ExplicitDecrypt(
            ClientEncryption clientEncryption,
            BsonBinaryData value,
            bool async)
        {
            BsonValue decryptedValue;
            if (async)
            {
                decryptedValue = clientEncryption
                    .DecryptAsync(
                        value,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
            }
            else
            {
                decryptedValue = clientEncryption.Decrypt(
                    value,
                    CancellationToken.None);
            }

            return decryptedValue;
        }

        private BsonBinaryData ExplicitEncrypt(
            ClientEncryption clientEncryption,
            EncryptOptions encryptOptions,
            BsonValue value,
            bool async)
        {
            BsonBinaryData encryptedValue;
            if (async)
            {
                encryptedValue = clientEncryption
                    .EncryptAsync(
                        value,
                        encryptOptions,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
            }
            else
            {
                encryptedValue = clientEncryption.Encrypt(
                    value,
                    encryptOptions,
                    CancellationToken.None);
            }

            return encryptedValue;
        }

        private IAsyncCursor<BsonDocument> Find(
            IMongoCollection<BsonDocument> collection,
            BsonDocument filter,
            bool async)
        {
            if (async)
            {
                return collection
                    .FindAsync(new BsonDocumentFilterDefinition<BsonDocument>(filter))
                    .GetAwaiter()
                    .GetResult();
            }
            else
            {
                return collection
                    .FindSync(new BsonDocumentFilterDefinition<BsonDocument>(filter));
            }
        }

        private void Insert(
            IMongoCollection<BsonDocument> collection,
            bool async,
            params BsonDocument[] documents)
        {
            if (async)
            {
                collection
                    .InsertManyAsync(documents)
                    .GetAwaiter()
                    .GetResult();
            }
            else
            {
                collection.InsertMany(documents);
            }
        }
    }
}
