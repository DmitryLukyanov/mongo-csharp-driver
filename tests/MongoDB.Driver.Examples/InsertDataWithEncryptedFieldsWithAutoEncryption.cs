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
using MongoDB.Bson;
using MongoDB.Driver.Encryption;
using Xunit;

namespace MongoDB.Driver.Examples
{
    public class InsertDataWithEncryptedFieldsWithAutoEncryption
    {
        private static readonly string __connectionString = "mongodb://localhost:27017";
        private static readonly string __sampleNameValue = "John Doe";
        private static readonly int __sampleSsnValue = 145014000;

        private static readonly BsonDocument __sampleDocFields =
            new BsonDocument
            {
                { "name", __sampleNameValue },
                { "ssn", __sampleSsnValue },
                { "bloodType", "AB-" },
                {
                    "medicalRecords",
                    new BsonArray(new []
                    {
                        new BsonDocument("weight", 180),
                        new BsonDocument("bloodPressure", "120/80")
                    })
                },
                {
                    "insurance",
                    new BsonDocument
                    {
                        { "policyNumber", 123142 },
                        { "provider", "MaestCare" }
                    }
                }
            };

        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-insertdatawithencryptedfields-cs
        [Fact]
        public void InsertDataWithEncryptedFields()
        {
            var keyVaultNamespace = CollectionNamespace.FromFullName("encryption.__keyVault");
            var medicalRecordsNamespace = CollectionNamespace.FromFullName("medicalRecords.patients");

            var keyIdBase64 = ""; // paste the generated key in Standard GuidRepresentation form here

            // Construct a JSON Schema
            var schema = JsonSchemaCreator.CreateJsonSchema(keyIdBase64);

            // Construct an auto-encrypting client
            var autoEncryptingClient = CreateAutoEncryptingClient(
                medicalRecordsNamespace,
                keyVaultNamespace,
                schema);
            var collection = autoEncryptingClient
                .GetDatabase(medicalRecordsNamespace.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(medicalRecordsNamespace.CollectionName);

            var ssnQuery = Builders<BsonDocument>.Filter.Eq("ssn", __sampleSsnValue);

            // Insert a document into the collection
            collection.UpdateOne(ssnQuery, new BsonDocument("$set", __sampleDocFields), new UpdateOptions() { IsUpsert = true });
            Console.WriteLine("Successfully upserted the sample document!");

            // Query by SSN field with auto-encrypting client
            var result = collection.Find(ssnQuery).Single();

            Console.WriteLine($"Query by SSN (deterministically-encrypted field) using auto-encrypting client returned:\n {result}.");

            // Query by SSN field with a normal non-auto-encrypting client
            var nonAutoEncryptingClient = new MongoClient(__connectionString);
            collection = nonAutoEncryptingClient
              .GetDatabase(medicalRecordsNamespace.DatabaseNamespace.DatabaseName)
              .GetCollection<BsonDocument>(medicalRecordsNamespace.CollectionName);
            result = collection.Find(ssnQuery).FirstOrDefault();
            if (result != null)
            {
                throw new Exception("Expected no document to be found but one was found.");
            }

            // Query by name field with a normal non-auto-encrypting client
            var nameQuery = Builders<BsonDocument>.Filter.Eq("name", __sampleNameValue);
            result = collection.Find(nameQuery).FirstOrDefault();
            if (result == null)
            {
                throw new Exception("Expected the document to be found but none was found.");
            }

            Console.WriteLine($"Query by name (non-encrypted field) using non-auto-encrypting client returned:\n {result}.");
        }

        private IMongoClient CreateAutoEncryptingClient(
            CollectionNamespace medicalRecordsNamespace,
            CollectionNamespace keyVaultNamespace,
            BsonDocument schema)
        {
            var kmsProviders = new Dictionary<string, IReadOnlyDictionary<string, object>>();

            // For local master key
            var localMasterKeyBase64 = File.ReadAllText("master-key.txt");
            var localMasterKeyBytes = Convert.FromBase64String(localMasterKeyBase64);

            var localOptions = new Dictionary<string, object>
            {
                { "key", localMasterKeyBytes }
            };
            kmsProviders.Add("local", localOptions);

            /* For Aws KMS, uncomment this block.
            var awsAccessKey = "<Aws access key>";
            var awsSecretAccessKey = "<Aws secret access key>";
            var awsKmsOptions = new Dictionary<string, object>
            {
                { "accessKeyId", awsAccessKey },
                { "secretAccessKey", awsSecretAccessKey }
            };
            kmsProviders.Add("aws", awsKmsOptions);
            */

            /* For Azure KMS, uncomment this block.
            var azureTenantId = "<Azure account organization>";
            var azureClientId = "<Azure client ID>";
            var azureClientSecret = "<Azure client secret>";
            var azureKmsOptions = new Dictionary<string, object>
            {
                { "tenantId", azureTenantId },
                { "clientId", azureClientId },
                { "clientSecret", azureClientSecret }
            };
            kmsProviders.Add("azure", azureKmsOptions);
            */

            /* For Gcp KMS, uncomment this block.
            var gcpEmail = "<Gcp email>";
            var gcpPrivateKey = "<Gcp private key>";
            var gcpKmsOptions = new Dictionary<string, object>
            {
                { "email", gcpEmail },
                { "privateKey", gcpPrivateKey }
            };
            kmsProviders.Add("gcp", gcpKmsOptions);
            */

            var schemaMap = new Dictionary<string, BsonDocument>();
            schemaMap.Add(medicalRecordsNamespace.ToString(), schema);

            var extraOptions = new Dictionary<string, object>()
            {
                // uncomment the following line if you are running mongocryptd manually
                // { "mongocryptdBypassSpawn", true }
            };

            var clientSettings = MongoClientSettings.FromConnectionString(__connectionString);
            var autoEncryptionOptions = new AutoEncryptionOptions(
                keyVaultNamespace: keyVaultNamespace,
                kmsProviders: kmsProviders,
                schemaMap: schemaMap,
                extraOptions: extraOptions);
            clientSettings.AutoEncryptionOptions = autoEncryptionOptions;
            return new MongoClient(clientSettings);
        }

        [Fact]
        public void GenerateJsonSchema()
        {
            // 4. Code snippet: generate the JSON Schema and assign it to a variable

            var schema = JsonSchemaCreator.CreateJsonSchema("<your_key_id>").ToString(); // replace "your_key_id" with your base64 data encryption key id 
        }

        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-autoencryptionconfiguration-cs
        [Fact]
        public void AutoEncryptionConfiguration()
        {
            AutoEncryptionOptions autoEncryptionOptions = null;

            var extraOptions = new Dictionary<string, object>()
            {
                { "mongocryptdSpawnArgs", new [] { "--port=30000" } },
            };
            autoEncryptionOptions.With(extraOptions: extraOptions);
        }

        // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-insertpatient-cs
#pragma warning disable xUnit1013 // Public method should be marked as test
        public static void InsertPatient(
#pragma warning restore xUnit1013 // Public method should be marked as test
            IMongoCollection<BsonDocument> collection,
            string name,
            int ssn,
            string bloodType,
            BsonDocument[] medicalRecords,
            int policyNumber,
            string provider)
        {
            var insurance = new BsonDocument
            {
                { "policyNumber", policyNumber },
                { "provider", provider }
            };

            var patient = new BsonDocument
            {
                { "name", name },
                { "ssn", ssn },
                { "bloodType", bloodType },
                { "medicalRecords", BsonArray.Create(medicalRecords) },
                { "insurance", insurance }
            };

            collection.InsertOne(patient);
        }
    }

    // https://gist.github.com/DmitryLukyanov/8417d1f5c30f12ffeb2be29056aff455#file-jsonschemacreator-cs
    public static class JsonSchemaCreator
    {
        private static readonly string DETERMINISTIC_ENCRYPTION_TYPE = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic";
        private static readonly string RANDOM_ENCRYPTION_TYPE = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";

        private static BsonDocument CreateEncryptMetadata(string keyIdBase64)
        {
            var keyId = new BsonBinaryData(Convert.FromBase64String(keyIdBase64), BsonBinarySubType.UuidStandard);
            return new BsonDocument("keyId", new BsonArray(new[] { keyId }));
        }

        private static BsonDocument CreateEncryptedField(string bsonType, bool isDeterministic)
        {
            return new BsonDocument
            {
                {
                    "encrypt",
                    new BsonDocument
                    {
                        { "bsonType", bsonType },
                        { "algorithm", isDeterministic ? DETERMINISTIC_ENCRYPTION_TYPE : RANDOM_ENCRYPTION_TYPE}
                    }
                }
            };
        }

        public static BsonDocument CreateJsonSchema(string keyId)
        {
            return new BsonDocument
            {
                { "bsonType", "object" },
                { "encryptMetadata", CreateEncryptMetadata(keyId) },
                {
                    "properties",
                    new BsonDocument
                    {

                        { "ssn", CreateEncryptedField("int", true) },
                        { "bloodType", CreateEncryptedField("string", false) },
                        { "medicalRecords", CreateEncryptedField("array", false) },
                        {
                            "insurance",
                            new BsonDocument
                            {
                                { "bsonType", "object" },
                                {
                                    "properties",
                                    new BsonDocument
                                    {
                                        { "policyNumber", CreateEncryptedField("int", true) }
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }
    }
}
