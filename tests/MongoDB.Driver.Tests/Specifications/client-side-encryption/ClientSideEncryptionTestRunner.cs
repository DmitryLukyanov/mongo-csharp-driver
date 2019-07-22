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
using System.Collections.ObjectModel;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver.Core;
using MongoDB.Driver.Tests.Specifications.Runner;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.client_side_encryption
{
    public class ClientSideEncryptionTestRunner : MongoClientJsonDrivenTestRunnerBase
    {
        [SkippableTheory]
        [ClassData(typeof(TestCaseFactory))]
        public void Run(JsonDrivenTestCase testCase)
        {
            SetupAndRunTest(testCase);
        }
        protected override string[] ExpectedTestColumns => new[] { "description", "clientOptions", "operations", "expectations", "skipReason", "async", "outcome" };

        protected override string[] ExpectedSharedColumns
        {
            get
            {
                // todo: refactor
                var expectedSharedColumns = new List<string>(base.ExpectedSharedColumns);
                expectedSharedColumns.AddRange(new[] { "json_schema", "key_vault_data" });
                return expectedSharedColumns.ToArray();
            }
        }

        protected override void AssertEvents(EventCapturer eventCapturer, BsonDocument expectedEvent)
        {
            base.AssertEvents(
                eventCapturer,
                expectedEvent,
                (actual, expected) =>
                {
                    var expectedCommand = expected.GetValue("command_started_event", null)?.AsBsonDocument?.GetValue("command", null)?.AsBsonDocument;
                    if (expectedCommand == null)
                    {
                        return;
                    }

                    ReplaceTypeAssertionWithActual(expectedCommand, actual.Command);
                });
        }

        protected override MongoClient CreateClientForTestSetup()
        {
            var clientSettings = DriverTestConfiguration.GetClientSettings();
            clientSettings.GuidRepresentation = GuidRepresentation.Standard;
            return new MongoClient(clientSettings);
        }

        protected override void CreateCollection(IMongoClient client, string databaseName, string collectionName, BsonDocument test, BsonDocument shared)
        {
            if (shared.TryGetElement("json_schema", out var jsonSchema))
            {
                BsonDefaults.GuidRepresentation = GuidRepresentation.Standard;
                // todo: right db and collection?
                var database = client.GetDatabase(databaseName).WithWriteConcern(WriteConcern.WMajority);
                var validatorSchema = new BsonDocument("$jsonSchema", jsonSchema.Value.ToBsonDocument());
                database.CreateCollection(
                    collectionName,
                    new CreateCollectionOptions<BsonDocument>
                    {
                        Validator = new BsonDocumentFilterDefinition<BsonDocument>(validatorSchema)
                    });
            }
            else
            {
                // todo: need?
                base.CreateCollection(client, databaseName, collectionName, test, shared);
            }
        }

        protected override void DropCollection(MongoClient client, string databaseName, string collectionName, BsonDocument test, BsonDocument shared)
        {
            base.DropCollection(client, databaseName, collectionName, test, shared);

            if (shared.Contains("key_vault_data"))
            {
                var adminDatabase = client.GetDatabase("admin").WithWriteConcern(WriteConcern.WMajority); ;
                adminDatabase.DropCollection("datakeys");
            }
        }

        protected override void InsertData(IMongoClient client, string databaseName, string collectionName, BsonDocument shared)
        {
            base.InsertData(client, databaseName, collectionName, shared);

            if (shared.TryGetValue("key_vault_data", out var keyVaultData))
            {
                var adminDatabase = client.GetDatabase("admin");
                var keyVaultCollection = adminDatabase.GetCollection<BsonDocument>(
                    "datakeys",
                    new MongoCollectionSettings()
                    {
                        AssignIdOnInsert = false
                    });
                var keyVaultDocuments = keyVaultData.AsBsonArray?.Select(c => c.AsBsonDocument);
                keyVaultCollection.InsertMany(keyVaultDocuments);
            }
        }

        protected override void ModifyOperationIfNeeded(BsonDocument operation)
        {
            base.ModifyOperationIfNeeded(operation);
            if (!operation.Contains("object"))
            {
                operation.Add(new BsonElement("object", "collection"));
            }
        }

        protected override bool TryConfigureClientOption(MongoClientSettings settings, BsonElement option)
        {
            switch (option.Name)
            {
                case "autoEncryptOpts":
                    settings.AutoEncryptionOptions = ConfigureAutoEncryptionOptions(option.Value.AsBsonDocument);
                    break;
                default:
                    return false;
            }

            return true;
        }

        protected override void VerifyCollectionData(IEnumerable<BsonDocument> expectedDocuments)
        {
            base.VerifyCollectionData(
                expectedDocuments, 
                (actual, expected) => 
                {
                    ReplaceTypeAssertionWithActual(expected, actual);
                });
        }

        // private methods
        private AutoEncryptionOptions ConfigureAutoEncryptionOptions(BsonDocument autoEncryptOpts)
        {
            var keyVaultCollectionNamespace = new CollectionNamespace("admin", "datakeys");

            IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> kmsProviders = new ReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>(new Dictionary<string, IReadOnlyDictionary<string, object>>());
            var autoEncryptionOptions = new AutoEncryptionOptions(
                keyVaultCollectionNamespace,
                kmsProviders
                //,
                //keyVaultClient: keyVaultClient
                );

            foreach (var option in autoEncryptOpts.Elements)
            {
                switch (option.Name)
                {
                    case "kmsProviders":
                        autoEncryptionOptions = autoEncryptionOptions
                            .With(
                                kmsProviders: new Optional<IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>>(
                                    ParseKmsProviders(option.Value.AsBsonDocument)));
                        break;
                    case "schemaMap":
                        var schemaMaps = new Dictionary<string, BsonDocument>();
                        var schemaMapsDocument = option.Value.AsBsonDocument;
                        foreach (var schemaMapElement in schemaMapsDocument.Elements)
                        {
                            schemaMaps.Add(schemaMapElement.Name, schemaMapElement.Value.AsBsonDocument);
                        }
                        autoEncryptionOptions = autoEncryptionOptions.With(schemaMap: schemaMaps);
                        break;
                    default:
                        throw new Exception($"TODO: Unexpected auto encryption option {option.Name}.");
                }
            }

            return autoEncryptionOptions;
        }

        private IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> ParseKmsProviders(BsonDocument kmsProviders)
        {
            var providers = new Dictionary<string, IReadOnlyDictionary<string, object>>();
            foreach (var kmsProvider in kmsProviders.Elements)
            {
                switch (kmsProvider.Name)
                {
                    case "aws":
                        var kmsOptions = new Dictionary<string, object>();
                        if (kmsProvider.Value != new BsonDocument())
                        {
                            throw new NotSupportedException("TODO: delete after be ready.");
                        }
                        else
                        {
                            var awsRegion = Environment.GetEnvironmentVariable("AWS_REGION", EnvironmentVariableTarget.Machine) ?? "us-east-1";
                            var awsAccessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID", EnvironmentVariableTarget.Machine) ?? throw new Exception("The AWS_ACCESS_KEY_ID system variable should be configured on the machine.");
                            var awsSecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", EnvironmentVariableTarget.Machine) ?? throw new Exception("The AWS_SECRET_ACCESS_KEY system variable should be configured on the machine.");
                            //todo: remove constants dependencies
                            kmsOptions.Add("region", awsRegion);
                            kmsOptions.Add("accessKeyId", awsAccessKey);
                            kmsOptions.Add("secretAccessKey", awsSecretAccessKey);
                        }

                        providers.Add(kmsProvider.Name, kmsOptions);
                        break;
                }
            }

            return new ReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>(providers);
        }

        public void ReplaceTypeAssertionWithActual(BsonDocument expected, BsonDocument actual)
        {
            for (int i = 0; i < expected.ElementCount; i++)
            {
                var expectedElement = expected.ElementAt(i);
                BsonValue value = expectedElement.Value;
                if (value.IsBsonDocument)
                {
                    BsonDocument valueDocument = value.AsBsonDocument;
                    BsonValue actualValue = actual.GetValue(expectedElement.Name, null);
                    if (valueDocument.ElementCount == 1 && valueDocument.Select(c => c.Name).Single().Equals("$$type"))
                    {
                        var type = valueDocument["$$type"].AsString;
                        if (type.Equals("binData"))
                        {
                            // todo: 
                            // 1. check on bsonType?
                            // 2. One level higher?
                            expected[expectedElement.Name] = actualValue;
                        }
                        else if (type.Equals("long"))
                        {
                            expected[expectedElement.Name] = actualValue;
                        }
                        else
                        {
                            throw new NotSupportedException("Unsupported type: " + type);
                        }
                    }
                    else if (actualValue != null && actualValue.IsBsonDocument)
                    {
                        ReplaceTypeAssertionWithActual(valueDocument, actualValue.AsBsonDocument);
                    }
                    else
                    {
                        //throw new RuntimeException(String.format("Expecting '%s' as actual value but found '%s' ", valueDocument, actualValue));
                        // do nothing
                    }
                }
                else if (value.IsBsonArray)
                {
                    ReplaceTypeAssertionWithActual(value.AsBsonArray, actual[expectedElement.Name].AsBsonArray);
                }
            }
        }

        private void ReplaceTypeAssertionWithActual(BsonArray expected, BsonArray actual)
        {
            for (int i = 0; i < expected.Count(); i++)
            {
                BsonValue value = expected.ElementAt(i);
                if (value.IsBsonDocument)
                {
                    ReplaceTypeAssertionWithActual(value.AsBsonDocument, actual.ElementAt(i).AsBsonDocument);
                }
                else if (value.IsBsonArray)
                {
                    ReplaceTypeAssertionWithActual(value.AsBsonArray, actual.ElementAt(i).AsBsonArray);
                }
            }
        }

        // nested types
        public class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            // protected properties
            protected override string PathPrefix => "MongoDB.Driver.Tests.Specifications.client_side_encryption.tests.";

            // protected methods
            protected override IEnumerable<JsonDrivenTestCase> CreateTestCases(BsonDocument document)
            {
                foreach (var testCase in base.CreateTestCases(document))
                {
                    foreach (var async in new[] { false })
                    {
                        var name = $"{testCase.Name}:async={async}";
                        var test = testCase.Test.DeepClone().AsBsonDocument.Add("async", async);
                        yield return new JsonDrivenTestCase(name, testCase.Shared, test);
                    }
                }
            }
        }
    }
}
