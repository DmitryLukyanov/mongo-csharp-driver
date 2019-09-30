/* Copyright 2018-present MongoDB Inc.
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
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Authentication;
using MongoDB.Driver.Core.Configuration;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.auth
{
    public class TestRunner
    {
        [SkippableTheory]
        [ClassData(typeof(TestCaseFactory))]
        public void RunTestDefinition(JsonDrivenTestCase testCase)
        {
            var definition = testCase.Test;
            JsonDrivenHelper.EnsureAllFieldsAreValid(definition, "description", "uri", "valid", "credential");

            MongoCredential mongoCredential = null;
            Exception parseException = null;
            try
            {
                var connectionString = new ConnectionString((string)definition["uri"]);
                mongoCredential = MongoClientSettings.FromConnectionString(connectionString.ToString()).Credential;
            }
            catch (Exception ex)
            {
                parseException = ex;
            }

            if (parseException == null)
            {
                AssertValid(mongoCredential, definition);
            }
            else
            {
                AssertInvalid(parseException, definition);
            }
        }

        private void AssertValid(MongoCredential mongoCredential, BsonDocument definition)
        {
            if (!definition["valid"].ToBoolean())
            {
                throw new AssertionException($"The connection string '{definition["uri"]}' should be invalid.");
            }

            var credential = definition["credential"] as BsonDocument;
            if (credential != null)
            {
                JsonDrivenHelper.EnsureAllFieldsAreValid(credential, "username", "password", "source", "mechanism", "mechanism_properties");

                mongoCredential.Source.Should().Be(ValueToString(credential["source"]));
                mongoCredential.Username.Should().Be(ValueToString(credential["username"]));
#pragma warning disable 618
                mongoCredential.Password.Should().Be(ValueToString(credential["password"]));
#pragma warning restore 618
                mongoCredential.Mechanism.Should().Be(ValueToString(credential["mechanism"]));

                if (credential.TryGetValue("mechanism_properties", out var mechanismProperties))
                {
                    if (mechanismProperties.IsBsonNull)
                    {
                        // todo:
                    }
                    else
                    {
                        foreach (var mechanismProperty in mechanismProperties.AsBsonDocument)
                        {
                            var mechanismName = mechanismProperty.Name;
                            switch (mechanismName)
                            {
                                case "SERVICE_NAME":
                                    var serviceName = mongoCredential.ToAuthenticator()._mechanism()._serviceName();
                                    serviceName.Should().Be(ValueToString(mechanismProperty.Value));
                                    break;
                                case "CANONICALIZE_HOST_NAME":
                                    var canonicalizeHostName = mongoCredential.ToAuthenticator()._mechanism()._canonicalizeHostName();
                                    canonicalizeHostName.Should().Be(mechanismProperty.Value.ToBoolean());
                                    break;
                                default:
                                    throw new Exception($"Invalid mechanism property {mechanismName}.");
                            }
                        }
                    }
                }
            }
        }

        private void AssertInvalid(Exception ex, BsonDocument definition)
        {
            if (definition["valid"].ToBoolean())
            {
                throw new AssertionException($"The connection string '{definition["uri"]}' should be valid.", ex);
            }
        }

        private string ValueToString(BsonValue value)
        {
            return value == BsonNull.Value ? null : value.ToString();
        }

        private class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            protected override string PathPrefix => "MongoDB.Driver.Tests.Specifications.auth.tests.";
        }
    }

    internal static class GssapiMechanismReflector
    {
        public static bool _canonicalizeHostName(this object obj) => (bool)Reflector.GetFieldValue(obj, nameof(_canonicalizeHostName));
        public static string _serviceName(this object obj) => (string)Reflector.GetFieldValue(obj, nameof(_serviceName));
    }

    internal static class AuthenticatorReflector
    {
        public static object _mechanism(this IAuthenticator obj) => Reflector.GetFieldValue(obj, nameof(_mechanism));
    }
}
