/* Copyright 2017-present MongoDB Inc.
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
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver.Core.Configuration;
using Xunit;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;

namespace MongoDB.Driver.Specifications.initial_dns_seedlist_discovery
{
    [Trait("Category", "ConnectionString")]
    public class TestRunner
    {
        [Theory]
        [ClassData(typeof(TestCaseFactory))]
        public void RunTestDefinition(JsonDrivenTestCase testCase)
        {
            var definition = testCase.Test;

            JsonDrivenHelper.EnsureAllFieldsAreValid(definition, "_path", "uri", "seeds", "hosts", "options", "comment", "error", "async");

            var uri = (string)testCase.Test["uri"];

            ConnectionString connectionString = null;
            Exception resolveException = null;

            if (definition["async"].ToBoolean())
            {
                resolveException = Record.Exception(
                    () =>
                        connectionString = new ConnectionString(uri)
                            .ResolveAsync()
                            .GetAwaiter()
                            .GetResult());
            }
            else
            {
                resolveException = Record.Exception(() => connectionString = new ConnectionString(uri).Resolve());
            }

            Assert(connectionString, resolveException, definition);
        }

        // private methods
        private void Assert(ConnectionString connectionString, Exception resolveException, BsonDocument definition)
        {
            if (definition.GetValue("error", false).ToBoolean())
            {
                resolveException.Should().BeOfType<MongoConfigurationException>();
            }
            else
            {
                if (resolveException != null)
                {
                    throw new AssertionException("failed to parse and resolve connection string", resolveException);
                }

                AssertValid(connectionString, definition);
            }
        }

        private void AssertValid(ConnectionString connectionString, BsonDocument definition)
        {
            var expectedSeeds = definition["seeds"].AsBsonArray.Select(x => x.ToString()).ToList();
            var actualSeeds = connectionString.Hosts.Select(ConvertEndPointToString).ToList();

            actualSeeds.ShouldAllBeEquivalentTo(expectedSeeds);

            if (definition.Contains("options"))
            {
                foreach (BsonElement option in definition["options"].AsBsonDocument)
                {
                    var expectedValue = ValueToString(option.Name, option.Value);

                    var optionName = GetOptionName(option);
                    var actualValue = Uri.UnescapeDataString(connectionString.GetOption(optionName).Split(',').Last());

                    actualValue.Should().Be(expectedValue);
                }
            }
        }

        private string GetOptionName(BsonElement option)
        {
            switch (option.Name.ToLowerInvariant())
            {
                case "ssl" when option.Value == true:
                    // Needs to restore some json tests which expect "ssl=true" option as autogenerated option for mongo+srv if no user defined "ssl" options, but now, we generate "tls=true"
                    return "tls";
                default:
                    return option.Name;
            }
        }

        private string ValueToString(string name, BsonValue value)
        {
            if (value == BsonNull.Value)
            {
                return null;
            }

            return value.ToString();
        }

        private string ConvertEndPointToString(EndPoint ep)
        {
            if (ep is DnsEndPoint)
            {
                var dep = (DnsEndPoint)ep;
                return $"{dep.Host}:{dep.Port}";
            }
            else if (ep is IPEndPoint)
            {
                var iep = (IPEndPoint)ep;
                return $"{iep.Address}:{iep.Port}";
            }

            throw new AssertionException($"Invalid endpoint: {ep}");
        }

        // nested types
        private class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            // protected properties
            protected override string PathPrefix => "MongoDB.Driver.Core.Tests.Specifications.initial_dns_seedlist_discovery.tests.";

            // protected methods
            protected override IEnumerable<JsonDrivenTestCase> CreateTestCases(BsonDocument document)
            {
                foreach (var async in new[] { false, true })
                {
                    var name = $"{GetTestCaseName(document, document, 0)}:async={async}";
                    var testCase = new JsonDrivenTestCase(name, document, document);
                    var test = testCase.Test.DeepClone().AsBsonDocument.Add("async", async);
                    yield return new JsonDrivenTestCase(name, testCase.Shared, test);
                }
            }
        }
    }
}
