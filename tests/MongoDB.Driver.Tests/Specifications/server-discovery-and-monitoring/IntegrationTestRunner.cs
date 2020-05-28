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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Tests.JsonDrivenTests;
using MongoDB.Driver.Tests.Specifications.Runner;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.server_discovery_and_monitoring
{
    public class IntegrationTestRunner : MongoClientJsonDrivenTestRunnerBase
    {
        protected override string[] ExpectedTestColumns => new[] { "description", "failPoint", "clientOptions", "operations", "expectations", "outcome", "async" };

        [SkippableTheory]
        [ClassData(typeof(TestCaseFactory))]
        public void Run(JsonDrivenTestCase testCase)
        {
            SetupAndRunTest(testCase);
        }

        protected override EventCapturer InitializeEventCapturer(EventCapturer eventCapturer)
        {
            return base
                .InitializeEventCapturer(eventCapturer) // CommandStartedEvent is added by default
                .Capture<ServerDescriptionChangedEvent>()
                .Capture<ConnectionPoolClearedEvent>();
        }

        protected override List<object> ExtractEventsForAsserting(EventCapturer eventCapturer)
        {
            return base
                .ExtractEventsForAsserting(eventCapturer)
                .Where(@event => @event is CommandStartedEvent) // apply events asserting only for this event
                .ToList();
        }

        protected override JsonDrivenTestFactory CreateJsonFactory(IMongoClient client, string databaseName, string collectionName, Dictionary<string, object> objectMap, EventCapturer eventCapturer)
        {
            return new JsonDrivenTestFactory(
                testRunner: null,
                client,
                databaseName,
                collectionName,
                bucketName: null,
                objectMap,
                eventCapturer,
                new ConcurrentDictionary<string, Task>());
        }

        // nested types
        private class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            // protected properties
            protected override string PathPrefix => "MongoDB.Driver.Tests.Specifications.server_discovery_and_monitoring.tests.integration.";

            // protected methods
            protected override IEnumerable<JsonDrivenTestCase> CreateTestCases(BsonDocument document)
            {
                var testCases = base.CreateTestCases(document);
                foreach (var testCase in testCases)
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
