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

using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Tests.Specifications.Runner;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.server_discovery_and_monitoring
{
    public class IntegrationTestRunner : MongoClientJsonDrivenTestRunnerBase//, IJsonDrivenTestRunner
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
                .InitializeEventCapturer(eventCapturer) // CommandStartedEvent is added inside
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

        // nested types
        private class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            //#region static
            //private static readonly string[] __ignoredTestNames =
            //{
            //    // https://jira.mongodb.org/browse/SPEC-1403
            //    "maxWireVersion.json:operation fails with maxWireVersion < 8"
            //};
            //#endregion

            // protected properties
            protected override string PathPrefix => "MongoDB.Driver.Tests.Specifications.server_discovery_and_monitoring.tests.integration.";

            // protected methods
            protected override IEnumerable<JsonDrivenTestCase> CreateTestCases(BsonDocument document)
            {
                var testCases = base.CreateTestCases(document);//.Where(test => !__ignoredTestNames.Any(ignoredName => test.Name.EndsWith(ignoredName)));
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
