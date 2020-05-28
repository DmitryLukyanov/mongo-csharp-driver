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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenRecordPrimaryContext : IJsonDrivenTestContext
    {
        public EndPoint RecordedPrimary { get; set; }
    }

    public class JsonDrivenRecordPrimary : JsonDrivenTestRunnerTest
    {
        private readonly IMongoClient _client;
        private readonly new JsonDrivenRecordPrimaryContext _testContext;

        public JsonDrivenRecordPrimary(IJsonDrivenTestContext testContext, IJsonDrivenTestRunner testRunner, IMongoClient client, Dictionary<string, object> objectMap)
            : base(testContext, testRunner, objectMap)
        {
            //TODO JsonDrivenHelper.EnsureAllFieldsAreValid(document, expectedNames);
            //TODO: Ensure
            _client = client;
            _testContext = (JsonDrivenRecordPrimaryContext)testContext;
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            _testContext.RecordedPrimary = GetPrimary();
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            _testContext.RecordedPrimary = GetPrimary();
            return Task.FromResult(true);
        }

        public override void Assert()
        {
            // do nothing
        }

        // private methods
        private EndPoint GetPrimary()
        {
            var clusterDescription = _client.Cluster.Description;
            foreach (var server in clusterDescription.Servers)
            {
                if (server.Type == ServerType.ReplicaSetPrimary)
                {
                    return server.EndPoint;
                }
            }
            return null;
        }
    }
}
