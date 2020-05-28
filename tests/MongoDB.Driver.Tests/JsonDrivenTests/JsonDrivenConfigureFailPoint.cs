/* Copyright 2020–present MongoDB Inc.
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
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.TestHelpers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public sealed class JsonDrivenConfigureFailPoint : JsonDrivenTestRunnerTest
    {
        private BsonDocument _failCommand;

        public JsonDrivenConfigureFailPoint(IJsonDrivenTestRunner testRunner, Dictionary<string, object> objectMap)
            : base(testRunner, objectMap)
        {
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            var cluster = DriverTestConfiguration.Client.Cluster;
            _ = cluster.SelectServer(WritableServerSelector.Instance, CancellationToken.None);
            FailPoint.Configure(cluster, NoCoreSession.NewHandle(), _failCommand);
        }

        protected override async Task CallMethodAsync(CancellationToken cancellationToken)
        {
            var cluster = DriverTestConfiguration.Client.Cluster;
            _ = cluster.SelectServer(WritableServerSelector.Instance, CancellationToken.None);
            await Task.Run(() => FailPoint.Configure(cluster, NoCoreSession.NewHandle(), _failCommand)).ConfigureAwait(false); ;
        }

        protected override void AssertResult()
        {
            // do nothing
        }

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "failPoint":
                    _failCommand = (BsonDocument)value;
                    return;
            }

            base.SetArgument(name, value);
        }
    }
}
