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

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenWaitForPrimaryChange : JsonDrivenTestRunnerTest
    {
        private readonly IMongoClient _client;
        private readonly JsonDrivenRecordPrimaryContext _context;
        private TimeSpan _timeout;

        public JsonDrivenWaitForPrimaryChange(IJsonDrivenTestsContext testContext, IJsonDrivenTestRunner testRunner, IMongoClient client, Dictionary<string, object> objectMap) : base(testRunner, objectMap)
        {
            _client = Ensure.IsNotNull(client, nameof(client));
            _context = (JsonDrivenRecordPrimaryContext)Ensure.IsNotNull(testContext, nameof(testContext));
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            var changedPrimary = WaitPrimaryChange(_context.RecordedPrimary);
            if (changedPrimary != null)
            {
                _context.RecordedPrimary = changedPrimary;
            }
            else
            {
                throw new Exception("The primary has not been changed or timeout has been exceeded.");
            }
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            var changedPrimary = WaitPrimaryChange(_context.RecordedPrimary);
            if (changedPrimary != null)
            {
                _context.RecordedPrimary = changedPrimary;
            }
            else
            {
                throw new Exception("The primary has not been changed or timeout has been exceeded.");
            }
            return Task.FromResult(true);
        }

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "timeoutMS":
                    _timeout = TimeSpan.FromMilliseconds(value.ToInt32());
                    return;
            }

            base.SetArgument(name, value);
        }

        // private methods
        private EndPoint GetPrimary()
        {
            foreach (var server in _client.Cluster.Description.Servers)
            {
                if (server.Type == ServerType.ReplicaSetPrimary)
                {
                    return server.EndPoint;
                }
            }

            return null;
        }

        private EndPoint WaitPrimaryChange(EndPoint previousPrimary)
        {
            var cancelationTokenSource = new CancellationTokenSource(_timeout);

            var currentPrimary = GetPrimary();
            while ((
                currentPrimary == null || // temporary case when all servers are secondary
                currentPrimary == previousPrimary) &&
                !cancelationTokenSource.IsCancellationRequested) // timeout is exceeded
            {
                currentPrimary = GetPrimary();
            }
            return currentPrimary != previousPrimary ? currentPrimary : null;
        }
    }
}
