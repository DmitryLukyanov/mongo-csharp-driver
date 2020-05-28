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
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenWaitForPrimaryChange : JsonDrivenTestRunnerTest
    {
        private CancellationTokenSource waitCancellationTokenSource;
        private readonly IMongoClient _client;

        private JsonDrivenRecordPrimaryContext _context;
        private TimeSpan _timeout;

        public JsonDrivenWaitForPrimaryChange(IJsonDrivenTestContext testContext, IJsonDrivenTestRunner testRunner, IMongoClient client, Dictionary<string, object> objectMap) : base(testContext, testRunner, objectMap)
        {
            _client = client;
            _context = (JsonDrivenRecordPrimaryContext)_testContext;
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            waitCancellationTokenSource = new CancellationTokenSource(_timeout);
            var changedPrimary = WaitPrimaryChange(_context.RecordedPrimary, waitCancellationTokenSource.Token);
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
            waitCancellationTokenSource = new CancellationTokenSource(_timeout);
            var changedPrimary = WaitPrimaryChange(_context.RecordedPrimary, waitCancellationTokenSource.Token);
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

        //// public methods
        //public override void Assert()
        //{
        //    // do nothing
        //}

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

        private EndPoint WaitPrimaryChange(EndPoint previousPrimary, CancellationToken cancellationToken)
        {
            var currentPrimary = GetPrimary();
            while (currentPrimary == previousPrimary && !cancellationToken.IsCancellationRequested)
            {
                currentPrimary = GetPrimary();
            }
            return currentPrimary != previousPrimary ? currentPrimary : null;
        }
    }
}
