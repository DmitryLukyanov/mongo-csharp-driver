﻿/* Copyright 2020-present MongoDB Inc.
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
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenStartThread : JsonDrivenWithThread
    {
        public JsonDrivenStartThread(IJsonDrivenTestRunner testRunner, Dictionary<string, object> objectMap, ConcurrentDictionary<string, Task> tasks) : base(testRunner, objectMap, tasks)
        {
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            _tasks.GetOrAdd(_name, (Task)null);
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            _tasks.GetOrAdd(_name, (Task)null);
            return Task.FromResult(true);
        }

        // public methods
        public override void Assert()
        {
            // do nothing
        }
    }
}