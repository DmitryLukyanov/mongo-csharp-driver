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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenWaitForThread : JsonDrivenWithThread
    {
        public JsonDrivenWaitForThread(
            IJsonDrivenTestRunner testRunner,
            Dictionary<string, object> objectMap,
            ConcurrentDictionary<string, Task> tasks) : base(testRunner, objectMap, tasks)
        {
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            if (_tasks.TryGetValue(_name, out var task) && task != null)
            {
                task.GetAwaiter().GetResult();
            }
            else
            {
                throw new Exception($"The task {_name} must be configured before waiting.");
            }
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            if (_tasks.TryGetValue(_name, out var task) && task != null)
            {
                task.GetAwaiter().GetResult();
            }
            else
            {
                throw new Exception($"The task {_name} must be configured before waiting.");
            }
            return Task.FromResult(true);
        }

        // public methods
        public override void Assert()
        {
            // do nothing
        }
    }
}