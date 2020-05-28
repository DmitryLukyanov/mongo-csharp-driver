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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public abstract class JsonDrivenWithThread : JsonDrivenTestRunnerTest
    {
        protected string _name;
        protected readonly ConcurrentDictionary<string, Task> _tasks;

        public JsonDrivenWithThread(
            IJsonDrivenTestRunner testRunner,
            Dictionary<string, object> objectMap,
            ConcurrentDictionary<string, Task> tasks) : base(testRunner, objectMap)
        {
            _tasks = Ensure.IsNotNull(tasks, nameof(tasks));
        }

        protected Task CreateTask(Action action)
        {
            return Task.Factory.StartNew(
                action,
                CancellationToken.None,
                TaskCreationOptions.None,
                //new ThreadPerTaskScheduler()
                TaskScheduler.Default);
        }

        protected void AssignTask(Action action)
        {
            if (_tasks.ContainsKey(_name))
            {
                var taskAction = _tasks[_name];
                if (taskAction != null)
                {
                    throw new Exception($"Task {_name} must not be processed.");
                }
                else
                {
                    _tasks[_name] = CreateTask(action);
                }
            }
            else
            {
                throw new ArgumentException($"Task {_name} must be started before usage.");
            }
        }

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "name":
                    _name = value.ToString();
                    return;
            }

            base.SetArgument(name, value);
        }
    }
}
