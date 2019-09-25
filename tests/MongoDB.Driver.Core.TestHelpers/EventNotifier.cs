/* Copyright 2019-present MongoDB Inc.
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
using System.Threading.Tasks;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.TestHelpers
{
    public class EventNotifier
    {
        private readonly Func<IEnumerable<object>, bool> _condition;
        private readonly TaskCompletionSource<bool> _taskCompletionSource;

        public EventNotifier(EventCapturer eventCapturer, Func<IEnumerable<object>, bool> condition)
        {
            Ensure.IsNotNull(eventCapturer, nameof(eventCapturer));
            _taskCompletionSource = new TaskCompletionSource<bool>();
            _condition = Ensure.IsNotNull(condition, nameof(condition));
            InitializeTask(eventCapturer);
        }

        public Task TaskCompletion => _taskCompletionSource.Task;

        // private methods
        private void AddEventAction(IEnumerable<object> events, object @event)
        {
            TriggerNotification(@events);
        }

        private void InitializeTask(EventCapturer eventCapturer)
        {
            eventCapturer.AddEventAction += AddEventAction;

            lock (eventCapturer._lock)
            {
                TriggerNotification(eventCapturer.Events);
            }
        }

        private void TriggerNotification(IEnumerable<object> events)
        {
            if (_condition(events))
            {
                _taskCompletionSource.TrySetResult(true);
            }
        }
    }
}
