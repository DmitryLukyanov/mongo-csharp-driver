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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenWaitForEvent : JsonDrivenTestRunnerTest
    {
        private int _count;
        private string _event;
        private readonly EventCapturer _eventCapturer;

        public JsonDrivenWaitForEvent(
            IJsonDrivenTestRunner testRunner,
            Dictionary<string, object> objectMap,
            EventCapturer eventCapturer) : base(testRunner, objectMap)
        {
            _eventCapturer = eventCapturer;
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            Wait();
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            return Task.Run(() => Wait());
        }

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "count":
                    _count = value.ToInt32();     // TODO:!!!!!
                    return;

                case "event":
                    _event = value.ToString();
                    return;
            }

            base.SetArgument(name, value);
        }

        // private methods
        private Func<IEnumerable<object>, bool> MapEventNameToCondition(string eventName)
        {
            Func<object, bool> eventCondition = null;
            switch (eventName)
            {
                case "ServerMarkedUnknownEvent":
                    eventCondition = @event =>
                        @event is ServerDescriptionChangedEvent serverDescriptionChangedEvent &&
                        serverDescriptionChangedEvent.NewDescription.Type == ServerType.Unknown;
                    break;

                case "PoolClearedEvent":
                    eventCondition = @event => @event is ConnectionPoolClearedEvent;
                    break;

                default:
                    throw new Exception("TODO: Unexpected event type.");
            }

            return events => events.Any(eventCondition);
            //return events => events.Count(eventCondition) == _count;
        }

        private void Wait()
        {
            var eventCondition = MapEventNameToCondition(_event);
            var notifyTask = _eventCapturer.NotifyWhen(eventCondition);

            var events = _eventCapturer.Events.ToList().Where(c => c is ServerDescriptionChangedEvent).ToList().Cast<ServerDescriptionChangedEvent>();
            var types = events.Select(c => c.NewDescription.Type.ToString()).ToList();

            var timeout = TimeSpan.FromSeconds(200);
            var testFailedTimeout = Task.Delay(timeout, CancellationToken.None);
            var index = Task.WaitAny(notifyTask, testFailedTimeout);
            if (index != 0)
            {
                throw new Exception($"Waiting for {_event} exceeded the timeout {timeout}.");
            }
        }
    }
}
