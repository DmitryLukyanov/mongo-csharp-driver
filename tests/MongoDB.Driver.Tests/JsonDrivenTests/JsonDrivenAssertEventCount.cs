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
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public class JsonDrivenAssertEventCount : JsonDrivenTestRunnerTest
    {
        private readonly EventCapturer _eventCapturer;
        private int _count;
        private string _event;

        public JsonDrivenAssertEventCount(IJsonDrivenTestRunner testRunner, Dictionary<string, object> objectMap, EventCapturer eventCapturer) : base(testRunner, objectMap)
        {
            _eventCapturer = Ensure.IsNotNull(eventCapturer, nameof(eventCapturer)); 
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            // do nothing
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            // do nothing;
            return Task.FromResult(true);
        }

        public override void Assert()
        {
            var eventCondition = MapEventNameToCondition(_event);
            var actualCount = _eventCapturer
            .Events
            .Count(eventCondition);

            actualCount.Should().Be(_count);
        }

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "count":
                    _count = value.ToInt32();
                    return;

                case "event":
                    _event = value.ToString();
                    return;
            }

            base.SetArgument(name, value);
        }

        // private methods
        private Func<object, bool> MapEventNameToCondition(string eventName)
        {
            switch (eventName)
            {
                // TODO: Move from here?
                case "ServerMarkedUnknownEvent":
                    return @event =>
                        @event is ServerDescriptionChangedEvent serverDescriptionChangedEvent &&
                        serverDescriptionChangedEvent.NewDescription.Type == Core.Servers.ServerType.Unknown;

                case "PoolClearedEvent":
                    return @event => @event is ConnectionPoolClearedEvent;

                default:
                    throw new Exception("TODO: Unexpected event type.");
            }
        }
    }
}
