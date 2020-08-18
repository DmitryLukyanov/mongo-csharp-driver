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
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.TestHelpers;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using Xunit;

namespace MongoDB.Driver.Tests.Jira
{
    public class CSharp3168Tests
    {
        [SkippableTheory]
        [ParameterAttributeData]
        public void Test_sync([Values(1000, 2000, 3000)] int timing)
        {
            var appName = "app_sync_3168" + timing;
            RequireServer.Check().VersionGreaterThanOrEqualTo(new SemanticVersion(4, 4, 0));

            var eventCapturer = new EventCapturer()
                .Capture<CommandStartedEvent>()
                .Capture<CommandSucceededEvent>()
                .Capture<ConnectionReceivingMessageEvent>() // contains more accurate timings
                .Capture<ConnectionReceivedMessageEvent>(); // contains more accurate timings

            var timeoutCommand = BsonDocument.Parse($@"
            {{
                configureFailPoint : 'failCommand',
                mode : {{
                    times : 1
                }},
                data : {{
                    failCommands : ['ping'],
                    appName : '{appName}',
                    blockConnection : true,
                    blockTimeMS : {timing}
                }}
            }}");
            var mongoClientSettings = DriverTestConfiguration.GetClientSettings().Clone();
            mongoClientSettings.ClusterConfigurator = (builder) => builder.Subscribe(eventCapturer);
            mongoClientSettings.ApplicationName = appName;
            mongoClientSettings.SocketTimeout = TimeSpan.FromMilliseconds(750);
            using (var client = DriverTestConfiguration.CreateDisposableClient(mongoClientSettings))
            {
                using (var failPoint = FailPoint.Configure(client.Cluster, NoCoreSession.NewHandle(), timeoutCommand))
                {
                    var exception = Record.Exception(() => client.GetDatabase("db").RunCommand<BsonDocument>("{ ping : 1 }"));
                    if (exception == null)
                    {
                        throw new Exception("No exception. Events_" + string.Join(",", eventCapturer.Events));
                    }
                    else
                    {
                        throw new Exception("Thrown exception:" + exception.ToString() + ". Events_" + string.Join(",", eventCapturer.Events));
                    }
                }
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Test_async([Values(1000, 2000, 3000)] int timing)
        {
            var appName = "app_async_3168" + timing;
            RequireServer.Check().VersionGreaterThanOrEqualTo(new SemanticVersion(4, 4, 0));

            var eventCapturer = new EventCapturer()
                .Capture<CommandStartedEvent>()
                .Capture<CommandSucceededEvent>()
                .Capture<ConnectionReceivingMessageEvent>()
                .Capture<ConnectionReceivedMessageEvent>();

            var timeoutCommand = BsonDocument.Parse($@"
            {{
                configureFailPoint : 'failCommand',
                mode : {{
                    times : 1
                }},
                data : {{
                    failCommands : ['ping'],
                    appName : '{appName}',
                    blockConnection : true,
                    blockTimeMS : {timing}
                }}
            }}");
            var mongoClientSettings = DriverTestConfiguration.GetClientSettings().Clone();
            mongoClientSettings.ClusterConfigurator = (builder) => builder.Subscribe(eventCapturer);
            mongoClientSettings.ApplicationName = appName;
            mongoClientSettings.SocketTimeout = TimeSpan.FromMilliseconds(750);
            using (var client = DriverTestConfiguration.CreateDisposableClient(mongoClientSettings))
            {
                using (var failPoint = FailPoint.Configure(client.Cluster, NoCoreSession.NewHandle(), timeoutCommand))
                {
                    var exception = Record.Exception(() => client.GetDatabase("db").RunCommandAsync<BsonDocument>("{ ping : 1 }").GetAwaiter().GetResult());
                    if (exception == null)
                    {
                        throw new Exception("No exception. Events_" + string.Join(",", eventCapturer.Events));
                    }
                    else
                    {
                        throw new Exception("Thrown exception:" + exception.ToString() + ". Events_" + string.Join(",", eventCapturer.Events));
                    }
                }
            }
        }
    }
}
