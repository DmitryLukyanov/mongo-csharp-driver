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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.TestHelpers;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using MongoDB.Driver.TestHelpers;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.server_discovery_and_monitoring
{
    public class ServerDiscoveryAndMonitoringProseTests
    {
        [Fact]
        public void Streaming_protocol_test()
        {
            var eventCapturer = new EventCapturer().Capture<ServerHeartbeatStartedEvent>();

            var heartbeatInterval = 500;
            using (var client = CreateClient(eventCapturer, heartbeatInterval))
            {
                eventCapturer.Clear();
                for (int attempt = 1; attempt <= 5; attempt++)
                {
                    var timeout = TimeSpan.FromMilliseconds(550); // a bit bigger than heartbeatInterval
                    var notifyTask = eventCapturer.NotifyWhen(events => events.Any(e => events.Count() == attempt));
                    var index = Task.WaitAny(notifyTask, Task.Delay(timeout));
                    if (index != 0)
                    {
                        throw new Exception($"The expected heartbeat interval is {heartbeatInterval} ms, but the attempt #{attempt} took more than {timeout.Milliseconds} ms.");
                    }
                }
            }
        }

        [SkippableFact]
        public void RoundTimeTrip_test()
        {
            RequireServer.Check().Supports(Feature.StreamingIsMaster);

            var eventCapturer = new EventCapturer().Capture<ServerDescriptionChangedEvent>();

            var heartbeatInterval = 500;
            using (var client = CreateClient(eventCapturer, heartbeatInterval, applicationName: "streamingRttTest"))
            {
                // Run a find command to wait for the server to be discovered.
                _ = client
                    .GetDatabase(DriverTestConfiguration.DatabaseNamespace.DatabaseName)
                    .GetCollection<BsonDocument>(DriverTestConfiguration.CollectionNamespace.CollectionName)
                    .Find(FilterDefinition<BsonDocument>.Empty);

                // Sleep for 2 seconds. This must be long enough for multiple heartbeats to succeed.
                Thread.Sleep(TimeSpan.FromSeconds(2));

                foreach (ServerDescriptionChangedEvent @event in eventCapturer.Events.ToList())
                {
                    @event.NewDescription.AverageRoundTripTime.Should().NotBe(default);
                }

                var failPointCommand = BsonDocument.Parse(
                    @"{
                        configureFailPoint : 'failCommand',
                        mode : { times : 1000 },
                        data :
                        {
                            failCommands : [ 'isMaster' ],
                            blockConnection : true,
                            blockTimeMS : 500,
                            appName : 'streamingRttTest'
                        }
                    }");

                using (FailPoint.Configure(client.Cluster, NoCoreSession.NewHandle(), failPointCommand))
                {
                    // Note that the Server Description Equality rule means that ServerDescriptionChangedEvents will not be published.
                    // So we use reflection to obtain the latest RTT instead.
                    var server = client.Cluster.SelectServer(WritableServerSelector.Instance, CancellationToken.None);
                    var roundTripTimeMonitor = server._monitor()._roundTripTimeMonitor();
                    var expectedRoundTripTime = TimeSpan.FromMilliseconds(250);
                    var timeout = TimeSpan.FromSeconds(30); // should not be reached without a driver bug
                    SpinWait.SpinUntil(() => roundTripTimeMonitor.ExponentiallyWeightedMovingAverage.Average >= expectedRoundTripTime, timeout).Should().BeTrue();
                }
            }
        }

        // private methods
        private DisposableMongoClient CreateClient(EventCapturer eventCapturer, int heartbeatInterval, string applicationName = null)
        {
            var mongoclientSettings = new MongoClientSettings
            {
                ApplicationName = applicationName,
                ClusterConfigurator = builder => builder.Subscribe(eventCapturer),
                HeartbeatInterval = TimeSpan.FromMilliseconds(heartbeatInterval)
            };
            return DriverTestConfiguration.CreateDisposableClient(mongoclientSettings);
        }
    }

    internal static class ServerRelfector
    {
        public static IServerMonitor _monitor(this IServer server)
        {
            return (IServerMonitor)Reflector.GetFieldValue(server, nameof(_monitor));
        }

        public static IRoundTripTimeMonitor _roundTripTimeMonitor(this IServerMonitor serverMonitor)
        {
            return (IRoundTripTimeMonitor)Reflector.GetFieldValue(serverMonitor, nameof(_roundTripTimeMonitor));
        }
    }
}
