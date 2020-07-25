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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.ConnectionPools;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Helpers;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.WireProtocol.Messages;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using Moq;
using Xunit;
namespace MongoDB.Driver.Core.Tests.Jira
{
    public class Help16369Tests
    {
        private readonly static ClusterConnectionMode __clusterConnectionMode = ClusterConnectionMode.Sharded;
        private readonly static ClusterId __clusterId = new ClusterId();
        private readonly static EndPoint __endPoint1 = new DnsEndPoint("localhost", 27017);
        private readonly static EndPoint __endPoint2 = new DnsEndPoint("localhost", 27018);
        private readonly static ServerId __serverId1 = new ServerId(__clusterId, __endPoint1);
        private readonly static ServerId __serverId2 = new ServerId(__clusterId, __endPoint2);
        private readonly static ServerMonitorSettings __serverMonitorSettings = new ServerMonitorSettings(
            connectTimeout: TimeSpan.FromMilliseconds(1),
            heartbeatInterval: TimeSpan.FromMilliseconds(100));
        private readonly static ServerSettings __serverSettings = new ServerSettings(__serverMonitorSettings.HeartbeatInterval);
        private readonly static ClusterSettings __clusterSettings = new ClusterSettings(
            connectionMode: ClusterConnectionMode.Sharded,
            serverSelectionTimeout: TimeSpan.FromMinutes(1), // just to not worry about it
            endPoints: new[]
            {
                __endPoint1,
                __endPoint2
            });
        private readonly static (ServerId ServerId, EndPoint Endpoint)[] __serverInfoCollection = new[]
        {
            (__serverId1, __endPoint1),
            (__serverId2, __endPoint2),
        };

        [SkippableFact]
        public void Ensure_command_netwwork_error_is_correctly_handled_before_hadnshake()
        {
            var eventCapturer = new EventCapturer().Capture<ServerDescriptionChangedEvent>();

            var hasNetworkErrorBeenTriggered = new TaskCompletionSource<bool>();

            var connectionPoolFactory = CreateAndSetupConnectionPoolFactory(__serverInfoCollection);
            var serverMonitorConnectionFactory = CreateAndSetupServerMonitorConnectionFactory(hasNetworkErrorBeenTriggered, __serverInfoCollection);
            var serverMonitorFactory = new ServerMonitorFactory(__serverMonitorSettings, serverMonitorConnectionFactory, eventCapturer);

            var serverFactory = new ServerFactory(__clusterConnectionMode, __serverSettings, connectionPoolFactory, serverMonitorFactory, eventCapturer);

            EndPoint initialSelectedEndpoint = null;
            using (var cluster = new MultiServerCluster(__clusterSettings, serverFactory, eventCapturer))
            {
                cluster._clusterId(__clusterId);

                // 0. Initial heartbeat via `connection.Open`
                // The next isMaster response will be delayed because the Task.WaitAny in the mock.Returns
                cluster.Initialize();
                var selectedServer = SelectServer(cluster);
                initialSelectedEndpoint = selectedServer.EndPoint;

                // make sure the next isMaster check has been called
                Thread.Sleep(__serverMonitorSettings.HeartbeatInterval + TimeSpan.FromMilliseconds(20));

                // 1. Trigger the command network error BEFORE handshake
                var exception = Record.Exception(() => selectedServer.GetChannel(CancellationToken.None));
                var e = exception.Should().BeOfType<MongoConnectionException>().Subject;
                e.Message.Should().Be("DnsException");

                // 2. Waiting for the isMaster check
                hasNetworkErrorBeenTriggered.SetResult(true); // unlock the in-progress isMaster response
                Thread.Sleep(100); // make sure the delayed isMaster check had time to change description if there is a bug
                var knownServers = cluster.Description.Servers.Where(s => s.Type != ServerType.Unknown);
                if (knownServers.Select(s => s.EndPoint).Contains(initialSelectedEndpoint))
                {
                    throw new Exception($"The type of failed server {initialSelectedEndpoint} has not been changed to Unknown.");
                }

                // ensure that a new server can be elected
                selectedServer = SelectServer(cluster);

                // ensure that the selected server is not the same as the initial
                GetPort(initialSelectedEndpoint).Should().NotBe(GetPort(selectedServer.EndPoint));

                // wait for the next isMaster check will fail during opening connection (the 4th event).
                // Since the Dns error is still expected when we create a new connection for the heartbeat - we emulate it
                eventCapturer.WaitForOrThrowIfTimeout(events => events.Count() >= 4, TimeSpan.FromSeconds(5));
            }

            var initialHeartbeatEvents = new[]
            {
                // endpoints can be in random order
                eventCapturer.Next().Should().BeOfType<ServerDescriptionChangedEvent>().Subject,
                eventCapturer.Next().Should().BeOfType<ServerDescriptionChangedEvent>().Subject
            }
            .OrderBy(c => GetPort(c.NewDescription.EndPoint))
            .ToList();
            AssertEvent(initialHeartbeatEvents[0], __endPoint1, ServerType.ShardRouter, "Heartbeat");
            AssertEvent(initialHeartbeatEvents[1], __endPoint2, ServerType.ShardRouter, "Heartbeat");

            AssertNextEvent(eventCapturer, initialSelectedEndpoint, ServerType.Unknown, "InvalidatedBecause:ChannelException during handshake");
            AssertNextEvent(eventCapturer, initialSelectedEndpoint, ServerType.Unknown, "Heartbeat", typeof(MongoConnectionException));
            AssertNextEvent(eventCapturer, initialSelectedEndpoint, ServerType.Unknown, "Heartbeat", typeof(Exception));
            eventCapturer.Any().Should().BeFalse();

            int GetPort(EndPoint endpoint) => ((DnsEndPoint)endpoint).Port;

            IServer SelectServer(ICluster cluster)
            {
                var selectServerTask = cluster.SelectServerAsync(WritableServerSelector.Instance, CancellationToken.None);
                // make sure that there is no infinite  waiting
                WaitForTaskOrTimeout(selectServerTask, TimeSpan.FromSeconds(10), "server selection");
                return selectServerTask.GetAwaiter().GetResult();
            }
        }
        // private method
        private void AssertEvent(ServerDescriptionChangedEvent @event, EndPoint expectedEndPoint, ServerType expectedServerType, string expectedReasonStart, Type exceptionType = null)
        {
            @event.NewDescription.EndPoint.Should().Be(expectedEndPoint);
            @event.NewDescription.Type.Should().Be(expectedServerType);
            @event.NewDescription.State.Should().Be(expectedServerType == ServerType.Unknown ? ServerState.Disconnected : ServerState.Connected);
            if (exceptionType != null)
            {
                @event.NewDescription.HeartbeatException.Should().BeOfType(exceptionType);
            }
            else
            {
                @event.NewDescription.HeartbeatException.Should().BeNull();
            }
            @event.NewDescription.ReasonChanged.Should().StartWith(expectedReasonStart);
        }

        private void AssertNextEvent(EventCapturer eventCapturer, EndPoint expectedEndPoint, ServerType expectedServerType, string expectedReasonStart, Type exceptionType = null)
        {
            var @event = eventCapturer.Next().Should().BeOfType<ServerDescriptionChangedEvent>().Subject;
            AssertEvent(@event, expectedEndPoint, expectedServerType, expectedReasonStart, exceptionType);
        }

        private IConnectionPoolFactory CreateAndSetupConnectionPoolFactory(params (ServerId ServerId, EndPoint Endpoint)[] serverInfoCollection)
        {
            var mockConnectionPoolFactory = new Mock<IConnectionPoolFactory>();

            foreach (var serverInfo in serverInfoCollection)
            {
                var mockConnectionPool1 = new Mock<IConnectionPool>();
                SetupConnectionPoolFactory(mockConnectionPoolFactory, mockConnectionPool1.Object, serverInfo.ServerId, serverInfo.Endpoint);

                var mockServerConnection1 = new Mock<IConnectionHandle>();
                SetupConnection(mockServerConnection1, serverInfo.ServerId);

                SetupConnectionPool(mockConnectionPool1, mockServerConnection1.Object);
            }

            return mockConnectionPoolFactory.Object;

            void SetupConnection(Mock<IConnectionHandle> mockConnectionHandle, ServerId serverId)
            {
                mockConnectionHandle.SetupGet(c => c.ConnectionId).Returns(new ConnectionId(serverId));
                mockConnectionHandle
                    .Setup(c => c.Open(It.IsAny<CancellationToken>()))
                    .Throws(CreateDnsException(mockConnectionHandle.Object.ConnectionId)); // throw command dns exception
                mockConnectionHandle
                    .Setup(c => c.OpenAsync(It.IsAny<CancellationToken>()))
                    .Throws(CreateDnsException(mockConnectionHandle.Object.ConnectionId)); // throw command dns exception
            }

            void SetupConnectionPool(Mock<IConnectionPool> mockConnectionPool, IConnectionHandle connection)
            {
                mockConnectionPool
                    .Setup(c => c.AcquireConnection(It.IsAny<CancellationToken>()))
                    .Returns(connection);
                mockConnectionPool
                    .Setup(c => c.AcquireConnectionAsync(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(connection));
            }

            void SetupConnectionPoolFactory(Mock<IConnectionPoolFactory> mockFactory, IConnectionPool connectionPool, ServerId serverId, EndPoint endPoint)
            {
                mockFactory.Setup(c => c.CreateConnectionPool(serverId, endPoint)).Returns(connectionPool);
            }
        }

        private IConnectionFactory CreateAndSetupServerMonitorConnectionFactory(TaskCompletionSource<bool> hasNetworkErrorBeenTriggered, params (ServerId ServerId, EndPoint Endpoint)[] serverInfoCollection)
        {
            var mockConnectionFactory = new Mock<IConnectionFactory>();

            foreach (var serverInfo in serverInfoCollection)
            {
                var mockServerMonitorConnection = new Mock<IConnection>();
                SetupConnection(mockServerMonitorConnection, serverInfo.ServerId);
                mockConnectionFactory
                    .Setup(c => c.CreateConnection(serverInfo.ServerId, serverInfo.Endpoint))
                    .Returns(mockServerMonitorConnection.Object);
            }

            return mockConnectionFactory.Object;

            void SetupConnection(Mock<IConnection> mockConnection, ServerId serverId)
            {
                var connectionId = new ConnectionId(serverId);
                var isMasterDocument = new BsonDocument
                {
                    { "ok", 1 },
                    { "minWireVersion", 6 },
                    { "maxWireVersion", 7 },
                    { "msg", "isdbgrid" },
                    { "version", "2.6" }
                };

                mockConnection.SetupGet(c => c.ConnectionId).Returns(new ConnectionId(serverId));

                // Open methods
                mockConnection
                    .SetupSequence(c => c.Open(It.IsAny<CancellationToken>()))
                    .Pass() // the first isMaster configuration passes
                    .Pass() // RTT. Just in case since RTT is only in async code path
                    .Throws(CreateDnsException(mockConnection.Object.ConnectionId)) // the dns exception. Should be triggered after Invalidate
                    .Throws(new Exception()); // generate not network error to trigger heartbeat waiting
                mockConnection
                    .SetupSequence(c => c.OpenAsync(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true)) // the first isMaster configuration passes
                    .Returns(Task.FromResult(true)) // RTT
                    .Throws(CreateDnsException(mockConnection.Object.ConnectionId)) // the dns exception. Should be triggered after Invalidate
                    .Throws(new Exception()); // generate not network error to trigger heartbeat waiting
                // Receive methods
                Func<ReplyMessage<RawBsonDocument>> commandResponseAction =
                    () =>
                    {
                        return MessageHelper.BuildReply(new RawBsonDocument(isMasterDocument.ToBson()));
                    };
                mockConnection
                    .Setup(c => c.ReceiveMessage(It.IsAny<int>(), It.IsAny<IMessageEncoderSelector>(), It.IsAny<MessageEncoderSettings>(), It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        WaitForTaskOrTimeout(hasNetworkErrorBeenTriggered.Task, TimeSpan.FromMinutes(1), "network error");
                        return commandResponseAction();
                    });
                mockConnection
                    .Setup(c => c.ReceiveMessageAsync(It.IsAny<int>(), It.IsAny<IMessageEncoderSelector>(), It.IsAny<MessageEncoderSettings>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(() =>
                    {
                        WaitForTaskOrTimeout(hasNetworkErrorBeenTriggered.Task, TimeSpan.FromMinutes(1), "network error");
                        return commandResponseAction();
                    });
                mockConnection
                    .SetupGet(c => c.Description)
                    .Returns(
                        new ConnectionDescription(
                            mockConnection.Object.ConnectionId,
                            new IsMasterResult(isMasterDocument),
                            new BuildInfoResult(new BsonDocument("version", "2.6")))); // no streaming
            }
        }

        private Exception CreateDnsException(ConnectionId connectionId)
        {
            return new MongoConnectionException(connectionId, "DnsException");
        }

        private void WaitForTaskOrTimeout(Task task, TimeSpan timeout, string testTarget)
        {
            var index = Task.WaitAny(task, Task.Delay(timeout));
            if (index != 0)
            {
                throw new Exception($"The waiting for {testTarget} is exceeded timeout {timeout}.");
            }
        }
    }
}
