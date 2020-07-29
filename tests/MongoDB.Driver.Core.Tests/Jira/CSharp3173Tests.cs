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
    public class CSharp3173Tests
    {
        private readonly static ClusterConnectionMode __clusterConnectionMode = ClusterConnectionMode.Sharded;
        private readonly static ClusterId __clusterId = new ClusterId();
        private readonly static EndPoint __endPoint1 = new DnsEndPoint("localhost", 27017);
        private readonly static EndPoint __endPoint2 = new DnsEndPoint("localhost", 27018);
        private readonly static IServerSelector __endPoint1ServerSelector = new EndPointServerSelector(__endPoint1);
        private readonly static IServerSelector __endpoint1AndWritableServerSelector = new CompositeServerSelector(
            new[]
            {
                WritableServerSelector.Instance,
                __endPoint1ServerSelector
            });
        private readonly static ServerId __serverId1 = new ServerId(__clusterId, __endPoint1);
        private readonly static ServerId __serverId2 = new ServerId(__clusterId, __endPoint2);
        private readonly static ServerMonitorSettings __serverMonitorSettings = new ServerMonitorSettings(
            connectTimeout: TimeSpan.FromMilliseconds(1),
            heartbeatInterval: TimeSpan.FromMilliseconds(200));
        private readonly static ServerSettings __serverSettings = new ServerSettings(__serverMonitorSettings.HeartbeatInterval);
        private readonly static (ServerId ServerId, EndPoint Endpoint, bool IsHealthy)[] __serverInfoCollection = new[]
        {
            (__serverId1, __endPoint1, false),
            (__serverId2, __endPoint2, true),
        };
        private readonly static ClusterSettings __clusterSettings = new ClusterSettings(
            connectionMode: ClusterConnectionMode.Sharded,
            serverSelectionTimeout: TimeSpan.FromSeconds(30),
            endPoints: __serverInfoCollection.Select(c => c.Endpoint).ToArray());

        [Fact]
        public void Ensure_command_network_error_before_hadnshake_is_correctly_handled()
        {
            var eventCapturer = new EventCapturer().Capture<ServerDescriptionChangedEvent>();

            // ensure that isMaster check response is finished only after network error
            var hasNetworkErrorBeenTriggered = new TaskCompletionSource<bool>();
            // ensure that there are no unexpected events between test ending and cluster disposing
            var hasClusterBeenDisposed = new TaskCompletionSource<bool>();

            var connectionPoolFactory = CreateAndSetupConnectionPoolFactory(__serverInfoCollection);
            var serverMonitorConnectionFactory = CreateAndSetupServerMonitorConnectionFactory(hasNetworkErrorBeenTriggered, hasClusterBeenDisposed, __serverInfoCollection);
            var serverMonitorFactory = new ServerMonitorFactory(__serverMonitorSettings, serverMonitorConnectionFactory, eventCapturer);

            var serverFactory = new ServerFactory(__clusterConnectionMode, __serverSettings, connectionPoolFactory, serverMonitorFactory, eventCapturer);

            EndPoint initialSelectedEndpoint = null;
            using (var cluster = new MultiServerCluster(__clusterSettings, serverFactory, eventCapturer))
            {
                cluster._clusterId(__clusterId);

                // 0. Initial heartbeat via `connection.Open`
                // The next isMaster response will be delayed because the Task.WaitAny in the mock.Returns
                cluster.Initialize();

                var selectedServer = cluster.SelectServer(__endpoint1AndWritableServerSelector, CancellationToken.None);
                initialSelectedEndpoint = selectedServer.EndPoint;
                initialSelectedEndpoint.Should().Be(__endPoint1);

                // make sure the next isMaster check has been called
                Thread.Sleep(__serverMonitorSettings.HeartbeatInterval + TimeSpan.FromMilliseconds(50));

                // 1. Trigger the command network error BEFORE handshake. At this time isMaster response is alreaady delayed until `hasNetworkErrorBeenTriggered.SetResult`
                Exception exception = Record.Exception(() => selectedServer.GetChannelAsync(CancellationToken.None).GetAwaiter().GetResult());

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

                // ensure that a new server can be selected
                selectedServer = cluster.SelectServer(WritableServerSelector.Instance, CancellationToken.None);

                // ensure that the selected server is not the same as the initial
                selectedServer.EndPoint.Should().Be(__endPoint2);

                // the 4th event is MongoConnectionException which will trigger the next isMaster check immediately
                eventCapturer.WaitForOrThrowIfTimeout(events => events.Count() >= 4, TimeSpan.FromSeconds(5));
            }
            hasClusterBeenDisposed.SetCanceled(); // Cut off not related events. Stop waiting in the latest mock.Returns for OpenAsync

            // Events asserting
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

            AssertNextEvent(eventCapturer, initialSelectedEndpoint, ServerType.Unknown, "InvalidatedBecause:ChannelException during handshake: MongoDB.Driver.MongoConnectionException: DnsException");
            AssertNextEvent(eventCapturer, initialSelectedEndpoint, ServerType.Unknown, "Heartbeat", typeof(MongoConnectionException));
            eventCapturer.Any().Should().BeFalse();

            int GetPort(EndPoint endpoint) => ((DnsEndPoint)endpoint).Port;
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

        private IConnectionPoolFactory CreateAndSetupConnectionPoolFactory(params (ServerId ServerId, EndPoint Endpoint, bool IsHealthy)[] serverInfoCollection)
        {
            var mockConnectionPoolFactory = new Mock<IConnectionPoolFactory>();

            foreach (var serverInfo in serverInfoCollection)
            {
                var mockConnectionPool = new Mock<IConnectionPool>();
                SetupConnectionPoolFactory(mockConnectionPoolFactory, mockConnectionPool.Object, serverInfo.ServerId, serverInfo.Endpoint);

                var mockServerConnection = new Mock<IConnectionHandle>();
                SetupConnection(mockServerConnection, serverInfo.ServerId);

                SetupConnectionPool(mockConnectionPool, mockServerConnection.Object);
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

        private IConnectionFactory CreateAndSetupServerMonitorConnectionFactory(
            TaskCompletionSource<bool> hasNetworkErrorBeenTriggered,
            TaskCompletionSource<bool> hasClusterBeenDisposed,
            params (ServerId ServerId, EndPoint Endpoint, bool IsHealthy)[] serverInfoCollection)
        {
            var mockConnectionFactory = new Mock<IConnectionFactory>();

            foreach (var serverInfo in serverInfoCollection)
            {
                var mockServerMonitorConnection = new Mock<IConnection>();
                SetupServerMonitorConnection(mockServerMonitorConnection, serverInfo.ServerId, serverInfo.IsHealthy, hasNetworkErrorBeenTriggered, hasClusterBeenDisposed);
                mockConnectionFactory
                    .Setup(c => c.CreateConnection(serverInfo.ServerId, serverInfo.Endpoint))
                    .Returns(mockServerMonitorConnection.Object);
            }

            return mockConnectionFactory.Object;
        }

        private Exception CreateDnsException(ConnectionId connectionId)
        {
            return new MongoConnectionException(connectionId, "DnsException");
        }

        private void SetupServerMonitorConnection(
            Mock<IConnection> mockConnection,
            ServerId serverId,
            bool isHealthy,
            TaskCompletionSource<bool> hasNetworkErrorBeenTriggered,
            TaskCompletionSource<bool> hasClusterBeenDisposed)
        {
            var connectionId = new ConnectionId(serverId);
            var serverVersion = "2.6";
            var isMasterDocument = new BsonDocument
            {
                { "ok", 1 },
                { "minWireVersion", 6 },
                { "maxWireVersion", 7 },
                { "msg", "isdbgrid" },
                { "version", serverVersion },
            };

            mockConnection.SetupGet(c => c.ConnectionId).Returns(new ConnectionId(serverId));

            mockConnection
                .SetupGet(c => c.Description)
                .Returns(
                    new ConnectionDescription(
                        mockConnection.Object.ConnectionId,
                        new IsMasterResult(isMasterDocument),
                        new BuildInfoResult(new BsonDocument("version", serverVersion))));

            Func<ReplyMessage<RawBsonDocument>> commandResponseAction = () => { return MessageHelper.BuildReply(new RawBsonDocument(isMasterDocument.ToBson())); };

            if (isHealthy)
            {
                SetupHealthyConnection(mockConnection);
            }
            else
            {
                SetupFailedConnection(mockConnection);
            }

            void SetupFailedConnection(Mock<IConnection> mockFaultyConnection)
            {
                // Open methods
                // sync path is not used in serverMonitor
                mockFaultyConnection
                    .SetupSequence(c => c.OpenAsync(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true)) // the first isMaster configuration passes
                    .Returns(Task.FromResult(true)) // RTT
                    .Throws(CreateDnsException(mockConnection.Object.ConnectionId)) // the dns exception. Should be triggered after Invalidate
                    .Returns(() =>
                    {
                        WaitForTaskOrTimeout(hasClusterBeenDisposed.Task, TimeSpan.FromMinutes(1), "cluster dispose");
                        return Task.FromResult(true);
                    }); // ensure that there is no unrelated events

                mockFaultyConnection
                    .Setup(c => c.ReceiveMessageAsync(It.IsAny<int>(), It.IsAny<IMessageEncoderSelector>(), It.IsAny<MessageEncoderSettings>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(() =>
                    {
                        // wait until the command network error has been triggered
                        WaitForTaskOrTimeout(hasNetworkErrorBeenTriggered.Task, TimeSpan.FromMinutes(1), "network error");
                        return commandResponseAction();
                    });
            }

            void SetupHealthyConnection(Mock<IConnection> mockHealthyConnection)
            {
                // Open methods
                mockHealthyConnection.Setup(c => c.Open(It.IsAny<CancellationToken>())); // no action is required
                mockHealthyConnection.Setup(c => c.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.FromResult(true)); // no action is required
                mockHealthyConnection
                    .Setup(c => c.ReceiveMessage(It.IsAny<int>(), It.IsAny<IMessageEncoderSelector>(), It.IsAny<MessageEncoderSettings>(), It.IsAny<CancellationToken>()))
                    .Returns(commandResponseAction);
                mockConnection
                    .Setup(c => c.ReceiveMessageAsync(It.IsAny<int>(), It.IsAny<IMessageEncoderSelector>(), It.IsAny<MessageEncoderSettings>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(commandResponseAction);
            }
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
