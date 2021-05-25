/* Copyright 2021-present MongoDB Inc.
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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.ConnectionPools;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Servers.Strategies
{
    internal class DefaultServerStrategy : IServerStrategy
    {
        #region static
        // static fields
        private static readonly List<Type> __invalidatingExceptions = new List<Type>
        {
            typeof(MongoConnectionException),
            typeof(SocketException),
            typeof(EndOfStreamException),
            typeof(IOException),
        };
        #endregion

        private readonly ServerDescription _baseDescription;
        private readonly IConnectionPool _connectionPool; // TODO: define where it belongs to
        private ServerDescription _currentDescription;
        private readonly IServerMonitor _monitor;

        public DefaultServerStrategy(
            ServerId serverId,
            EndPoint endPoint,
            IServerMonitorFactory monitorFactory,
            IConnectionPool connectionPool,
            ServerSettings settings)
        {
            _connectionPool = connectionPool;
            _monitor = monitorFactory.Create(serverId, endPoint);
            _baseDescription = _currentDescription = new ServerDescription(serverId, endPoint, reasonChanged: "ServerInitialDescription", heartbeatInterval: settings.HeartbeatInterval);
        }

        public ServerDescription CurrentDescription => Interlocked.CompareExchange(ref _currentDescription, value: null, comparand: null);
        public event EventHandler<ServerDescriptionChangedEventArgs> DescriptionChanged;

        public void Dispose()
        {
            _monitor.Dispose();
            _monitor.DescriptionChanged -= OnMonitorDescriptionChanged;
        }

        public void HandleBeforeHandshakeCompletesException(Exception ex)
        {
            if (ex is MongoAuthenticationException)
            {
                _connectionPool.Clear();
                return;
            }

            if (ex is MongoConnectionException mongoConnectionException)
            {
                lock (_monitor.Lock)
                {
                    if (mongoConnectionException.Generation != null &&
                        mongoConnectionException.Generation != _connectionPool.Generation)
                    {
                        return; // stale generation number
                    }

                    if (mongoConnectionException.IsNetworkException &&
                        !mongoConnectionException.ContainsTimeoutException)
                    {
                        _monitor.CancelCurrentCheck();
                    }

                    if (mongoConnectionException.IsNetworkException || mongoConnectionException.ContainsTimeoutException)
                    {
                        Invalidate($"ChannelException during handshake: {ex}.", clearConnectionPool: true, topologyVersion: null);
                    }
                }
            }
        }

        public void HandleChannelException(IConnection connection, Exception ex)
        {
            var aggregateException = ex as AggregateException;
            if (aggregateException != null && aggregateException.InnerExceptions.Count == 1)
            {
                ex = aggregateException.InnerException;
            }

            // For most connection exceptions, we are going to immediately
            // invalidate the server. However, we aren't going to invalidate
            // because of OperationCanceledExceptions. We trust that the
            // implementations of connection don't leave themselves in a state
            // where they can't be used based on user cancellation.
            if (ex.GetType() == typeof(OperationCanceledException))
            {
                return;
            }

            lock (_monitor.Lock)
            {
                if (ex is MongoConnectionException mongoConnectionException)
                {
                    if (mongoConnectionException.Generation != null &&
                        mongoConnectionException.Generation != _connectionPool.Generation)
                    {
                        return; // stale generation number
                    }

                    if (mongoConnectionException.IsNetworkException &&
                        !mongoConnectionException.ContainsTimeoutException)
                    {
                        _monitor.CancelCurrentCheck();
                    }
                }

                var description = CurrentDescription; // use Description property to access _description value safely
                if (ShouldInvalidateServer(connection, ex, description, out TopologyVersion responseTopologyVersion))
                {
                    var shouldClearConnectionPool = ShouldClearConnectionPoolForChannelException(ex, connection.Description.ServerVersion);
                    Invalidate($"ChannelException:{ex}", shouldClearConnectionPool, responseTopologyVersion);
                }
                else
                {
                    RequestHeartbeat();
                }
            }
        }

        public void Initialize()
        {
            _monitor.DescriptionChanged += OnMonitorDescriptionChanged;
            _monitor.Initialize();
        }

        public void Invalidate(string reasonInvalidated, bool clearConnectionPool, TopologyVersion topologyVersion)
        {
            if (clearConnectionPool)
            {
                _connectionPool.Clear(); // TODO: move out from here?
            }
            var newDescription = _baseDescription.With(
                    $"InvalidatedBecause:{reasonInvalidated}",
                    lastUpdateTimestamp: DateTime.UtcNow,
                    topologyVersion: topologyVersion);
            SetDescription(newDescription);
            // TODO: make the heartbeat request conditional so we adhere to this part of the spec
            // > Network error when reading or writing: ... Clients MUST NOT request an immediate check of the server;
            // > since application sockets are used frequently, a network error likely means the server has just become
            // > unavailable, so an immediate refresh is likely to get a network error, too.
            RequestHeartbeat();
        }

        public void RequestHeartbeat()
        {
            _monitor.RequestHeartbeat();
        }

        // private methods
        private void OnMonitorDescriptionChanged(object sender, ServerDescriptionChangedEventArgs e)
        {
            var currentDescription = Interlocked.CompareExchange(ref _currentDescription, value: null, comparand: null);

            var heartbeatException = e.NewServerDescription.HeartbeatException;
            // The heartbeat commands are isMaster + buildInfo. These commands will throw a MongoCommandException on
            // {ok: 0}, but a reply (with a potential topologyVersion) will still have been received.
            // Not receiving a reply to the heartbeat commands implies a network error or a "HeartbeatFailed" type
            // exception (i.e. ServerDescription.WithHeartbeatException was called), in which case we should immediately
            // set the description to "Unknown"// (which is what e.NewServerDescription will be in such a case)
            var heartbeatReplyNotReceived = heartbeatException != null && !(heartbeatException is MongoCommandException);

            // We cannot use FresherThan(e.NewServerDescription.TopologyVersion, currentDescription.TopologyVersion)
            // because due to how TopologyVersions comparisons are defined, IsStalerThanOrEqualTo(x, y) does not imply
            // FresherThan(y, x)
            if (heartbeatReplyNotReceived ||
                TopologyVersion.IsStalerThanOrEqualTo(currentDescription.TopologyVersion, e.NewServerDescription.TopologyVersion))
            {
                SetDescription(e.NewServerDescription);
            }
        }

        private void SetDescription(ServerDescription newDescription)
        {
            var oldDescription = Interlocked.CompareExchange(ref _currentDescription, value: newDescription, comparand: _currentDescription);
            OnDescriptionChanged(sender: this, new ServerDescriptionChangedEventArgs(oldDescription, newDescription));
        }

        private void OnDescriptionChanged(object sender, ServerDescriptionChangedEventArgs e)
        {
            if (e.NewServerDescription.HeartbeatException != null)
            {
                _connectionPool.Clear();
            }

            // propagate event to upper levels
            var handler = DescriptionChanged;
            if (handler != null)
            {
                try { handler(this, e); }
                catch { } // ignore exceptions
            }
        }

        private bool ShouldInvalidateServer(
            IConnection connection,
            Exception exception,
            ServerDescription description,
            out TopologyVersion invalidatingResponseTopologyVersion)
        {
            if (exception is MongoConnectionException mongoConnectionException &&
                mongoConnectionException.ContainsTimeoutException)
            {
                invalidatingResponseTopologyVersion = null;
                return false;
            }

            if (__invalidatingExceptions.Contains(exception.GetType()))
            {
                invalidatingResponseTopologyVersion = null;
                return true;
            }

            var exceptionsToCheck = new[]
            {
                exception as MongoCommandException,
                (exception as MongoWriteConcernException)?.NestedException 
            }
            .OfType<MongoCommandException>();
            foreach (MongoCommandException commandException in exceptionsToCheck)
            {
                if (IsStateChangeException(commandException))
                {
                    return !IsStaleStateChangeError(commandException.Result, out invalidatingResponseTopologyVersion);
                }
            }

            invalidatingResponseTopologyVersion = null;
            return false;

            bool IsStaleStateChangeError(BsonDocument response, out TopologyVersion nonStaleResponseTopologyVersion)
            {
                if (_connectionPool.Generation > connection.Generation)
                {
                    // stale generation number
                    nonStaleResponseTopologyVersion = null;
                    return true;
                }

                var responseTopologyVersion = TopologyVersion.FromMongoCommandResponse(response);
                // We use FresherThanOrEqualTo instead of FresherThan because a state change should come with a new
                // topology version.
                // We cannot use StalerThan(responseTopologyVersion, description.TopologyVersion) because due to how
                // TopologyVersions comparisons are defined, FresherThanOrEqualTo(x, y) does not imply StalerThan(y, x)
                bool isStale = TopologyVersion.IsFresherThanOrEqualTo(description.TopologyVersion, responseTopologyVersion);

                nonStaleResponseTopologyVersion = isStale ? null : responseTopologyVersion;
                return isStale;
            }
        }


        private bool ShouldClearConnectionPoolForChannelException(Exception ex, SemanticVersion serverVersion)
        {
            if (ex is MongoConnectionException mongoCommandException &&
                mongoCommandException.IsNetworkException &&
                !mongoCommandException.ContainsTimeoutException)
            {
                return true;
            }
            if (IsStateChangeException(ex))
            {
                return
                    IsShutdownException(ex) ||
                    !Feature.KeepConnectionPoolWhenNotMasterConnectionException.IsSupported(serverVersion); // i.e. serverVersion < 4.1.10
            }
            return false;
        }

        private bool IsStateChangeException(Exception ex) => ex is MongoNotPrimaryException || ex is MongoNodeIsRecoveringException;

        private bool IsShutdownException(Exception ex) => ex is MongoNodeIsRecoveringException mongoNodeIsRecoveringException && mongoNodeIsRecoveringException.IsShutdownError;
    }
}
