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
using System.Net;
using System.Threading;
using MongoDB.Driver.Core.ConnectionPools;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Servers.Strategies
{
    internal class LoadBalancedServerStrategy : IServerStrategy
    {
        private readonly ServerDescription _baseDescription;
        private ServerDescription _currentDescription;
        private readonly IConnectionPool _connectionPool;

        public LoadBalancedServerStrategy(
            ServerId serverId,
            EndPoint endPoint,
            IConnectionPool connectionPool)
        {
            _baseDescription = _currentDescription = new ServerDescription(serverId, endPoint, reasonChanged: "ServerInitialDescription");
            _connectionPool = connectionPool;
        }

        public ServerDescription CurrentDescription => Interlocked.CompareExchange(ref _currentDescription, value: null, comparand: null);

        public event EventHandler<ServerDescriptionChangedEventArgs> DescriptionChanged;

        public void Dispose()
        {
            // do nothing
        }

        public void HandleBeforeHandshakeCompletesException(Exception ex)
        {
            // drivers MUST NOT perform SDAM error handling for any errors that occur before the MongoDB Handshake

            if (ex is MongoAuthenticationException)
            {
                // when requiring the connection pool to be cleared, MUST only clear connections for the serviceId.
                _connectionPool.Clear(); // TODO: serviceId is not implemented yet
            }
        }

        public void HandleChannelException(IConnection connection, Exception ex)
        {
            var aggregateException = ex as AggregateException;
            if (aggregateException != null && aggregateException.InnerExceptions.Count == 1)
            {
                ex = aggregateException.InnerException;
            }

            // TODO: lock?
            if (ex is MongoConnectionException mongoConnectionException &&
                mongoConnectionException.Generation != null &&
                mongoConnectionException.Generation != _connectionPool.Generation)
            {
                return; // stale generation number
            }

            if (ShouldClearConnectionPoolForChannelException(ex, connection.Description.ServerVersion))
            {
                // when requiring the connection pool to be cleared, MUST only clear connections for the serviceId.
                _connectionPool.Clear(); // TODO: serviceId is not implemented yet
            }
        }

        public void Initialize()
        {
            // generate initial server description
            var newDescription = _baseDescription
                .With(
                    type: ServerType.LoadBalanced,
                    reasonChanged: "Initialized");
            var eventArgs = new ServerDescriptionChangedEventArgs(_baseDescription, newDescription);

            // propagate event to upper levels, this will be called only once
            var handler = DescriptionChanged;
            if (handler != null)
            {
                try { handler(this, eventArgs); }
                catch { } // ignore exceptions
            }
        }

        public void Invalidate(string reasonInvalidated, bool clearConnectionPool, TopologyVersion topologyVersion)
        {
            // no-opt
        }

        public void RequestHeartbeat()
        {
            // no-opt
        }

        // TODO: share?
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
