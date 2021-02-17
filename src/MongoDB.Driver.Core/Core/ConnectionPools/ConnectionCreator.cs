
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using static MongoDB.Driver.Core.ConnectionPools.ExclusiveConnectionPool;

namespace MongoDB.Driver.Core.ConnectionPools
{
    internal sealed class ConnectionCreator// : ConnectionHelper?
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly EndPoint _endPoint;
        private readonly ExclusiveConnectionPool _pool;
        private readonly ServerId _serverId;
        private readonly Action<ConnectionCreatedEvent> _connectionCreatedEventHandler;

        public ConnectionCreator(ExclusiveConnectionPool pool, IConnectionFactory connectionFactory, ServerId serverId, EndPoint endpoint, IEventSubscriber eventSubscriber)
        {
            _connectionFactory = Ensure.IsNotNull(connectionFactory, nameof(connectionFactory));
            _endPoint = endpoint;
            _pool = Ensure.IsNotNull(pool, nameof(pool));
            _serverId = Ensure.IsNotNull(serverId, nameof(serverId));

            eventSubscriber.TryGetEventHandler(out _connectionCreatedEventHandler);
        }

        private PooledConnection CreateNewConnection()
        {
            var connection = _connectionFactory.CreateConnection(_serverId, _endPoint);
            var pooledConnection = new PooledConnection(_pool, connection);
            _connectionCreatedEventHandler?.Invoke(new ConnectionCreatedEvent(connection.ConnectionId, connection.Settings, EventContext.OperationId));
            return pooledConnection;
        }

        public PooledConnection CreateAndOpenConnection(CancellationToken cancellationToken)
        {
            var connection = CreateNewConnection();

            try
            {
                connection.Open(cancellationToken);
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }

            return connection;
        }

        public async Task<PooledConnection> CreateAndOpenConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = CreateNewConnection();

            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }

            return connection;
        }
    }
}
