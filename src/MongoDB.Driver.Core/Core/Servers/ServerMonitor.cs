/* Copyright 2016-present MongoDB Inc.
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
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol;

namespace MongoDB.Driver.Core.Servers
{
    internal sealed class ServerMonitor : IServerMonitor
    {
        private static readonly TimeSpan __minHeartbeatInterval = TimeSpan.FromMilliseconds(500);

        private readonly ServerDescription _baseDescription;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private volatile IConnection _connection;
        private readonly IConnectionFactory _connectionFactory;
        private volatile bool _currentCheckCancelled;
        private ServerDescription _currentDescription;
        private readonly EndPoint _endPoint;
        private volatile BuildInfoResult _handshakeBuildInfoResult;
        private HeartbeatDelay _heartbeatDelay;
        private readonly TimeSpan _heartbeatInterval;
        private readonly object _lock = new object();
        private readonly ServerId _serverId;
        private readonly InterlockedInt32 _state;
        private readonly TimeSpan _timeout;
        private readonly IRoundTripTimeMonitor _roundTripTimeMonitor;

        private readonly Action<ServerHeartbeatStartedEvent> _heartbeatStartedEventHandler;
        private readonly Action<ServerHeartbeatSucceededEvent> _heartbeatSucceededEventHandler;
        private readonly Action<ServerHeartbeatFailedEvent> _heartbeatFailedEventHandler;
        private readonly Action<SdamInformationEvent> _sdamInformationEventHandler;

        public event EventHandler<ServerDescriptionChangedEventArgs> DescriptionChanged;

        public ServerMonitor(ServerId serverId, EndPoint endPoint, IConnectionFactory connectionFactory, TimeSpan heartbeatInterval, TimeSpan timeout, IEventSubscriber eventSubscriber)
            : this(serverId, endPoint, connectionFactory, heartbeatInterval, timeout, eventSubscriber, new CancellationTokenSource())
        {
        }

        public ServerMonitor(ServerId serverId, EndPoint endPoint, IConnectionFactory connectionFactory, TimeSpan heartbeatInterval, TimeSpan timeout, IEventSubscriber eventSubscriber, CancellationTokenSource cancellationTokenSource)
            : this(
                  serverId,
                  endPoint,
                  connectionFactory,
                  heartbeatInterval,
                  timeout,
                  eventSubscriber,
                  roundTripTimeMonitor: new RoundTripTimeMonitor(connectionFactory, serverId, endPoint, heartbeatInterval, cancellationTokenSource.Token),
                  cancellationTokenSource)
        {
        }

        public ServerMonitor(ServerId serverId, EndPoint endPoint, IConnectionFactory connectionFactory, TimeSpan heartbeatInterval, TimeSpan timeout, IEventSubscriber eventSubscriber, IRoundTripTimeMonitor roundTripTimeMonitor, CancellationTokenSource cancellationTokenSource)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _serverId = Ensure.IsNotNull(serverId, nameof(serverId));
            _endPoint = Ensure.IsNotNull(endPoint, nameof(endPoint));
            _connectionFactory = Ensure.IsNotNull(connectionFactory, nameof(connectionFactory));
            Ensure.IsNotNull(eventSubscriber, nameof(eventSubscriber));

            _baseDescription = _currentDescription = new ServerDescription(_serverId, endPoint, reasonChanged: "InitialDescription", heartbeatInterval: heartbeatInterval);
            _heartbeatInterval = heartbeatInterval;
            _timeout = timeout;
            _roundTripTimeMonitor = roundTripTimeMonitor;

            _state = new InterlockedInt32(State.Initial);
            eventSubscriber.TryGetEventHandler(out _heartbeatStartedEventHandler);
            eventSubscriber.TryGetEventHandler(out _heartbeatSucceededEventHandler);
            eventSubscriber.TryGetEventHandler(out _heartbeatFailedEventHandler);
            eventSubscriber.TryGetEventHandler(out _sdamInformationEventHandler);
        }

        public ServerDescription Description => Interlocked.CompareExchange(ref _currentDescription, null, null);

        // public methods
        public void CurrentCheckCancel()
        {
            if (_connection != null)
            {
                _currentCheckCancelled = true;
                _connection.Dispose();
            }
        }

        public void Dispose()
        {
            if (_state.TryChange(State.Disposed))
            {
                _cancellationTokenSource.Cancel();
                _cancellationTokenSource.Dispose();
                if (_connection != null)
                {
                    _connection.Dispose();
                }
                _roundTripTimeMonitor.Dispose();
            }
        }

        public void Initialize()
        {
            if (_state.TryChange(State.Initial, State.Open))
            {
                MonitorServerAsync().ConfigureAwait(false);
                _roundTripTimeMonitor.Run().ConfigureAwait(false);
            }
        }

        public void RequestHeartbeat()
        {
            ThrowIfNotOpen();
            var heartbeatDelay = Interlocked.CompareExchange(ref _heartbeatDelay, null, null);
            if (heartbeatDelay != null)
            {
                heartbeatDelay.RequestHeartbeat();
            }
        }

        // private methods
        private CommandWireProtocol<BsonDocument> InitializeIsMasterProtocol(IConnection connection)
        {
            var isMasterCommand = IsMasterHelper.CreateCommand(
                connection.Description.IsMasterResult.TopologyVersion,  // can be null
                _heartbeatInterval);
            var commandResponseHandling = CommandResponseHandling.Return;
            if (connection.Description.IsMasterResult.TopologyVersion != null)
            {
                connection.SetReadTimeout(_heartbeatInterval);
                commandResponseHandling = CommandResponseHandling.ExhaustAllowed;
            }
            return IsMasterHelper.CreateProtocol(isMasterCommand, commandResponseHandling);
        }

        private async Task<IsMasterResult> InitializeConnectionAsync(CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                _connection = _connectionFactory.CreateConnection(_serverId, _endPoint);
            }
            // if we are cancelling, it's because the server has
            // been shut down and we really don't need to wait.
            var stopwatch = Stopwatch.StartNew();
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();

            _handshakeBuildInfoResult = _connection.Description.BuildInfoResult;
            _roundTripTimeMonitor.ExponentiallyWeightedMovingAverage.AddSample(stopwatch.Elapsed);
            return _connection.Description.IsMasterResult;
        }

        private async Task MonitorServerAsync()
        {
            var metronome = new Metronome(_heartbeatInterval);
            var heartbeatCancellationToken = _cancellationTokenSource.Token;
            while (!heartbeatCancellationToken.IsCancellationRequested)
            {
                try
                {
                    try
                    {
                        await HeartbeatAsync(heartbeatCancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (heartbeatCancellationToken.IsCancellationRequested)
                    {
                        // ignore OperationCanceledException when heartbeat cancellation is requested
                    }
                    catch (Exception unexpectedException)
                    {
                        // if we catch an exception here it's because of a bug in the driver (but we need to defend ourselves against that)

                        var handler = _sdamInformationEventHandler;
                        if (handler != null)
                        {
                            try
                            {
                                handler.Invoke(new SdamInformationEvent(() =>
                                    string.Format(
                                        "Unexpected exception in ServerMonitor.MonitorServerAsync: {0}",
                                        unexpectedException.ToString())));
                            }
                            catch
                            {
                                // ignore any exceptions thrown by the handler (note: event handlers aren't supposed to throw exceptions)
                            }
                        }

                        // since an unexpected exception was thrown set the server description to Unknown (with the unexpected exception)
                        try
                        {
                            // keep this code as simple as possible to keep the surface area with any remaining possible bugs as small as possible
                            var newDescription = _baseDescription.WithHeartbeatException(unexpectedException); // not With in case the bug is in With
                            SetDescription(newDescription); // not SetDescriptionIfChanged in case the bug is in SetDescriptionIfChanged
                        }
                        catch
                        {
                            // if even the simple code in the try throws just give up (at least we've raised the unexpected exception via an SdamInformationEvent)
                        }
                    }

                    var newHeartbeatDelay = new HeartbeatDelay(metronome.GetNextTickDelay(), __minHeartbeatInterval);
                    var oldHeartbeatDelay = Interlocked.Exchange(ref _heartbeatDelay, newHeartbeatDelay);
                    if (oldHeartbeatDelay != null)
                    {
                        oldHeartbeatDelay.Dispose();
                    }
                    await newHeartbeatDelay.Task.ConfigureAwait(false);
                }
                catch
                {
                    // ignore these exceptions
                }
            }
        }

        private async Task HeartbeatAsync(CancellationToken cancellationToken)
        {
            CommandWireProtocol<BsonDocument> isMasterProtocol = null;

            bool immediateAttempt = true;
            while (immediateAttempt)
            {
                IsMasterResult heartbeatIsMasterResult = null;
                Exception heartbeatException = null;
                var previousDescription = _currentDescription;

                try
                {
                    if (_connection == null)
                    {
                        heartbeatIsMasterResult = await InitializeConnectionAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        isMasterProtocol = isMasterProtocol ?? InitializeIsMasterProtocol(_connection);
                        heartbeatIsMasterResult = await GetHeartbeatInfoAsync(isMasterProtocol, _connection, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (ObjectDisposedException) when (_currentCheckCancelled)
                {
                    _currentCheckCancelled = false;
                    _connection = null;
                    return;
                }
                catch (MongoConnectionException ex) when (ex.InnerException is OperationCanceledException)
                {
                    // MongoConnectionException can wrap OperationCancellationException
                    return;
                }
                catch (Exception ex)
                {
                    heartbeatException = ex;

                    if (_connection != null)
                    {
                        if (IsNetworkError(ex) || ex is MongoCommandException)
                        {
                            _connection.Dispose();
                            _connection = null;
                        }
                    }
                }


                if (_currentCheckCancelled)
                {
                    _currentCheckCancelled = false;
                    return;
                }

                ServerDescription newDescription;
                if (heartbeatIsMasterResult != null)
                {
                    if (_handshakeBuildInfoResult == null)
                    {
                        // we can be here only if there is a bug in the driver
                        throw new ArgumentNullException("BuildInfo has been lost.");
                    }

                    var averageRoundTripTime = _roundTripTimeMonitor.ExponentiallyWeightedMovingAverage.Average;
                    var averageRoundTripTimeRounded = TimeSpan.FromMilliseconds(Math.Round(averageRoundTripTime.TotalMilliseconds));

                    newDescription = _baseDescription.With(
                        averageRoundTripTime: averageRoundTripTimeRounded,
                        canonicalEndPoint: heartbeatIsMasterResult.Me,
                        electionId: heartbeatIsMasterResult.ElectionId,
                        lastWriteTimestamp: heartbeatIsMasterResult.LastWriteTimestamp,
                        logicalSessionTimeout: heartbeatIsMasterResult.LogicalSessionTimeout,
                        maxBatchCount: heartbeatIsMasterResult.MaxBatchCount,
                        maxDocumentSize: heartbeatIsMasterResult.MaxDocumentSize,
                        maxMessageSize: heartbeatIsMasterResult.MaxMessageSize,
                        replicaSetConfig: heartbeatIsMasterResult.GetReplicaSetConfig(),
                        state: ServerState.Connected,
                        tags: heartbeatIsMasterResult.Tags,
                        type: heartbeatIsMasterResult.ServerType,
                        version: _handshakeBuildInfoResult.ServerVersion,
                        wireVersionRange: new Range<int>(heartbeatIsMasterResult.MinWireVersion, heartbeatIsMasterResult.MaxWireVersion));
                }
                else
                {
                    newDescription = _baseDescription.With(lastUpdateTimestamp: DateTime.UtcNow);
                }

                if (heartbeatException != null)
                {
                    if (heartbeatException is MongoCommandException heartbeatCommandException)
                    {
                        var topologyVersion = TopologyVersion.FromMongoCommandException(heartbeatCommandException);
                        newDescription = newDescription.With(heartbeatException: heartbeatException, topologyVersion: topologyVersion);
                    }
                    else
                    {
                        newDescription = newDescription.With(heartbeatException: heartbeatException);
                    }
                }

                newDescription = newDescription.With(reasonChanged: "Heartbeat", lastHeartbeatTimestamp: DateTime.UtcNow);

                SetDescription(newDescription);

                immediateAttempt =
                    (heartbeatIsMasterResult != null && heartbeatIsMasterResult.TopologyVersion != null) ||
                    (isMasterProtocol != null && isMasterProtocol.MoreToCome) ||
                    (IsNetworkError(heartbeatException) && previousDescription.Type != ServerType.Unknown);
            }

            bool IsNetworkError(Exception ex)
            {
                return ex is MongoConnectionException mongoConnectionException && mongoConnectionException.IsNetworkException;
            }
        }

        private async Task<IsMasterResult> GetHeartbeatInfoAsync(
            CommandWireProtocol<BsonDocument> isMasterProtocol,
            IConnection connection,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_heartbeatStartedEventHandler != null)
            {
                _heartbeatStartedEventHandler(new ServerHeartbeatStartedEvent(connection.ConnectionId, connection.Description.IsMasterResult.TopologyVersion != null));
            }

            try
            {
                var stopwatch = Stopwatch.StartNew();
                var isMasterResult = await IsMasterHelper.GetResultAsync(connection, isMasterProtocol, cancellationToken).ConfigureAwait(false);
                stopwatch.Stop();

                if (_heartbeatSucceededEventHandler != null)
                {
                    _heartbeatSucceededEventHandler(new ServerHeartbeatSucceededEvent(connection.ConnectionId, stopwatch.Elapsed, connection.Description.IsMasterResult.TopologyVersion != null));
                }

                return isMasterResult;
            }
            catch (Exception ex)
            {
                if (_heartbeatFailedEventHandler != null)
                {
                    _heartbeatFailedEventHandler(new ServerHeartbeatFailedEvent(connection.ConnectionId, ex, connection.Description.IsMasterResult.TopologyVersion != null));
                }
                throw;
            }
        }

        private void OnDescriptionChanged(ServerDescription oldDescription, ServerDescription newDescription)
        {
            var handler = DescriptionChanged;
            if (handler != null)
            {
                var args = new ServerDescriptionChangedEventArgs(oldDescription, newDescription);
                try { handler(this, args); }
                catch { } // ignore exceptions
            }
        }

        private void SetDescription(ServerDescription newDescription)
        {
            var oldDescription = Interlocked.CompareExchange(ref _currentDescription, null, null);
            SetDescription(oldDescription, newDescription);
        }

        private void SetDescription(ServerDescription oldDescription, ServerDescription newDescription)
        {
            Interlocked.Exchange(ref _currentDescription, newDescription);
            OnDescriptionChanged(oldDescription, newDescription);
        }

        private void ThrowIfDisposed()
        {
            if (_state.Value == State.Disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        private void ThrowIfNotOpen()
        {
            if (_state.Value != State.Open)
            {
                ThrowIfDisposed();
                throw new InvalidOperationException("Server monitor must be initialized.");
            }
        }

        // nested types
        private static class State
        {
            public const int Initial = 0;
            public const int Open = 1;
            public const int Disposed = 2;
        }
    }
}
