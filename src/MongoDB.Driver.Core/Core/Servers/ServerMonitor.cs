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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Servers
{
    internal sealed class ServerMonitor : IServerMonitor
    {
        private static readonly TimeSpan __minHeartbeatInterval = TimeSpan.FromMilliseconds(500);

        private readonly ExponentiallyWeightedMovingAverage _averageRoundTripTimeCalculator = new ExponentiallyWeightedMovingAverage(0.2);
        private readonly ServerDescription _baseDescription;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private volatile IConnection _connection;
        private readonly IConnectionFactory _connectionFactory;
        private ServerDescription _currentDescription;
        private readonly EndPoint _endPoint;
        private HeartbeatDelay _heartbeatDelay;
        private readonly TimeSpan _heartbeatInterval;
        private readonly ServerId _serverId;
        private readonly InterlockedInt32 _state;
        private readonly TimeSpan _timeout;
        private readonly RoundTripTimeMonitor _roundTripTimeMonitor;

        private readonly Action<ServerHeartbeatStartedEvent> _heartbeatStartedEventHandler;
        private readonly Action<ServerHeartbeatSucceededEvent> _heartbeatSucceededEventHandler;
        private readonly Action<ServerHeartbeatFailedEvent> _heartbeatFailedEventHandler;
        private readonly Action<SdamInformationEvent> _sdamInformationEventHandler;

        public event EventHandler<ServerDescriptionChangedEventArgs> DescriptionChanged;

        public ServerMonitor(ServerId serverId, EndPoint endPoint, IConnectionFactory connectionFactory, TimeSpan heartbeatInterval, TimeSpan timeout, IEventSubscriber eventSubscriber)
        {
            _serverId = Ensure.IsNotNull(serverId, nameof(serverId));
            _endPoint = Ensure.IsNotNull(endPoint, nameof(endPoint));
            _connectionFactory = Ensure.IsNotNull(connectionFactory, nameof(connectionFactory));
            Ensure.IsNotNull(eventSubscriber, nameof(eventSubscriber));

            _baseDescription = _currentDescription = new ServerDescription(_serverId, endPoint, reasonChanged: "InitialDescription", heartbeatInterval: heartbeatInterval);
            _heartbeatInterval = heartbeatInterval;
            _timeout = timeout;
            _roundTripTimeMonitor = new RoundTripTimeMonitor(_connectionFactory, _serverId, _endPoint, _averageRoundTripTimeCalculator, _heartbeatInterval, _cancellationTokenSource.Token);

            _state = new InterlockedInt32(State.Initial);
            eventSubscriber.TryGetEventHandler(out _heartbeatStartedEventHandler);
            eventSubscriber.TryGetEventHandler(out _heartbeatSucceededEventHandler);
            eventSubscriber.TryGetEventHandler(out _heartbeatFailedEventHandler);
            eventSubscriber.TryGetEventHandler(out _sdamInformationEventHandler);
        }

        public ServerDescription Description => Interlocked.CompareExchange(ref _currentDescription, null, null);

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

        public void Invalidate(string reasonInvalidated)
        {
            SetDescription(_baseDescription.With($"InvalidatedBecause:{reasonInvalidated}", lastUpdateTimestamp: DateTime.UtcNow));
            RequestHeartbeat();
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

        private async Task MonitorServerAsync()
        {
            var metronome = new Metronome(_heartbeatInterval);
            var heartbeatCancellationToken = _cancellationTokenSource.Token;
            //var heartbeatProtocol = new Comm // TODO: safe?
            while (!heartbeatCancellationToken.IsCancellationRequested)
            {
                try
                {
                    try
                    {
                        while (await HeartbeatAsync(heartbeatCancellationToken).ConfigureAwait(false));
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
                    await newHeartbeatDelay.Task.ConfigureAwait(false); // RequestHeartbeat() !!!
                }
                catch
                {
                    // ignore these exceptions
                }
            }
        }

        private async Task<bool> HeartbeatAsync(CancellationToken cancellationToken)
        {
            HeartbeatInfo heartbeatInfo = null;
            Exception heartbeatException = null;

            try
            {
                if (_connection == null)
                {
                    _connection = _connectionFactory.CreateConnection(_serverId, _endPoint);
                    // if we are cancelling, it's because the server has
                    // been shut down and we really don't need to wait.
                    await _connection.OpenAsync(cancellationToken).ConfigureAwait(false); //TODO: cancellation token?
                    heartbeatInfo = new HeartbeatInfo
                    {
                        BuildInfoResult = _connection.Description.BuildInfoResult,
                        IsMasterResult = _connection.Description.IsMasterResult,
                        RoundTripTime = _connection.Description.IsMasterResult.RoundTripTime
                    };
                }
                else
                {
                    heartbeatInfo = await GetHeartbeatInfoAsync(heartbeatInfo, _connection, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                heartbeatException = ex;
                if (_connection != null)
                {
                    if (ex is MongoConnectionException mongoConnectionException && mongoConnectionException.IsNetworkException ||
                        ex is MongoCommandException)
                    {
                        _connection.Dispose();
                        Invalidate("Heartbeat exception.");
                        // TODO: clear connection pool
                        // if this was a network error and the server was in a known state before the error, the client MUST NOT sleep and MUST begin the next check immediately. (See retry ismaster calls once and JAVA-1159.)
                    }
                }
            }

            ServerDescription newDescription;
            if (heartbeatInfo != null)
            {
                var averageRoundTripTime = _averageRoundTripTimeCalculator.AddSample(heartbeatInfo.RoundTripTime);
                var averageRoundTripTimeRounded = TimeSpan.FromMilliseconds(Math.Round(averageRoundTripTime.TotalMilliseconds));
                var isMasterResult = heartbeatInfo.IsMasterResult;
                var buildInfoResult = heartbeatInfo.BuildInfoResult;

                newDescription = _baseDescription.With(
                    averageRoundTripTime: averageRoundTripTimeRounded,
                    canonicalEndPoint: isMasterResult.Me,
                    electionId: isMasterResult.ElectionId,
                    lastWriteTimestamp: isMasterResult.LastWriteTimestamp,
                    logicalSessionTimeout: isMasterResult.LogicalSessionTimeout,
                    maxBatchCount: isMasterResult.MaxBatchCount,
                    maxDocumentSize: isMasterResult.MaxDocumentSize,
                    maxMessageSize: isMasterResult.MaxMessageSize,
                    replicaSetConfig: isMasterResult.GetReplicaSetConfig(),
                    state: ServerState.Connected,
                    tags: isMasterResult.Tags,
                    type: isMasterResult.ServerType,
                    version: buildInfoResult.ServerVersion,
                    wireVersionRange: new Range<int>(isMasterResult.MinWireVersion, isMasterResult.MaxWireVersion));
            }
            else
            {
                newDescription = _baseDescription.With(lastUpdateTimestamp: DateTime.UtcNow);
            }

            if (heartbeatException != null)
            {
                newDescription = newDescription.With(heartbeatException: heartbeatException);
            }

            newDescription = newDescription.With(reasonChanged: "Heartbeat", lastHeartbeatTimestamp: DateTime.UtcNow);

            SetDescription(newDescription);

            if (heartbeatInfo.IsMasterResult.TopologyVersion != null ||
                heartbeatInfo.IsMasterResult.HasMoreToCome)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private async Task<HeartbeatInfo> GetHeartbeatInfoAsync(HeartbeatInfo previousHeartbeatInfo/*TODO*/, IConnection connection, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_heartbeatStartedEventHandler != null)
            {
                _heartbeatStartedEventHandler(new ServerHeartbeatStartedEvent(connection.ConnectionId, connection.Description.IsMasterResult.TopologyVersion != null));
            }

            try
            {
                //connection.Description.IsMasterResult.with

                var isMasterCommand = IsMasterHelper.CreateCommand(
                    connection.Description.IsMasterResult.TopologyVersion,
                    _heartbeatInterval);
                var isMasterProtocol = IsMasterHelper.CreateProtocol(isMasterCommand); //TODO: make protocol global?

                // TODO: validate ex == 11?
                var isMasterResult = await IsMasterHelper.GetResultAsync(connection, isMasterProtocol, cancellationToken).ConfigureAwait(false);
                var newConnectionDescription = new ConnectionDescription(connection.ConnectionId, isMasterResult, connection.Description.BuildInfoResult);
                _connection = _connection.With(newConnectionDescription);

                if (_heartbeatSucceededEventHandler != null)
                {
                    _heartbeatSucceededEventHandler(new ServerHeartbeatSucceededEvent(connection.ConnectionId, isMasterResult.RoundTripTime, connection.Description.IsMasterResult.TopologyVersion != null));
                }

                return new HeartbeatInfo
                {
                    HasMoreToCome = isMasterResult.HasMoreToCome,
                    PreviousResponseId = isMasterResult.PreviousResponseId,
                    RoundTripTime = isMasterResult.RoundTripTime,
                    IsMasterResult = isMasterResult,
                    BuildInfoResult = new BuildInfoResult(new BsonDocument("version", "4.5.1")) // TODO
                };
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

        private class HeartbeatInfo
        {
            public BuildInfoResult BuildInfoResult; //TODO:
            public bool HasMoreToCome;
            public IsMasterResult IsMasterResult;
            public int? PreviousResponseId;
            public TimeSpan RoundTripTime;
        }
    }


    internal class RoundTripTimeMonitor : IDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly IConnectionFactory _connectionFactory;
        private readonly EndPoint _endPoint;
        private readonly ExponentiallyWeightedMovingAverage _exponentiallyWeightedMovingAverage;
        private IConnection _roundTripTimeConnection;
        private readonly ServerId _serverId;
        private readonly TimeSpan _heartbeatFrequency;

        public RoundTripTimeMonitor(
            IConnectionFactory connectionFactory,
            ServerId serverId,
            EndPoint endpoint,
            ExponentiallyWeightedMovingAverage exponentiallyWeightedMovingAverage,
            TimeSpan heartbeatFrequency,
            CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            _connectionFactory = Ensure.IsNotNull(connectionFactory, nameof(connectionFactory));
            _endPoint = Ensure.IsNotNull(endpoint, nameof(endpoint));
            _exponentiallyWeightedMovingAverage = Ensure.IsNotNull(exponentiallyWeightedMovingAverage, nameof(exponentiallyWeightedMovingAverage));
            _heartbeatFrequency = heartbeatFrequency;
            _serverId = Ensure.IsNotNull(serverId, nameof(serverId));
        }

        public ExponentiallyWeightedMovingAverage ExponentiallyWeightedMovingAverage => _exponentiallyWeightedMovingAverage;

        public async Task Initialize()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            _roundTripTimeConnection = _connectionFactory.CreateConnection(_serverId, _endPoint);
            // if we are cancelling, it's because the server has
            // been shut down and we really don't need to wait.
            await _roundTripTimeConnection.OpenAsync(_cancellationToken).ConfigureAwait(false);
        }

        public async Task Run()
        {
            while (!_cancellationToken.IsCancellationRequested) //TODO: not disposed
            {
                try
                {
                    if (_roundTripTimeConnection == null)
                    {
                        await Initialize().ConfigureAwait(false);
                        var roundTripTime = _roundTripTimeConnection.Description.IsMasterResult.RoundTripTime;
                        _exponentiallyWeightedMovingAverage.AddSample(roundTripTime);
                    }
                    else
                    {
                        var isMasterCommand = IsMasterHelper.CreateCommand();
                        var isMasterProtocol = IsMasterHelper.CreateProtocol(isMasterCommand);
                        var isMasterResult = await IsMasterHelper.GetResultAsync(_roundTripTimeConnection, isMasterProtocol, _cancellationToken).ConfigureAwait(false);
                        _exponentiallyWeightedMovingAverage.AddSample(isMasterResult.RoundTripTime);
                    }
                }

                await Task.Delay(_heartbeatFrequency).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            if (_roundTripTimeConnection != null)
            {
                _roundTripTimeConnection.Dispose();      //TODO: try/catch?
            }
        }
    }

}
