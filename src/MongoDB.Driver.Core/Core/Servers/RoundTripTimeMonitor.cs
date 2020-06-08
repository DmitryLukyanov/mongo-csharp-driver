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
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Servers
{
    internal interface IRoundTripTimeMonitor : IDisposable
    {
        ExponentiallyWeightedMovingAverage ExponentiallyWeightedMovingAverage { get; }
        Task Run();
    }

    internal class RoundTripTimeMonitor : IRoundTripTimeMonitor
    {
        private readonly ExponentiallyWeightedMovingAverage _averageRoundTripTimeCalculator = new ExponentiallyWeightedMovingAverage(0.2);

        private readonly CancellationToken _cancellationToken;
        private readonly IConnectionFactory _connectionFactory;
        private readonly EndPoint _endPoint;
        private IConnection _roundTripTimeConnection;
        private readonly ServerId _serverId;
        private readonly TimeSpan _heartbeatFrequency;

        public RoundTripTimeMonitor(
            IConnectionFactory connectionFactory,
            ServerId serverId,
            EndPoint endpoint,
            TimeSpan heartbeatFrequency,
            CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            _connectionFactory = Ensure.IsNotNull(connectionFactory, nameof(connectionFactory));
            _endPoint = Ensure.IsNotNull(endpoint, nameof(endpoint));
            _heartbeatFrequency = heartbeatFrequency;
            _serverId = Ensure.IsNotNull(serverId, nameof(serverId));
        }

        public ExponentiallyWeightedMovingAverage ExponentiallyWeightedMovingAverage => _averageRoundTripTimeCalculator;

        public async Task Run()
        {
            while (!_cancellationToken.IsCancellationRequested) // TODO: different token?
            {
                try
                {
                    if (_roundTripTimeConnection == null)
                    {
                        await InitializeConnectionAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        try
                        {
                            var isMasterCommand = IsMasterHelper.CreateCommand();
                            var isMasterProtocol = IsMasterHelper.CreateProtocol(isMasterCommand);

                            var stopwatch = Stopwatch.StartNew();
                            var isMasterResult = await IsMasterHelper.GetResultAsync(_roundTripTimeConnection, isMasterProtocol, _cancellationToken).ConfigureAwait(false);
                            stopwatch.Stop();
                            _averageRoundTripTimeCalculator.AddSample(stopwatch.Elapsed);
                        }
                        catch (Exception ex) when (ShouldCloseRoundTripConnection(ex))
                        {
                            _roundTripTimeConnection.Dispose();
                            _roundTripTimeConnection = null;
                        }
                    }
                }
                catch (Exception)
                {
                    // ignore exceptions
                }

                await Task.Delay(_heartbeatFrequency).ConfigureAwait(false);
            }

            bool ShouldCloseRoundTripConnection(Exception ex)
            {
                return
                    ex is MongoCommandException ||
                    (ex is MongoConnectionException mongoConnectionException &&
                    mongoConnectionException.ContainsSocketTimeoutException);
            }
        }

        public void Dispose()
        {
            if (_roundTripTimeConnection != null)
            {
                try
                {
                    _roundTripTimeConnection.Dispose();
                }
                catch
                {
                    // ignore it
                }
            }
        }

        // private methods
        private async Task InitializeConnectionAsync()
        {
            _cancellationToken.ThrowIfCancellationRequested();

            _roundTripTimeConnection = _connectionFactory.CreateConnection(_serverId, _endPoint);
            // if we are cancelling, it's because the server has
            // been shut down and we really don't need to wait.
            var stopwatch = Stopwatch.StartNew();
            await _roundTripTimeConnection.OpenAsync(_cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _averageRoundTripTimeCalculator.AddSample(stopwatch.Elapsed);
        }
    }
}
