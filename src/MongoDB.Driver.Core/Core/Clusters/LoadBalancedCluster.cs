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
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using MongoDB.Libmongocrypt;

namespace MongoDB.Driver.Core.Clusters
{
    /// <summary>
    /// Represents the cluster that use load balancing.
    /// </summary>
    internal class LoadBalancedCluster : ICluster
    {
        private readonly IClusterClock _clusterClock = new ClusterClock();
        private readonly ClusterId _clusterId;
        private CryptClient _cryptClient = null;
        private ClusterDescription _description;
        private readonly IClusterableServerFactory _serverFactory;
        private IClusterableServer _server;
        private readonly ICoreServerSessionPool _serverSessionPool;
        private readonly ClusterSettings _settings;
        private readonly InterlockedInt32 _state;

        private readonly Action<ClusterClosingEvent> _closingEventHandler;
        private readonly Action<ClusterClosedEvent> _closedEventHandler;
        private readonly Action<ClusterOpeningEvent> _openingEventHandler;
        private readonly Action<ClusterOpenedEvent> _openedEventHandler;
        private readonly Action<ClusterDescriptionChangedEvent> _descriptionChangedEventHandler;

        public LoadBalancedCluster(ClusterSettings settings, IClusterableServerFactory serverFactory, IEventSubscriber eventSubscriber)
        {
            Ensure.That(settings.Scheme != ConnectionStringScheme.MongoDBPlusSrv, nameof(settings.Scheme));
            Ensure.IsEqualTo(settings.EndPoints.Count, 1, nameof(settings.EndPoints.Count));
            Ensure.IsEqualTo(settings.LoadBalanced, true, nameof(settings.LoadBalanced));
            Ensure.IsNull(settings.ReplicaSetName, nameof(settings.ReplicaSetName));

            _state = new InterlockedInt32(State.Initial);
            _clusterId = new ClusterId();
            _serverFactory = Ensure.IsNotNull(serverFactory, nameof(serverFactory));
            _serverSessionPool = new CoreServerSessionPool(this);
            _settings = Ensure.IsNotNull(settings, nameof(settings));
            _description = ClusterDescription.CreateInitial(
                ClusterType.Unknown,
                _clusterId,
#pragma warning disable CS0618 // Type or member is obsolete
                ClusterConnectionMode.Automatic, // TODO: replace
                ConnectionModeSwitch.UseConnectionMode, // TODO: replace
                null);
#pragma warning restore CS0618 // Type or member is obsolete

            eventSubscriber.TryGetEventHandler(out _closingEventHandler);
            eventSubscriber.TryGetEventHandler(out _closedEventHandler);
            eventSubscriber.TryGetEventHandler(out _openingEventHandler);
            eventSubscriber.TryGetEventHandler(out _openedEventHandler);
            eventSubscriber.TryGetEventHandler(out _descriptionChangedEventHandler);
        }

        public ClusterId ClusterId => _clusterId;

        public ClusterDescription Description => _description;

        public ClusterSettings Settings => _settings;

        public CryptClient CryptClient => _cryptClient;

        public event EventHandler<ClusterDescriptionChangedEventArgs> DescriptionChanged;

        public ICoreServerSession AcquireServerSession() => _serverSessionPool.AcquireSession();

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_state.TryChange(State.Disposed))
            {
                if (disposing)
                {
                    _closingEventHandler?.Invoke(new ClusterClosingEvent(ClusterId));
                    var stopwatch = Stopwatch.StartNew();
                    _closedEventHandler?.Invoke(new ClusterClosedEvent(ClusterId, stopwatch.Elapsed));
                }
            }
        }

        public void Initialize()
        {
            if (_state.TryChange(State.Initial, State.Open))
            {
                var stopwatch = Stopwatch.StartNew(); 
                if (_openingEventHandler != null)
                {
                    _openingEventHandler(new ClusterOpeningEvent(ClusterId, Settings));
                }

                if (_settings.KmsProviders != null || _settings.SchemaMap != null)
                {
                    _cryptClient = CryptClientCreator.CreateCryptClient(_settings.KmsProviders, _settings.SchemaMap);
                }

                _server = CreateServer(Settings.EndPoints[0]);

                // TODO: lock?
                var newClusterDescription = Description
                     .WithServerDescription(_server.Description)
                     .WithType(ClusterType.LoadBalanced);
                _description = newClusterDescription;
                OnDescriptionChanged(Description, newClusterDescription, true);

                _server.Initialize(); //TODO: order?

                if (_openedEventHandler != null)
                {
                    _openedEventHandler(new ClusterOpenedEvent(ClusterId, Settings, stopwatch.Elapsed));
                }
            }
        }

        public IServer SelectServer(IServerSelector selector, CancellationToken cancellationToken)
        {
            return _server;
        }

        public Task<IServer> SelectServerAsync(IServerSelector selector, CancellationToken cancellationToken)
        {
            return Task.FromResult<IServer>(_server);
        }

        public ICoreSessionHandle StartSession(CoreSessionOptions options = null)
        {
            options = options ?? new CoreSessionOptions();
            var serverSession = AcquireServerSession();
            var session = new CoreSession(this, serverSession, options);
            return new CoreSessionHandle(session);
        }

        // private method
        private IClusterableServer CreateServer(EndPoint endPoint)
        {
            return _serverFactory.CreateServer(_settings.GetInitialClusterType() /*just bool?*/, _clusterId, _clusterClock, endPoint);
        }

        protected void OnDescriptionChanged(ClusterDescription oldDescription, ClusterDescription newDescription, bool shouldClusterDescriptionChangedEventBePublished)
        {
            if (shouldClusterDescriptionChangedEventBePublished && _descriptionChangedEventHandler != null)
            {
                _descriptionChangedEventHandler(new ClusterDescriptionChangedEvent(oldDescription, newDescription));
            }

            var handler = DescriptionChanged;
            if (handler != null)
            {
                var args = new ClusterDescriptionChangedEventArgs(oldDescription, newDescription);
                handler(this, args);
            }
        }

        //private void WaitServerSelectionTimeout(Task serverSelectionTask, CancellationToken cancellationToken)
        //{
        //    var result = Task.WaitAny(serverSelectionTask, Task.Delay(Settings.ServerSelectionTimeout, cancellationToken));
        //    if (result != 1) //0
        //    {
        //        throw new TimeoutException();
        //    }
        //}

        //private async Task WaitServerSelectionTimeoutAsync(Task serverSelectionTask, CancellationToken cancellationToken)
        //{
        //    var result = await Task.WhenAny(serverSelectionTask, Task.Delay(Settings.ServerSelectionTimeout, cancellationToken)).ConfigureAwait(false);
        //    if (result != null) //0
        //    {
        //        throw new TimeoutException();
        //    }
        //}

        // nested types
        private static class State
        {
            public const int Initial = 0;
            public const int Open = 1;
            public const int Disposed = 2;
        }
    }
}
