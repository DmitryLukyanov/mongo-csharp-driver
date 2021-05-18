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
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Core.Bindings
{
    internal class TrackedRunContext
    {
        #region static
        public static TrackedRunContext CreateTrackedRunContext(ICoreSession session, bool withCursorResult) => new TrackedRunContext(session.IsInTransaction, withCursorResult);

        public static TrackedRunContext CreateEmpty() => new TrackedRunContext(false, false);
        #endregion

        private readonly bool _isInTransaction;
        private readonly bool _withCursorResult;

        private TrackedRunContext(bool isInTransaction, bool withCursorResult)
        {
            _isInTransaction = isInTransaction;
            _withCursorResult = withCursorResult;
        }

        public bool IsInTransaction => _isInTransaction;
        public bool WithCursorResult => _withCursorResult;
    }

    internal class TrackedChannelHelper
    {
        public static IServerChannelSourceFactory CreateTrackedServerChannelSource(TrackedRunContext trackedRunContext)
        {
            return new TrackedServerChannelSourceFactory(trackedRunContext);
        }

        // nested types
        private class TrackedServerChannelSourceFactory : DefaultServerChannelSourceFactory
        {
            private readonly TrackedRunContext _trackedRunContext;

            public TrackedServerChannelSourceFactory(TrackedRunContext trackedRunContext)
            {
                _trackedRunContext = trackedRunContext;
            }

            public override IChannelSource CreateServerChannelSource(IServer server, ICoreSessionHandle session)
            {
                return server is IServerWithTrackedGetChannel trackedServer
                    ? new TrackedServerChannelSource(trackedServer, session, _trackedRunContext)
                    : base.CreateServerChannelSource(server, session);
            }
        }

        private sealed class TrackedServerChannelSource : IChannelSource
        {
            // fields
            private bool _disposed;
            private readonly IServerWithTrackedGetChannel _server;
            private readonly TrackedRunContext _trackedRunContext;
            private readonly ICoreSessionHandle _session;

            // constructors
            public TrackedServerChannelSource(IServerWithTrackedGetChannel server, ICoreSessionHandle session, TrackedRunContext trackedRunContext)
            {
                _server = Ensure.IsNotNull(server, nameof(server));
                _session = Ensure.IsNotNull(session, nameof(session));
                _trackedRunContext = Ensure.IsNotNull(trackedRunContext, nameof(trackedRunContext));
            }

            // properties
            /// <inheritdoc/>
            public IServer Server
            {
                get { return _server; }
            }

            /// <inheritdoc/>
            public ServerDescription ServerDescription
            {
                get { return _server.Description; }
            }

            /// <inheritdoc/>
            public ICoreSessionHandle Session
            {
                get { return _session; }
            }

            // methods
            /// <inheritdoc/>
            public void Dispose()
            {
                if (!_disposed)
                {
                    _session.Dispose();
                    _disposed = true;
                }
            }

            /// <inheritdoc/>
            public IChannelHandle GetChannel(CancellationToken cancellationToken)
            {
                ThrowIfDisposed();
                return _server.GetChannel(_trackedRunContext, cancellationToken);
            }

            /// <inheritdoc/>
            public Task<IChannelHandle> GetChannelAsync(CancellationToken cancellationToken)
            {
                ThrowIfDisposed();
                return _server.GetChannelAsync(_trackedRunContext, cancellationToken);
            }

            private void ThrowIfDisposed()
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(GetType().FullName);
                }
            }
        }
    }
}
