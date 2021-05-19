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
using MongoDB.Driver.Core.Connections;

namespace MongoDB.Driver.Core.Servers.Strategies
{
    internal interface IServerStrategy : IDisposable
    {
        event EventHandler<ServerDescriptionChangedEventArgs> DescriptionChanged;
        ServerDescription CurrentDescription { get; }
        void Initialize();
        void HandleBeforeHandshakeCompletesException(Exception ex);
        void HandleChannelException(IConnection connection, Exception ex);
        void RequestHeartbeat();
        void Invalidate(string reasonInvalidated, bool clearConnectionPool, TopologyVersion topologyVersion); // TODO
    }
}
