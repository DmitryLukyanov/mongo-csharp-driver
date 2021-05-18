/*Copyright 2021 - present MongoDB Inc.
 *
* Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
*Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Connections;

namespace MongoDB.Driver.Core.ConnectionPools
{
    internal enum CheckedOutReason
    {
        /// <summary>
        /// Make internal
        /// </summary>
        NotSet,
        /// <summary>
        /// Make internal
        /// </summary>
        Cursor,
        /// <summary>
        /// Make internal
        /// </summary>
        Transaction
    }

    internal interface ITrackedConnectionPool : IConnectionPool
    {
        // methods
        /// <summary>
        /// Acquires a connection.
        /// </summary>
        /// <param name="reason">The check out reason.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A connection.</returns>
        IConnectionHandle AcquireConnection(CheckedOutReason reason, CancellationToken cancellationToken);

        /// <summary>
        /// Acquires a connection.
        /// </summary>
        /// <param name="reason">The check out reason.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A Task whose result is a connection.</returns>
        Task<IConnectionHandle> AcquireConnectionAsync(CheckedOutReason reason, CancellationToken cancellationToken);
    }
}
