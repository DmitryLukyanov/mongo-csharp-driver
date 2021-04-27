/* Copyright 2013-present MongoDB Inc.
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
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents a database read operation.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    public interface IReadOperation<TResult>
    {
        // methods
        /// <summary>
        /// Executes the operation.
        /// </summary>
        /// <param name="binding">The binding.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the operation.</returns>
        TResult Execute(IReadBinding binding, CancellationToken cancellationToken);

        /// <summary>
        /// Executes the operation.
        /// </summary>
        /// <param name="binding">The binding.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A Task whose result is the result of the operation.</returns>
        Task<TResult> ExecuteAsync(IReadBinding binding, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Represents a database write operation.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    public interface IWriteOperation<TResult>
    {
        // methods
        /// <summary>
        /// Executes the operation.
        /// </summary>
        /// <param name="binding">The binding.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the operation.</returns>
        TResult Execute(IWriteBinding binding, CancellationToken cancellationToken);

        /// <summary>
        /// Executes the operation.
        /// </summary>
        /// <param name="binding">The binding.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A Task whose result is the result of the operation.</returns>
        Task<TResult> ExecuteAsync(IWriteBinding binding, CancellationToken cancellationToken);
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IWithClientSideTimeout        // IExecutableWithClientSideTimeout
    {
        /// <summary>
        /// 
        /// </summary>
        ClientSideTimeout ClientSideTimeout { get; }
    }

    /// <summary>
    /// TODO
    /// </summary>
    public static class ClientSideTimeoutExtensions
    {
        /// <summary>
        /// TODO
        /// </summary>
        /// <returns>TODO</returns>
        public static ClientSideTimeout GetTimeoutIfConfigured<TResult>(this IWriteOperation<TResult> writeOperation)  // TODO: rethink
        {
            if (writeOperation is IWithClientSideTimeout withClientSideTimeout)
            {
                return withClientSideTimeout.ClientSideTimeout;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="server">TODO</param>
        /// <param name="timeout">TODO</param>
        /// <param name="inputCancellationToken">TODO</param>
        /// <param name="func">TODO</param>
        /// <returns>TODO</returns>
#pragma warning disable CA1068 // CancellationToken parameters must come last
        public static IChannelHandle WithClientSideTimeout(this IServer server, ClientSideTimeout timeout, CancellationToken inputCancellationToken, Func<IServer, CancellationToken, IChannelHandle> func)
#pragma warning restore CA1068 // CancellationToken parameters must come last
        {
            return ExecuteWithTimeoutIfConfigured(server, timeout, inputCancellationToken, func);
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="cluster">TODO</param>
        /// <param name="timeout">TODO</param>
        /// <param name="inputCancellationToken">TODO</param>
        /// <param name="func">TODO</param>
        /// <returns>TODO</returns>
#pragma warning disable CA1068 // CancellationToken parameters must come last
        public static IServer WithClientSideTimeout(this ICluster cluster, ClientSideTimeout timeout, CancellationToken inputCancellationToken, Func<ICluster, CancellationToken, IServer> func)
#pragma warning restore CA1068 // CancellationToken parameters must come last
        {
            return ExecuteWithTimeoutIfConfigured(cluster, timeout, inputCancellationToken, func);
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="channelSourceHandle">TODO</param>
        /// <param name="timeout">TODO</param>
        /// <param name="inputCancellationToken">TODO</param>
        /// <param name="func">TODO</param>
        /// <returns></returns>
#pragma warning disable CA1068 // CancellationToken parameters must come last
        public static IChannelHandle WithClientSideTimeout(
#pragma warning restore CA1068 // CancellationToken parameters must come last
            this IChannelSourceHandle channelSourceHandle,
            ClientSideTimeout timeout,
            CancellationToken inputCancellationToken,
            Func<IChannelSourceHandle, CancellationToken, IChannelHandle> func)
        {
            return ExecuteWithTimeoutIfConfigured(channelSourceHandle, timeout, inputCancellationToken, func);
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <typeparam name="TResult">TODO</typeparam>
        /// <param name="server">TODO</param>
        /// <param name="binding">TODO</param>
        /// <param name="inputCancellationToken">TODO</param>
        /// <param name="func">TODO</param>
        /// <returns></returns>
#pragma warning disable CA1068 // CancellationToken parameters must come last
        public static TResult WithClientSideTimeout<TResult>(this IWriteOperation<TResult> server, IWriteBinding binding, CancellationToken inputCancellationToken, Func<IWriteBinding, CancellationToken, TResult> func)
#pragma warning restore CA1068 // CancellationToken parameters must come last
        {
            return ExecuteWithTimeoutIfConfigured(binding, binding.ClientSideTimeout, inputCancellationToken, func);
        }

        // private methods
        private static CancellationToken CreateLinkedCancellationToken(ClientSideTimeout timeout, CancellationToken cancellationToken)
        {
            return CancellationTokenSource.CreateLinkedTokenSource(timeout.CancellationToken, cancellationToken).Token;
        }

#pragma warning disable CA1068 // CancellationToken parameters must come last
        private static TResult ExecuteWithTimeoutIfConfigured<TInputParam, TResult>(TInputParam inputParam, ClientSideTimeout timeout, CancellationToken inputCancellationToken, Func<TInputParam, CancellationToken, TResult> func)
#pragma warning restore CA1068 // CancellationToken parameters must come last
        {
            if (timeout == null)
            {
                return func(inputParam, inputCancellationToken); // TODO: refactroring
            }

            try
            {
                var timeoutedCancellationToken = CreateLinkedCancellationToken(timeout, inputCancellationToken);
                return func(inputParam, timeoutedCancellationToken);
            }
            catch (OperationCanceledException ex)  // handle inner MongoClientTimeoutException?
            {
                if (ex.CancellationToken == inputCancellationToken)
                {
                    throw;
                }
                else
                {
                    throw ClientSideTimeout.CreateTimeoutException(ex);
                }
            }
        }
    }
}
