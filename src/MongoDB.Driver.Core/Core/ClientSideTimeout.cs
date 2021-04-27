/* Copyright 2021–present MongoDB Inc.
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
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core
{
    /// <summary>
    /// 
    /// </summary>
#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class ClientSideTimeout  // TODO: internal?
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        #region static
        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="timespan">TODO</param>
        /// <returns>TODO</returns>
        public static ClientSideTimeout CreateIfRequired(TimeSpan? timespan)
        {
            if (timespan.HasValue)
            {
                return new ClientSideTimeout(timespan.Value);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="ex">TODO</param>
        public static MongoClientTimeoutException CreateTimeoutException(OperationCanceledException ex)
        {
            return new MongoClientTimeoutException("TODO", ex);
        }
        #endregion

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly IClock _clock;
        private readonly DateTime _deadline;
        private readonly DateTime _started;
        private readonly TimeSpan _timeout;

        /// <summary>
        /// TODO
        /// </summary>
       // /// <param name="timeout">TODO</param>
        public ClientSideTimeout(TimeSpan timeout) : this(timeout, SystemClock.Instance)
        {
        }

        internal ClientSideTimeout(TimeSpan timeout, IClock clock)
        {
            _timeout = timeout; // TODO: ensure
            _clock = clock;
            _started = _clock.UtcNow;
            _deadline = _started + timeout;
            _cancellationTokenSource = new CancellationTokenSource(_timeout); // TODO: Dispose
        }

        /// <summary>
        /// TODO
        /// </summary>
        public CancellationToken CancellationToken => _cancellationTokenSource.Token;

        /// <summary>
        /// TODO
        /// </summary>
        public bool IsExpired => _deadline <= _clock.UtcNow;

        /// <summary>
        /// TODO
        /// </summary>
        /// <returns>TODO</returns>
        public TimeSpan GetRemainingTime() => _deadline - _clock.UtcNow;

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="selectionTimeout">TODO</param>
        /// <returns>TODO</returns>
        public ClientSideTimeout WithEffectiveSelectionTimeout(TimeSpan selectionTimeout)
        {
            var remainingTime = GetRemainingTime();
            var timeout = selectionTimeout < remainingTime ? selectionTimeout : remainingTime;
            return new ClientSideTimeout(timeout);
        }

        /// <summary>
        /// TODO
        /// </summary>
        public void ThrowIfTimeout()       // cancellationtoken
        {
            if (_clock.UtcNow >= _deadline)
            {
                throw new MongoClientTimeoutException("TODO");
            }
        }
    }
}
