﻿/* Copyright 2018-present MongoDB Inc.
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

namespace MongoDB.Driver
{
    /// <summary>
    /// Transaction options.
    /// </summary>
    public class TransactionOptions
    {
        // private fields
        private readonly TimeSpan? _maxCommitTime;
        private readonly ReadConcern _readConcern;
        private readonly ReadPreference _readPreference;
        private readonly WriteConcern _writeConcern;
        private readonly TimeSpan? _timeout;

        // public constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionOptions" /> class.
        /// </summary>
        /// <param name="readConcern">The read concern.</param>
        /// <param name="readPreference">The read preference.</param>
        /// <param name="writeConcern">The write concern.</param>
        /// <param name="maxCommitTime">The max commit time.</param>
        /// <param name="timeout">TODO</param>
        public TransactionOptions(
            Optional<ReadConcern> readConcern = default(Optional<ReadConcern>),
            Optional<ReadPreference> readPreference = default(Optional<ReadPreference>),
            Optional<WriteConcern> writeConcern = default(Optional<WriteConcern>),
            Optional<TimeSpan?> maxCommitTime = default(Optional<TimeSpan?>),
            Optional<TimeSpan?> timeout = default)
        {
            _readConcern = readConcern.WithDefault(null);
            _readPreference = readPreference.WithDefault(null);
            _writeConcern = writeConcern.WithDefault(null);
            _maxCommitTime = maxCommitTime.WithDefault(null);
            _timeout = timeout.WithDefault(null);
        }

        // public properties
        /// <summary>
        /// Gets the max commit time.
        /// </summary>
        /// <value>
        /// The max commit time.
        /// </value>
        public TimeSpan? MaxCommitTime => _maxCommitTime;

        /// <summary>
        /// Gets the read concern.
        /// </summary>
        /// <value>
        /// The read concern.
        /// </value>
        public ReadConcern ReadConcern => _readConcern;

        /// <summary>
        /// Gets the read preference.
        /// </summary>
        /// <value>
        /// The read preference.
        /// </value>
        public ReadPreference ReadPreference => _readPreference;

        /// <summary>
        /// TODO
        /// </summary>
        public TimeSpan? Timeout => _timeout;

        /// <summary>
        /// Gets the write concern.
        /// </summary>
        /// <value>
        /// The write concern.
        /// </value>
        public WriteConcern WriteConcern => _writeConcern;

        // public methods
        /// <summary>
        /// Returns a new TransactionOptions with some values changed.
        /// </summary>
        /// <param name="readConcern">The new read concern.</param>
        /// <param name="readPreference">The read preference.</param>
        /// <param name="writeConcern">The new write concern.</param>
        /// <param name="maxCommitTime">The max commit time.</param>
        /// <param name="timeout">TODO</param>
        /// <returns>
        /// The new TransactionOptions.
        /// </returns>
        public TransactionOptions With(
            Optional<ReadConcern> readConcern = default(Optional<ReadConcern>),
            Optional<ReadPreference> readPreference = default(Optional<ReadPreference>),
            Optional<WriteConcern> writeConcern = default(Optional<WriteConcern>),
            Optional<TimeSpan?> maxCommitTime = default(Optional<TimeSpan?>),
            Optional<TimeSpan?> timeout = default)
        {
            return new TransactionOptions(
                readConcern: readConcern.WithDefault(_readConcern),
                readPreference: readPreference.WithDefault(_readPreference),
                writeConcern: writeConcern.WithDefault(_writeConcern),
                maxCommitTime: maxCommitTime.WithDefault(_maxCommitTime),
                timeout: timeout.WithDefault(_timeout));
        }
    }
}
