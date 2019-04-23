/* Copyright 2019-present MongoDB Inc.
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

using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    /// <summary>
    /// Represents the output mode of an aggregate query.
    /// </summary>
    public enum AggregateOutMode
    {
        /// <summary>
        /// Replace the collection.
        /// </summary>
        ReplaceCollection,
        /// <summary>
        /// Replace all the documents in the collection.
        /// </summary>
        ReplaceDocuments,
        /// <summary>
        /// Insert new documents into the collection.
        /// </summary>
        InsertDocuments
    }

    /// <summary>
    /// Aggregate $out options.
    /// </summary>
    public class AggregateOutStageOptions
    {
        private readonly string _collection = null;
        private readonly string _database = null;
        private readonly AggregateOutMode _mode;
        private readonly BsonDocument _uniqueKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateOutStageOptions" /> class.
        /// </summary>
        /// <param name="outputCollection">The collection namespace.</param>
        public AggregateOutStageOptions(CollectionNamespace outputCollection) : this(outputCollection.CollectionName)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateOutStageOptions" /> class.
        /// </summary>
        /// <param name="to">The collection name.</param>
        public AggregateOutStageOptions(string to)
            : this(AggregateOutMode.ReplaceCollection, to, null, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateOutStageOptions" /> class.
        /// </summary>
        /// <param name="mode">The aggregate $out mode.</param>
        /// <param name="to">The collection name.</param>
        /// <param name="db">The database name.</param>
        /// <param name="uniqueKey">The unique key.</param>
        public AggregateOutStageOptions(AggregateOutMode mode, string to, string db, BsonDocument uniqueKey)
        {
            Ensure.IsNotNullOrEmpty(to, nameof(to));

            _mode = mode;
            _collection = to;
            _database = db;
            _uniqueKey = uniqueKey;
        }

        /// <summary>
        /// Gets the collection name.
        /// </summary>
        public string Collection => _collection;

        /// <summary>
        /// Gets the database name.
        /// </summary>
        public string DataBase => _database;

        /// <summary>
        /// Gets the aggregate $out mode.
        /// </summary>
        public AggregateOutMode Mode => _mode;

        /// <summary>
        /// Gets the unique key.
        /// </summary>
        public BsonDocument UniqueKey => _uniqueKey;
    }
}
