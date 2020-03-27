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

using MongoDB.Bson;

namespace MongoDB.Driver
{
    /// <summary>
    /// Options for the aggregate search stage for Atlas.
    /// </summary>
    public class AggregateSearchOptions
    {
        /// <summary>
        /// Gets or sets the compound operator.
        /// </summary>
        public BsonDocument Compound { get; set; }

        /// <summary>
        /// Gets or sets the exists operator.
        /// </summary>
        public BsonDocument Exists { get; set; }

        /// <summary>
        /// Gets or sets the near operator.
        /// </summary>
        public BsonDocument Near { get; set; }

        /// <summary>
        /// Gets or sets the phrase operator.
        /// </summary>
        public BsonDocument Phrase { get; set; }

        /// <summary>
        /// Gets or sets the range operator.
        /// </summary>
        public BsonDocument Range { get; set; }

        /// <summary>
        /// Gets or sets the regex operator.
        /// </summary>
        public BsonDocument Regex { get; set; }

        /// <summary>
        /// Gets or sets the span operator.
        /// </summary>
        public BsonDocument Span { get; set; }

        /// <summary>
        /// Gets or sets the term operator.
        /// </summary>
        public BsonDocument Term { get; set; }

        /// <summary>
        /// Gets or sets the text operator.
        /// </summary>
        public BsonDocument Text { get; set; }

        /// <summary>
        /// Gets or sets the wildcard operator.
        /// </summary>
        public BsonDocument Wildcard { get; set; }
    }
}
