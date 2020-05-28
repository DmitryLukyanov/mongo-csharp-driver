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

namespace MongoDB.Driver.Core.Servers
{
    /// <summary>
    /// TODO: TopologyDescription?
    /// </summary>
    public class TopologyVersion : IConvertibleToBsonDocument
    {
        #region static
        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public static TopologyVersion Parse(BsonDocument document)
        {
            var processId = document.GetValue("processId").AsObjectId;
            var counter = document.GetValue("counter").AsInt64;

            return new TopologyVersion(processId, counter);
        }
        #endregion

        private readonly long _counter;
        private readonly ObjectId _processId;

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="processId">TODO</param>
        /// <param name="counter">TODO</param>
        public TopologyVersion(ObjectId processId, long counter)
        {
            _processId = processId;
            _counter = counter;
        }

        /// <summary>
        /// Gets the processId.
        /// </summary>
        public ObjectId ProcessId => _processId;

        /// <summary>
        /// Gets the counter;
        /// </summary>
        public long Counter => _counter;

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="obj">TODO</param>
        /// <returns>TODO</returns>
        public override bool Equals(object obj)
        {
            //TODO
            return base.Equals(obj);
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <returns>TODO</returns>
        public override int GetHashCode()
        {
            // TODO
            return base.GetHashCode();
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <returns>TODO</returns>
        public BsonDocument ToBsonDocument()
        {
            return new BsonDocument
            {
                { "processId", _processId } ,
                { "counter", _counter }
            };
        }
    }
}
