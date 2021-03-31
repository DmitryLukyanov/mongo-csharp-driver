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
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Configuration;

namespace MongoDB.Driver.Core.Events
{
    /// <summary>
    /// Occurs before a cluster is opened.
    /// </summary>
    public struct ClusterOpeningEvent
    {
        private readonly ClusterId _clusterId;
        private readonly ClusterSettings _clusterSettings;
        private readonly DateTime _timestamp;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterOpeningEvent"/> struct.
        /// </summary>
        /// <param name="clusterId">The cluster identifier.</param>
        /// <param name="clusterSettings">The cluster settings.</param>
        public ClusterOpeningEvent(ClusterId clusterId, ClusterSettings clusterSettings)
        {
            _clusterId = clusterId;
            _clusterSettings = clusterSettings;
            _timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the cluster identifier.
        /// </summary>
        public ClusterId ClusterId
        {
            get { return _clusterId; }
        }

        /// <summary>
        /// Gets the cluster settings.
        /// </summary>
        public ClusterSettings ClusterSettings
        {
            get { return _clusterSettings; }
        }

        /// <summary>
        /// Gets the timestamp.
        /// </summary>
        public DateTime Timestamp
        {
            get { return _timestamp; }
        }
    }
}
