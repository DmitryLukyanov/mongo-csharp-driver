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

using System;
using MongoDB.Bson;

namespace MongoDB.Driver.Core.Misc
{
    /// <summary>
    /// TODO.
    /// </summary>
    /// <seealso cref="MongoDB.Driver.Core.Misc.Feature" />
    public class HintForFindAndModifyFeature : Feature
    {
        private readonly SemanticVersion _lastNotSupportedVersionThatThrows;

        /// <summary>
        /// Initializes a new instance of the <see cref="HintForWriteOperationsFeature"/> class.
        /// </summary>
        /// <param name="name">The name of the feature.</param>
        /// <param name="firstSupportedVersion">The first server version that supports the feature.</param>
        /// <param name="lastNotSupportedVersionThatThrows">The last not supported server version that throws the exception.</param>
        public HintForFindAndModifyFeature(string name, SemanticVersion firstSupportedVersion, SemanticVersion lastNotSupportedVersionThatThrows)
            : base(name, firstSupportedVersion)
        {
            _lastNotSupportedVersionThatThrows = lastNotSupportedVersionThatThrows;
        }

        /// <summary>
        /// Determines whether a feature is supported by a version of the server.
        /// </summary>
        /// <param name="serverVersion">The server version.</param>
        /// <param name="allowThrowingException">Determines whether the driver can throw exception or not.</param>
        public bool IsSupported(SemanticVersion serverVersion, out bool allowThrowingException)
        {
            allowThrowingException = serverVersion < _lastNotSupportedVersionThatThrows;
            return base.IsSupported(serverVersion);
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="serverVersion">TODO</param>
        /// <param name="hint">TODO</param>
        public void ThrowIfNotSupportedAndAllowed(SemanticVersion serverVersion, BsonValue hint)
        {
            if (hint != null &&
                !IsSupported(serverVersion, out var allowThrowingException) &&
                allowThrowingException)
            {
                throw new NotSupportedException($"Server version {serverVersion} does not support collations.");
            }
        }
    }
}
