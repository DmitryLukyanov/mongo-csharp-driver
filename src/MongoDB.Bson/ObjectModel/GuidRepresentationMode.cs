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

using System;
using System.Linq;
using MongoDB.Shared;

namespace MongoDB.Bson
{
    /// <summary>
    /// Whether to handle GuidRepresentation using the v2.x mode of the v3.x mode.
    /// See the reference documentation for details.
    /// </summary>
    public class GuidRepresentationMode : IEquatable<GuidRepresentationMode>
    {
        #region static
        // static field
        private static GuidRepresentationMode __v2 = new GuidRepresentationMode(2);

        private static GuidRepresentationMode __v3 = new GuidRepresentationMode(3);

        // static properties
        /// <summary>
        /// Handle GuidRepresentation using the v2.x mode.
        /// </summary>
        public static GuidRepresentationMode V2 => __v2;

        /// <summary>
        /// Handle GuidRepresentation using the v3.x mode.
        /// </summary>
        public static GuidRepresentationMode V3 => __v3;

        // internal static properties
        internal static GuidRepresentationMode[] All => new[] { V2, V3 };

        // static methods
        /// <summary>
        /// Throws exception if GuidRepresentationMode.V2 is invalid in the current context.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <exception cref="InvalidOperationException">GuidRepresentation mode v2 is invalid in the current context.</exception>
        public static void ThrowIfInvalidMode(string message = null)
        {
#pragma warning disable 618
            if (BsonDefaults.GuidRepresentationMode != GuidRepresentationMode.V2)
#pragma warning restore 618
            {
                message = message ?? "GuidRepresentation mode v2 is invalid in the current context.";
                throw new InvalidOperationException(message);
            }
        }

        // internal static methods
        internal static bool TryCreateGuidRepresentationMode(int version, out GuidRepresentationMode mode)
        {
            mode = null;

            if (GuidRepresentationMode.All.Any(m => m.Version == version))
            {
                mode = new GuidRepresentationMode(version);
                return true;
            }
            return false;
        }
        #endregion

        private readonly int _version;

        // private constructor
        private GuidRepresentationMode(int version)
        {
            _version = version;
        }

        /// <summary>
        /// The version of guid representation mode.
        /// </summary>
        public int Version => _version;

        /// <inheritdoc />
        public override bool Equals(object other)
        {
            return Equals(other as GuidRepresentationMode);
        }

        /// <inheritdoc />
        public bool Equals(GuidRepresentationMode other)
        {
            if (other == null)
            {
                return false;
            }

            return _version == other._version;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return new Hasher()
                .Hash(_version)
                .GetHashCode();
        }
    }
}
