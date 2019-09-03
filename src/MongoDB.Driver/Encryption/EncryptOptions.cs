﻿/* Copyright 2019-present MongoDB Inc.
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

using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    /// <summary>
    /// Encryption options for explicit encryption.
    /// </summary>
    public class EncryptOptions
    {
        // private fields
        private readonly string _algorithm;
        private readonly string _alternateKeyName;
        private readonly byte[] _keyId;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="EncryptOptions"/> class.
        /// </summary>
        /// <param name="algorithm">The encryption algorithm.</param>
        /// <param name="alternateKeyName">The alternate key name.</param>
        /// <param name="keyId">The keyId.</param>
        public EncryptOptions(
            string algorithm,
            Optional<string> alternateKeyName = default,
            Optional<byte[]> keyId = default)
        {
            _algorithm = Ensure.IsNotNull(algorithm, nameof(algorithm));
            _alternateKeyName = alternateKeyName.WithDefault(null);
            _keyId = keyId.WithDefault(null);
            EnsureThatOptionsAreValid();
        }

        // public properties
        /// <summary>
        /// Gets the algorithm.
        /// </summary>
        /// <value>
        /// The algorithm.
        /// </value>
        public string Algorithm=> _algorithm;

        /// <summary>
        /// Gets the alternate key name.
        /// </summary>
        /// <value>
        /// The alternate key name.
        /// </value>
        public string AlternateKeyName => _alternateKeyName;

        /// <summary>
        /// Gets the key identifier.
        /// </summary>
        /// <value>
        /// The key identifier.
        /// </value>
        public byte[] KeyId => _keyId;

        // private methods
        private void EnsureThatOptionsAreValid()
        {
            Ensure.That(!(_keyId == null && _alternateKeyName == null), "KeyId and AlternateKeyName may not both be null.");
            Ensure.That(!(_keyId != null && _alternateKeyName != null), "KeyId and AlternateKeyName may not both be set.");
        }
    }
}
