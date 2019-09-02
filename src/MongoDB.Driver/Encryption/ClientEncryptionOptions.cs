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

using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver.Core.Misc;
using MongoDB.Shared;

namespace MongoDB.Driver
{
    /// <summary>
    /// Client encryption options.
    /// </summary>
    public class ClientEncryptionOptions
    {
        #region static
        /// <summary>
        /// Gets a new instance of the <see cref="ClientEncryptionOptions"/> initialized with values from a <see cref="AutoEncryptionOptions"/>.
        /// </summary>
        /// <param name="autoEncryptionOptions">The auto encryption options.</param>
        /// <returns>A new instance of <see cref="ClientEncryptionOptions"/>.</returns>
        public static ClientEncryptionOptions FromAutoEncryptionOptions(AutoEncryptionOptions autoEncryptionOptions)
        {
            return new ClientEncryptionOptions(
                keyVaultNamespace: autoEncryptionOptions.KeyVaultNamespace,
                kmsProviders: autoEncryptionOptions.KmsProviders,
                keyVaultClient: autoEncryptionOptions.KeyVaultClient); //todo:
        }
        #endregion

        // private fields
        private readonly IMongoClient _keyVaultClient;
        private readonly CollectionNamespace _keyVaultNamespace;
        private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> _kmsProviders;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="ClientEncryptionOptions"/> class.
        /// </summary>
        /// <param name="keyVaultClient">The key vault client.</param>
        /// <param name="keyVaultNamespace">The key vault namespace.</param>
        /// <param name="kmsProviders">The KMS providers.</param>
        public ClientEncryptionOptions(
            IMongoClient keyVaultClient,
            CollectionNamespace keyVaultNamespace,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> kmsProviders)
        {
            _keyVaultClient = Ensure.IsNotNull(keyVaultClient, nameof(keyVaultClient));
            _keyVaultNamespace = Ensure.IsNotNull(keyVaultNamespace, nameof(keyVaultNamespace));
            _kmsProviders = Ensure.IsNotNull(kmsProviders, nameof(kmsProviders));
        }

        // public properties
        /// <summary>
        /// Gets the key vault client.
        /// </summary>
        /// <value>
        /// The key vault client.
        /// </value>
        public IMongoClient KeyVaultClient => _keyVaultClient;

        /// <summary>
        /// Gets the key vault namespace.
        /// </summary>
        /// <value>
        /// The key vault namespace.
        /// </value>
        public CollectionNamespace KeyVaultNamespace => _keyVaultNamespace;

        /// <summary>
        /// Gets the KMS providers.
        /// </summary>
        /// <value>
        /// The KMS providers.
        /// </value>
        public IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> KmsProviders => _kmsProviders;

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (obj is ClientEncryptionOptions clientEncryptionOptions)
            {
                return
                    clientEncryptionOptions.KeyVaultNamespace == _keyVaultNamespace &&
                    clientEncryptionOptions.KmsProviders.SequenceEqual(_kmsProviders) &&
                    object.ReferenceEquals(clientEncryptionOptions.KeyVaultClient, _keyVaultClient);
            }
            else
            {
                return base.Equals(obj);
            }
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return new Hasher()
                .Hash(_keyVaultNamespace)
                .HashElements(_kmsProviders)
                .Hash(_keyVaultClient)
                .GetHashCode();
        }
    }
}
