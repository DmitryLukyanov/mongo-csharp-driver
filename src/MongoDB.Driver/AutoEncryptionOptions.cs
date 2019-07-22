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
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    /// <summary>
    /// Auto encryption options.
    /// </summary>
    public class AutoEncryptionOptions
    {
        // private fields
        private readonly bool _bypassAutoEncryption;
        private readonly IReadOnlyDictionary<string, object> _extraOptions;
        private readonly IMongoClient _keyVaultClient;
        //private readonly Lazy<IMongoCollection<BsonDocument>> _keyVaultCollection;
        private readonly CollectionNamespace _keyVaultNamespace;
        private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> _kmsProviders;
        private readonly IReadOnlyDictionary<string, BsonDocument> _schemaMap;

        // constructors
        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="keyVaultNamespace">TODO</param>
        /// <param name="kmsProviders">TODO</param>
        /// <param name="bypassAutoEncryption">TODO</param>
        /// <param name="extraOptions">TODO</param>
        /// <param name="keyVaultClient">TODO</param>
        /// <param name="schemaMap">TODO</param>
        public AutoEncryptionOptions(
            CollectionNamespace keyVaultNamespace,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> kmsProviders,
            Optional<bool> bypassAutoEncryption = default,
            Optional<IReadOnlyDictionary<string, object>> extraOptions = default,
            Optional<IMongoClient> keyVaultClient = default,
            Optional<IReadOnlyDictionary<string, BsonDocument>> schemaMap = default)
        {
            _keyVaultNamespace = Ensure.IsNotNull(keyVaultNamespace, nameof(keyVaultNamespace));
            _kmsProviders = Ensure.IsNotNull(kmsProviders, nameof(kmsProviders));
            _bypassAutoEncryption = bypassAutoEncryption.WithDefault(false);
            _extraOptions = extraOptions.WithDefault(null);
            _keyVaultClient = keyVaultClient.WithDefault(null);
            //_keyVaultCollection = new Lazy<IMongoCollection<BsonDocument>>(
            //    () =>
            //    {
            //        // todo: doesn't work!!! Needs to be moved to controller
            //        var keyVaultDatabase = _keyVaultClient.GetDatabase(_keyVaultNamespace.DatabaseNamespace.DatabaseName);
            //        var collection = keyVaultDatabase.GetCollection<BsonDocument>(_keyVaultNamespace.CollectionName);
            //        return collection;
            //    });
            _schemaMap = schemaMap.WithDefault(null);
        }

        // public properties
        /// <summary>
        /// Gets a value indicating whether to bypass automatic encryption.
        /// </summary>
        /// <value>
        ///   <c>true</c> if automatic encryption should be bypasssed; otherwise, <c>false</c>.
        /// </value>
        public bool BypassAutoEncryption => _bypassAutoEncryption;

        /// <summary>
        /// Gets the extra options.
        /// </summary>
        /// <value>
        /// The extra options.
        /// </value>
        public IReadOnlyDictionary<string, object> ExtraOptions => _extraOptions;

        /// <summary>
        /// Gets the key vault client.
        /// </summary>
        /// <value>
        /// The key vault client.
        /// </value>
        public IMongoClient KeyVaultClient => _keyVaultClient;

        ///// <summary>
        ///// TODO
        ///// </summary>
        ///// <value>
        ///// TODO
        ///// </value>
        //public IMongoCollection<BsonDocument> KeyVaultCollection => _keyVaultCollection.Value;

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

        /// <summary>
        /// Gets the schema map.
        /// </summary>
        /// <value>
        /// The schema map.
        /// </value>
        public IReadOnlyDictionary<string, BsonDocument> SchemaMap => _schemaMap;

        //todo:
        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="keyVaultNamespace">TODO</param>
        /// <param name="kmsProviders">TODO</param>
        /// <param name="bypassAutoEncryption">TODO</param>
        /// <param name="extraOptions">TODO</param>
        /// <param name="keyVaultClient">TODO</param>
        /// <param name="schemaMap">TODO</param>
        /// <returns></returns>
        public AutoEncryptionOptions With(
            Optional<CollectionNamespace> keyVaultNamespace = default,
            Optional<IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>>> kmsProviders = default,
            Optional<bool> bypassAutoEncryption = default,
            Optional<IReadOnlyDictionary<string, object>> extraOptions = default(Optional<IReadOnlyDictionary<string, object>>),
            Optional<IMongoClient> keyVaultClient = default,                            //todo: why client? can be null?
            Optional<IReadOnlyDictionary<string, BsonDocument>> schemaMap = default)
        {
            return new AutoEncryptionOptions(
                keyVaultNamespace.WithDefault(_keyVaultNamespace),
                kmsProviders.WithDefault(_kmsProviders),
                bypassAutoEncryption.WithDefault(_bypassAutoEncryption),
                new Optional<IReadOnlyDictionary<string, object>>(extraOptions.WithDefault(_extraOptions)),
                new Optional<IMongoClient>(keyVaultClient.WithDefault(_keyVaultClient)),
                new Optional<IReadOnlyDictionary<string, BsonDocument>>(schemaMap.WithDefault(_schemaMap)));
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="clientEncryptionOptions">TODO</param>
        /// <returns>TODO</returns>
        public static AutoEncryptionOptions FromClientEncryptionOptions(ClientEncryptionOptions clientEncryptionOptions)
        {
            return new AutoEncryptionOptions(
                keyVaultNamespace: clientEncryptionOptions.KeyVaultNamespace,
                kmsProviders: clientEncryptionOptions.KmsProviders,
                keyVaultClient: new Optional<IMongoClient>(clientEncryptionOptions.KeyVaultClient));
        }
    }
}
