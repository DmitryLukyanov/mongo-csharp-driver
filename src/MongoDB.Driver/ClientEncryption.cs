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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Crypt;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.LibMongoCrypt;

namespace MongoDB.Driver
{
    /// <summary>
    /// Explicit client encryption.
    /// </summary>
    public class ClientEncryption
    {
        // private fields
        private readonly LibMongoCryptController _libMongoCryptController;

        // constructors
        internal ClientEncryption(LibMongoCryptController libMongoCryptController)
        {
            _libMongoCryptController = Ensure.IsNotNull(libMongoCryptController, nameof(libMongoCryptController));
        }

        // public methods
        /// <summary>
        /// Creates a data key.
        /// </summary>
        /// <param name="kmsProvider">The kms provider.</param>
        /// <param name="dataKeyOptions">The data key options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A data key.</returns>
        public BsonValue CreateDataKey(string kmsProvider, DataKeyOptions dataKeyOptions, CancellationToken cancellationToken)
        {
            var key = GetKmsProvider(kmsProvider, dataKeyOptions);
            return _libMongoCryptController.GenerateKey(key, cancellationToken);
        }

        /// <summary>
        /// Creates a data key.
        /// </summary>
        /// <param name="kmsProvider">The kms provider.</param>
        /// <param name="dataKeyOptions">The data key options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A data key.</returns>
        public async Task<BsonValue> CreateDataKeyAsync(string kmsProvider, DataKeyOptions dataKeyOptions, CancellationToken cancellationToken)
        {
            var key = GetKmsProvider(kmsProvider, dataKeyOptions);
            return await _libMongoCryptController.GenerateKeyAsync(key, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Decrypts the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The decrypted value.</returns>
        public BsonBinaryData Decrypt(BsonBinaryData value, CancellationToken cancellationToken)
        {
            return _libMongoCryptController.DecryptField(value.Bytes, cancellationToken);
        }

        /// <summary>
        /// Decrypts the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The decrypted value.</returns>
        public async Task<BsonBinaryData> DecryptAsync(BsonBinaryData value, CancellationToken cancellationToken)
        {
            return await _libMongoCryptController.DecryptFieldAsync(value.Bytes, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Encrypts the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="encryptOptions">The encrypt options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The encrypted value.</returns>
        public BsonBinaryData Encrypt(BsonValue value, EncryptOptions encryptOptions, CancellationToken cancellationToken)
        {
            PrepareEncryptOptions(value, encryptOptions, out var keyId, out var valueBytes, out var keyAltNameBytes, out var algorithm);
            var encryptedBytes = _libMongoCryptController.EncryptField(
                valueBytes,
                keyId,
                keyAltNameBytes,
                algorithm,
                cancellationToken);
            var rawDocument = new RawBsonDocument(encryptedBytes);
            return new BsonBinaryData(rawDocument["v"].AsByteArray, BsonBinarySubType.Encryption);
        }

        /// <summary>
        /// Encrypts the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="encryptOptions">The encrypt options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The encrypted value.</returns>
        public async Task<BsonBinaryData> EncryptAsync(BsonValue value, EncryptOptions encryptOptions, CancellationToken cancellationToken)
        {
            PrepareEncryptOptions(value, encryptOptions, out var keyId, out var valueBytes, out var keyAltNameBytes, out var algorithm);
            var encryptedBytes = await _libMongoCryptController
                .EncryptFieldAsync(
                    valueBytes,
                    keyId,
                    keyAltNameBytes,
                    algorithm,
                    cancellationToken)
                .ConfigureAwait(false);
            var rawDocument = new RawBsonDocument(encryptedBytes);
            return new BsonBinaryData(rawDocument["v"].AsByteArray, BsonBinarySubType.Encryption);
        }

        private IKmsKeyId GetKmsProvider(string kmsProvider, DataKeyOptions dataKeyOptions)
        {
            IEnumerable<byte[]> keyAltNamesBytes = null;
            if (dataKeyOptions.KeyAltNames != null)
            {
                keyAltNamesBytes = dataKeyOptions
                    .KeyAltNames
                    .Select(c => new BsonDocument("keyAltName", c))
                    .Select(c => c.ToBson(serializer: BsonValueSerializer.Instance));
            }

            switch (kmsProvider)
            {
                case "aws":
                    var masterKey = dataKeyOptions.MasterKey;
                    if (keyAltNamesBytes != null)
                    {
                        return new AwsKeyId(masterKey["key"].ToString(), masterKey["region"].ToString(), keyAltNamesBytes);
                    }
                    else
                    {
                        return new AwsKeyId(masterKey["key"].ToString(), masterKey["region"].ToString());
                    }
                case "local":
                    if (keyAltNamesBytes != null)
                    {
                        return new LocalKeyId(keyAltNamesBytes);
                    }
                    else
                    {
                        return new LocalKeyId();
                    }
                default:
                    throw new ArgumentException($"Unexpected kmsProvider {kmsProvider}.");
            }
        }

        private void PrepareEncryptOptions(
            BsonValue value,
            EncryptOptions encryptOptions,
            out Guid? keyId,
            out byte[] valueBytes,
            out byte[] keyAltNameBytes,
            out EncryptionAlgorithm algorithm)
        {
            keyId = encryptOptions.KeyId != null ? new Guid(encryptOptions.KeyId) : (Guid?)null;
            valueBytes = new BsonDocument("v", value).ToBson(serializer: BsonValueSerializer.Instance);
            algorithm = (EncryptionAlgorithm)Enum.Parse(typeof(EncryptionAlgorithm), encryptOptions.Algorithm);
            keyAltNameBytes =
                !string.IsNullOrWhiteSpace(encryptOptions.KeyAltName)
                    ? new BsonDocument("keyAltName", encryptOptions.KeyAltName).ToBson(serializer: BsonValueSerializer.Instance)
                    : null;
        }
    }
}
