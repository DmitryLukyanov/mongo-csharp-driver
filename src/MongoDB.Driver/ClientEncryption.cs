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
using System.Threading;
using MongoDB.Bson;
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
        private readonly ClientEncryptionOptions _options;

        // constructors
        internal ClientEncryption(
            LibMongoCryptController libMongoCryptController,
            ClientEncryptionOptions options)
        {
            _libMongoCryptController = Ensure.IsNotNull(libMongoCryptController, nameof(libMongoCryptController));
            _options = Ensure.IsNotNull(options, nameof(options));
        }

        // public methods
        /// <summary>
        /// Creates a data key.
        /// </summary>
        /// <param name="dataKeyOptions">The data key options.</param>
        /// <param name="cancellationToken">TODO</param>
        /// <returns>A data key.</returns>
        public BsonValue CreateDataKey(DataKeyOptions dataKeyOptions, CancellationToken cancellationToken)
        {
            var key = ParseKmsKeyId(dataKeyOptions.MasterKey);
            return _libMongoCryptController.GenerateKey(key, cancellationToken);
        }

        /// <summary>
        /// Decrypts the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>The decrypted value.</returns>
        public BsonValue Decrypt(BsonBinaryData value)
        {
            return _libMongoCryptController.DecryptFields(value.Bytes, CancellationToken.None);
        }

        /// <summary>
        /// Encrypts the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="encryptOptions">The encrypt options.</param>
        /// <returns>The encrypted value.</returns>
        public BsonBinaryData Encrypt(BsonValue value, EncryptOptions encryptOptions)
        {
            return _libMongoCryptController.EncryptFields(null, null, CancellationToken.None);
        }

        private IKmsKeyId ParseKmsKeyId(BsonDocument masterKey)
        {
            if (!masterKey.TryGetValue("key", out var customerMasterKey))
            {
                // todo: or local?
                throw new ArgumentException("TODO");
            }

            if (!masterKey.TryGetValue("region", out var region))
            {
                // todo: or local
                throw new ArgumentException("TODO");
            }

            return new AwsKeyId(customerMasterKey.ToString(), region.ToString());
        }
    }
}
