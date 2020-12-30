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
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace MongoDB.Driver.Encryption
{
    internal sealed class ExplicitEncryptionLibMongoCryptController
    {
        // constructors
        public ExplicitEncryptionLibMongoCryptController(
            ClientEncryptionOptions clientEncryptionOptions)
        {
        }

        // public methods
        public Guid CreateDataKey(
            string kmsProvider,
            IReadOnlyList<string> alternateKeyNames,
            BsonDocument masterKey,
            CancellationToken cancellationToken)
        {
            try
            {
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                throw new MongoEncryptionException(ex);
            }
        }

        public  Task<Guid> CreateDataKeyAsync(
            string kmsProvider,
            IReadOnlyList<string> alternateKeyNames,
            BsonDocument masterKey,
            CancellationToken cancellationToken)
        {
            try
            {
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                throw new MongoEncryptionException(ex);
            }
        }

        public BsonValue DecryptField(BsonBinaryData encryptedValue, CancellationToken cancellationToken)
        {
            try
            {
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                throw new MongoEncryptionException(ex);
            }
        }

        public  Task<BsonValue> DecryptFieldAsync(BsonBinaryData wrappedBinaryValue, CancellationToken cancellationToken)
        {
            try
            {
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                throw new MongoEncryptionException(ex);
            }
        }

        public BsonBinaryData EncryptField(
            BsonValue value,
            Guid? keyId,
            string alternateKeyName,
            EncryptionAlgorithm encryptionAlgorithm,
            CancellationToken cancellationToken)
        {
            try
            {
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                throw new MongoEncryptionException(ex);
            }
        }

        public Task<BsonBinaryData> EncryptFieldAsync(
            BsonValue value,
            Guid? keyId,
            string alternateKeyName,
            EncryptionAlgorithm encryptionAlgorithm,
            CancellationToken cancellationToken)
        {
            try
            {
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                throw new MongoEncryptionException(ex);
            }
        }
    }
}
