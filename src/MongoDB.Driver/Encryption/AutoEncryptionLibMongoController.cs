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
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol;

namespace MongoDB.Driver.Encryption
{
    internal sealed class AutoEncryptionLibMongoCryptController : IBinaryDocumentFieldDecryptor, IBinaryCommandFieldEncryptor
    {
        // private fields
        private readonly MongocryptdFactory _mongocryptdFactory;

        // constructors
        public AutoEncryptionLibMongoCryptController(
            IMongoClient client,
            AutoEncryptionOptions autoEncryptionOptions)
        {
            _mongocryptdFactory = new MongocryptdFactory(autoEncryptionOptions.ExtraOptions);
        }

        // public methods
        public byte[] DecryptFields(byte[] encryptedDocumentBytes, CancellationToken cancellationToken)
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

        public  Task<byte[]> DecryptFieldsAsync(byte[] encryptedDocumentBytes, CancellationToken cancellationToken)
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

        public byte[] EncryptFields(string databaseName, byte[] unencryptedCommandBytes, CancellationToken cancellationToken)
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

        public Task<byte[]> EncryptFieldsAsync(string databaseName, byte[] unencryptedCommandBytes, CancellationToken cancellationToken)
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
