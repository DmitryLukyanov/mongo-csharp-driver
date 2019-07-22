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


using MongoDB.Crypt;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace MongoDB.Driver.LibMongoCrypt
{
    internal interface IEncryptionSource
    {
        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="options">TODO</param>
        CryptClient Get(CryptOptions options);
    }

    internal class EncryptionSource : IEncryptionSource
    {
        private readonly ConcurrentDictionary<CryptOptions, CryptClient> _cryptClientCache;

        public EncryptionSource()
        {
            _cryptClientCache = new ConcurrentDictionary<CryptOptions, CryptClient>(new CryptClientComparer());
        }

        public CryptClient Get(CryptOptions options)
        {
            return _cryptClientCache.GetOrAdd(options, CreateCryptClient);
        }

        // private methods
        private CryptClient CreateCryptClient(CryptOptions options)
        {
            return CryptClientFactory.Create(options);
        }

        private class CryptClientComparer : IEqualityComparer<CryptOptions>
        {
            public bool Equals(CryptOptions x, CryptOptions y)
            {
                if (x == null && y == null)
                {
                    return true;
                }
                else if (x == null || y == null)
                {
                    return false;
                }
                else
                {
                    // todo
                    var res = (x.Schema ?? new byte[0]).SequenceEqual(y.Schema ?? new byte[0]) && x.KmsCredentials.KmsType == y.KmsCredentials.KmsType;
                    return res;
                }
            }

            public int GetHashCode(CryptOptions obj)
            {
                // todo: check that it's enough
                //return obj.GetHashCode();
                return 1;
            }
        }
    }
}
