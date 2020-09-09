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
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Authentication
{
    /// <inheritdoc/>
    public class AuthenticatorsFactory : IAuthenticatorsFactory
    {
        #region static
        /// <summary>
        /// Create a connection factory with empty list of authenticators.
        /// </summary>
        /// <returns>The empty authenticators list.</returns>
        public static IAuthenticatorsFactory CreateEmpty()
        {
            return new AuthenticatorsFactory(() => new IAuthenticator[0]);
        }
        #endregion

        private readonly Func<IEnumerable<IAuthenticator>> _authenticatorsFactoryFunc;

        /// <summary>
        /// Create an authenticatorsFactory.
        /// </summary>
        /// <param name="authenticatorsFactoryFunc">The authenticatorsFactoryFunc.</param>
        public AuthenticatorsFactory(Func<IEnumerable<IAuthenticator>> authenticatorsFactoryFunc)
        {
            _authenticatorsFactoryFunc = Ensure.IsNotNull(authenticatorsFactoryFunc, nameof(authenticatorsFactoryFunc));
        }

        /// <inheritdoc/>
        public IReadOnlyList<IAuthenticator> Create()
        {
            return _authenticatorsFactoryFunc().ToList();
        }
    }
}
