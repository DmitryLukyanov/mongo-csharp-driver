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

namespace MongoDB.Driver.Core.Misc
{
    internal class ExceptionHelper
    {
        #region static
        private static IEqualityComparer<Exception> __exceptionEqualityComparer = new ExceptionEqualityComparerImpl();
        #endregion

        /// <summary>
        /// Compares two exceptions.
        /// </summary>
        /// <param name="a">The first exception.</param>
        /// <param name="b">The second exception.</param>
        /// <returns>True if both exceptions are equal, or if both are null.</returns>
        public static bool Equals(Exception a, Exception b)
        {
            return __exceptionEqualityComparer.Equals(a, b);
        }

        // nested types
        private class ExceptionEqualityComparerImpl : IEqualityComparer<Exception>
        {
            public bool Equals(Exception x, Exception y)
            {
                if (x == null && y == null)
                {
                    return true;
                }
                else if (x == null || y == null)
                {
                    return false;
                }

                if (x.GetType() != y.GetType())
                {
                    return false;
                }

                if (!string.Equals(x.Message, y.Message, StringComparison.Ordinal) ||
                    !string.Equals(x.StackTrace, y.StackTrace, StringComparison.Ordinal))
                {
                    return false;
                }

                if (x.InnerException != null || y.InnerException != null)
                {
                    return Equals(x.InnerException, y.InnerException);
                }

                return true;
            }

            public int GetHashCode(Exception obj)
            {
                return obj.GetHashCode();
            }
        }
    }
}
