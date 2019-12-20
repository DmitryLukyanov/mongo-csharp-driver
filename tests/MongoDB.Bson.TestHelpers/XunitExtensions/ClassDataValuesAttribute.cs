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
using System.Linq;
using System.Reflection;

namespace MongoDB.Bson.TestHelpers.XunitExtensions
{
    [AttributeUsage(AttributeTargets.Parameter, AllowMultiple = false)]
    public class ClassDataValuesAttribute : Attribute, IValueGeneratorAttribute
    {
        private readonly Type _classDataType;

        public ClassDataValuesAttribute(Type classDataType)
        {
            _classDataType = classDataType;
        }

        public object[] GenerateValues()
        {
            if (IsValidType())
            {
                var classDataTestCasesInstance = (IEnumerable<object[]>)Activator.CreateInstance(_classDataType);
                return classDataTestCasesInstance
                    .Select(c =>
                    {
                        if (c.Length != 1)
                        {
                            throw new ArgumentException("Class data instance must contain only one argument.");
                        }
                        return c[0];
                    })
                    .ToArray();
            }
            throw new ArgumentException("Class data type must be assignable from IEnumerable<object[]>.");
        }

        // private methods
        private bool IsValidType()
        {
            var interfacesTypes = _classDataType.GetInterfaces().Select(t => t.GetTypeInfo());
            return interfacesTypes
                .Any(i => 
                    i.IsGenericType && 
                    i.GetGenericTypeDefinition() == typeof(IEnumerable<>) &&
                    i.GenericTypeArguments.Single() == typeof(object[]));
        }
    }
}
