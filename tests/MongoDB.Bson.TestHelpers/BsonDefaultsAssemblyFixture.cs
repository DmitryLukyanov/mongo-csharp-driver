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
using MongoDB.Bson.TestHelpers.XunitExtensions;

namespace MongoDB.Bson.TestHelpers
{
    public class BsonDefaultsAssemblyFixture : IConfigureAssemblyFixture
    {
        public void Dispose()
        {
            // do nothing
        }

        public void Initialize()
        {
            var testWithDefaultGuidRepresentation = Environment.GetEnvironmentVariable("TEST_WITH_DEFAULT_GUID_REPRESENTATION");
            if (testWithDefaultGuidRepresentation != null)
            {
                if (Enum.TryParse<GuidRepresentation>(testWithDefaultGuidRepresentation, out var guidRepresentation))
                {
                    BsonDefaultsReflector.__guidRepresentation(guidRepresentation);
                }
            }

            var testWithDefaultGuidRepresentationMode = Environment.GetEnvironmentVariable("TEST_WITH_DEFAULT_GUID_REPRESENTATION_MODE");
            if (testWithDefaultGuidRepresentationMode != null)
            {
                if (Enum.TryParse<GuidRepresentationMode>(testWithDefaultGuidRepresentationMode, out var guidRepresentationMode))
                {
                    BsonDefaultsReflector.__guidRepresentationMode(guidRepresentationMode);
                }
            }
        }
    }

    internal class BsonDefaultsReflector
    {
        public static void __guidRepresentation(GuidRepresentation guidRepresentation)
        {
            Reflector.SetStaticFieldValue(typeof(BsonDefaults), nameof(__guidRepresentation), guidRepresentation);
        }

        public static void __guidRepresentationMode(GuidRepresentationMode guidRepresentationMode)
        {
            Reflector.SetStaticFieldValue(typeof(BsonDefaults), nameof(__guidRepresentationMode), guidRepresentationMode);
        }
    }
}
