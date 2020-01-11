
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
            var testWithDefaultGuidRepresentation = Environment.GetEnvironmentVariable("TESTWITHDEFAULTGUIDREPRESENTATION");
            if (testWithDefaultGuidRepresentation != null)
            {
                if (Enum.TryParse<GuidRepresentation>(testWithDefaultGuidRepresentation, out var guidRepresentation))
                {
                    BsonDefaultsReflector.__guidRepresentation(guidRepresentation);
                }
            }

            var testWithDefaultGuidRepresentationMode = Environment.GetEnvironmentVariable("TESTWITHDEFAULTGUIDREPRESENTATIONMODE");
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
