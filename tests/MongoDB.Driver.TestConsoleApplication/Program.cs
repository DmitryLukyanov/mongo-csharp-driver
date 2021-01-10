/* Copyright 2010-present MongoDB Inc.
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
using System.IO;
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events.Diagnostics;
using MongoDB.Driver.Linq.Translators;

namespace MongoDB.Driver.TestConsoleApplication
{
    class Program
    {
        static void Main(string[] args)
        {
                var result = Project(p => new { p.Id, p.A }, "{ _id: 1, A: \"Jack\" }");
                var projection = result.Projection;

                Console.WriteLine(projection);
        }

        private static ProjectedResult<T> Project<T>(Expression<Func<Root, T>> projector, string json)
        {
            var serializer = BsonSerializer.SerializerRegistry.GetSerializer<Root>();
            var projectionInfo = FindProjectionTranslator.Translate<Root, T>(projector, serializer, BsonSerializer.SerializerRegistry);

            using (var reader = new JsonReader(json))
            {
                var context = BsonDeserializationContext.CreateRoot(reader);
                return new ProjectedResult<T>
                {
                    Projection = projectionInfo.Document,
                    Value = projectionInfo.ProjectionSerializer.Deserialize(context)
                };
            }
        }

        private class ProjectedResult<T>
        {
            public BsonDocument Projection;
            public T Value;
        }

        private class Root
        {
            public int Id { get; set; }

            public string A { get; set; }

            public string A1 { get; set; }

            public string B { get; set; }

            public C C { get; set; }

            public IEnumerable<C> G { get; set; }
        }

        public class C
        {
            public string D { get; set; }

            public E E { get; set; }

            public E E1 { get; set; }
        }

        public class E
        {
            public int F { get; set; }

            public int H { get; set; }

            public IEnumerable<string> I { get; set; }

            public E E1 { get; set; }
        }

        private static void ConfigureCluster(ClusterBuilder cb)
        {
#if NET452
            cb.UsePerformanceCounters("test", true);
#endif
        }
    }
}
