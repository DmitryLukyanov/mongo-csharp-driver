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
using System.IO;
using MongoDB.Bson;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events.Diagnostics;

namespace MongoDB.Driver.TestConsoleApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            //var client = new MongoClient("mongodb+srv://user:test@cluster0.caoi8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&loadBalanced=true");
            var client = new MongoClient("mongodb+srv://test20.test.build.10gen.cc/?loadBalanced=true");
            var database = client.GetDatabase("test");
            database.RunCommand<BsonDocument>("{ping : 1 }");
        }

        private static void ConfigureCluster(ClusterBuilder cb)
        {
#if NET452
            cb.UsePerformanceCounters("test", true);
#endif
        }
    }
}
