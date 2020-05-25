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
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events.Diagnostics;

namespace MongoDB.Driver.TestConsoleApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new MongoClient();
            // System.Threading.Thread.Sleep(40000);
            var database = client.GetDatabase("test");
            var collection = database.GetCollection<BsonDocument>("inventory");
            Thread.Sleep(10000000);
        }

        private static void ConfigureCluster(ClusterBuilder cb)
        {
#if NET452
            cb.UsePerformanceCounters("test", true);
#endif
        }
    }
}
