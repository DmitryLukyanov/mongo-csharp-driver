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
            var sett = new MongoClientSettings()
            {
                //Timeout = TimeSpan.FromMinutes(10)
            };
            var client = new MongoClient(sett);
            var db = client.GetDatabase("db");
            var coll = db.GetCollection<BsonDocument>("c", new MongoCollectionSettings() { Timeout = TimeSpan.FromSeconds(30)});
            var sess = client.StartSession();

            coll.InsertOne(sess, new BsonDocument());
        }

        private static void ConfigureCluster(ClusterBuilder cb)
        {
#if NET452
            cb.UsePerformanceCounters("test", true);
#endif
        }
    }
}
