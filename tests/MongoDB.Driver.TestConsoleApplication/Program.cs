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
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events.Diagnostics;

namespace MongoDB.Driver.TestConsoleApplication
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var test = new TransactionTest();
            await test.TestTransactionAsync();
        }

        public class TransactionTest
        {
            private const string DatabaseName = "PressureTest";
            private const string CollectionName = "Test";
            public MongoClient GetMongoClient(int timeout = 5)
            {
                var clientSettings = new MongoClientSettings();//MongoClientSettings.FromConnectionString(ConnectionString);
                clientSettings.ConnectTimeout = TimeSpan.FromSeconds(5);
                clientSettings.ServerSelectionTimeout = TimeSpan.FromSeconds(timeout);
                clientSettings.AllowInsecureTls = true;
                var mongoClient = new MongoClient(clientSettings);
                return mongoClient;
            }

            public async Task TestTransactionAsync()
            {
                var client = GetMongoClient();
                var tasks = new List<Task>();

                for (int i = 0; i < 5; ++i)
                {
                    //var client = GetMongoClient(i + 5);
                    tasks.Add(DoAsync(client));
                }
                await Task.WhenAll(tasks);
            }

            private async Task DoAsync(IMongoClient mongoClient)
            {
                Console.WriteLine("Client hashcode: " + mongoClient.GetHashCode());
                var collection = mongoClient.GetDatabase(DatabaseName).GetCollection<BsonDocument>(CollectionName);

                while (true)
                {
                    var uuid1 = Guid.NewGuid().ToString("N").Substring(24);
                    var uuid2 = Guid.NewGuid().ToString("N").Substring(24);
                    try
                    {
                        using (var session = await mongoClient.StartSessionAsync())
                        {
                            session.StartTransaction();
                            await collection.InsertOneAsync(session, new BsonDocument("Uuid", uuid1));
                            await collection.InsertOneAsync(session, new BsonDocument("Uuid", uuid2));

                            await session.CommitTransactionAsync();
                        }
                        Console.WriteLine($"[{uuid1}] [{uuid2}]");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("$$$ " + e.Message);
                        break;
                    }
                }
            }
        }
    }
}
