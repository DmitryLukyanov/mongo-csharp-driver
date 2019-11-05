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

using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using Xunit;

namespace MongoDB.Driver.Examples.TransactionExamplesForDocs
{
    public class WithTransactionExample1
    {
        [Fact]
        public void Example1()
        {
            RequireServer.Check().ClusterTypes(ClusterType.ReplicaSet, ClusterType.Sharded).Supports(Feature.Transactions);

            var connectionString = CoreTestConfiguration.ConnectionString.ToString();
            DropCollections(
                connectionString, 
                CollectionNamespace.FromFullName("mydb1.foo"), 
                CollectionNamespace.FromFullName("mydb2.bar"));
            var result = string.Empty;

            // Start Transactions withTxn API Example 1
            // For a replica set, include the replica set name and a seedlist of the members in the URI string; e.g.
            // string uri = "mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017/?replicaSet=myRepl";
            // For a sharded cluster, connect to the mongos instances; e.g.
            // string uri = "mongodb://mongos0.example.com:27017,mongos1.example.com:27017:27017/";
            var client = new MongoClient(connectionString);

            // Prereq: Create collections. CRUD operations in transactions must be on existing collections.
            var database1 = client.GetDatabase("mydb1");
            var collection1 = database1.GetCollection<BsonDocument>("foo").WithWriteConcern(WriteConcern.WMajority);
            collection1.InsertOne(new BsonDocument("abc", 0));

            var database2 = client.GetDatabase("mydb2");
            var collection2 = database2.GetCollection<BsonDocument>("bar").WithWriteConcern(WriteConcern.WMajority);
            collection2.InsertOne(new BsonDocument("xyz", 0));

            // Step 1: Start a client session.
            using (var clientSession = client.StartSession())
            {
                // Step 2: Optional. Define options to use for the transaction.
                var transactionOptions = new TransactionOptions(
                    readPreference: ReadPreference.Primary,
                    readConcern: ReadConcern.Local,
                    writeConcern: WriteConcern.WMajority);

                // Step 3: Define the sequence of operations to perform inside the transactions
                result = clientSession.WithTransaction(
                    (handle, token) =>
                    {
                        collection1.InsertOne(clientSession, new BsonDocument("abc", 1));
                        collection2.InsertOne(clientSession, new BsonDocument("xyz", 999));
                        return "Inserted into collections in different databases";
                    },
                    transactionOptions);
            }
            //End Transactions withTxn API Example 1

            result.Should().Be("Inserted into collections in different databases");

            var abcDocuments = collection1.Find(FilterDefinition<BsonDocument>.Empty).ToList();
            abcDocuments.Count.Should().Be(2);
            abcDocuments[0]["abc"].Should().Be(0);
            abcDocuments[1]["abc"].Should().Be(1);

            var xyzDocuments = collection2.Find(FilterDefinition<BsonDocument>.Empty).ToList();
            xyzDocuments.Count.Should().Be(2);
            xyzDocuments[0]["xyz"].Should().Be(0);
            xyzDocuments[1]["xyz"].Should().Be(999);
        }

        // private methods
        private void DropCollections(string connectionString, params CollectionNamespace[] collectionNamespaces)
        {
            var client = new MongoClient(connectionString);
            foreach (var collectionNamespace in collectionNamespaces)
            {
                var database = client.GetDatabase(collectionNamespace.DatabaseNamespace.DatabaseName);
                database.DropCollection(collectionNamespace.CollectionName);
            }
        }
    }
}
