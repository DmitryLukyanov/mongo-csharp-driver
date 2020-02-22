﻿/* Copyright 2020-present MongoDB Inc.
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
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Operations;
using Xunit;

namespace MongoDB.Driver.Tests
{
    public class MongoIndexManagerTests
    {
        [Theory]
        [ParameterAttributeData]
        public void List_should_work_as_expected(
            [Values("{ singleIndex : 1 }", "{ compoundIndex1 : 1, compoundIndex2 : 1 }")] string key,
            [Values(false, true)] bool uniqueArgument,
            [Values(false, true)] bool async)
        {
            var indexKeyDocument = BsonDocument.Parse(key);
            var collection = GetEmptyCollection();
            var subject = collection.Indexes;

            subject.CreateOne(new CreateIndexModel<BsonDocument>(indexKeyDocument, new CreateIndexOptions() { Unique = uniqueArgument }));

            var indexesCursor =
                async
                    ? subject.ListAsync().GetAwaiter().GetResult()
                    : subject.List();
            var indexes = indexesCursor.ToList();

            indexes.Count.Should().Be(2);
            AssertIndex(collection.CollectionNamespace, indexes[0], "_id_");
            var indexName = IndexNameHelper.GetIndexName(indexKeyDocument);
            AssertIndex(collection.CollectionNamespace, indexes[1], indexName, unique : uniqueArgument);

            void AssertIndex(CollectionNamespace collectionNamespace, BsonDocument index, string nameValue, bool unique = false)
            {
                index["name"].Should().Be(BsonValue.Create(nameValue));

                if (unique)
                {
                    index["unique"].AsBoolean.Should().BeTrue();
                }
                else
                {
                    index.Contains("unique").Should().BeFalse();
                }

                if (CoreTestConfiguration.ServerVersion < new SemanticVersion(4, 3, 0))
                {
                    index["ns"].Should().Be(BsonValue.Create(collectionNamespace.ToString()));
                }
                else
                {
                    // the server doesn't return ns anymore
                    index.Contains("ns").Should().BeFalse();
                }
            }
        }

        // private methods
        private IMongoCollection<BsonDocument> GetEmptyCollection()
        {
            var collectionName = DriverTestConfiguration.CollectionNamespace.CollectionName;
            var client = DriverTestConfiguration.Client;
            var database = client.GetDatabase(DriverTestConfiguration.DatabaseNamespace.DatabaseName);
            database.DropCollection(collectionName);
            return database.GetCollection<BsonDocument>(collectionName);
        }
    }
}
