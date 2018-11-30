﻿/* Copyright 2017-present MongoDB Inc.
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
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using Xunit;

namespace MongoDB.Driver.Tests
{
    public class PipelineStageDefinitionBuilderTests
    {
        // public methods
        [Theory]
        [InlineData(null, "{ $changeStream : { fullDocument : 'default' } }")]
        [InlineData(false, "{ $changeStream : { fullDocument : 'default' } }")]
        [InlineData(true, "{ $changeStream : { fullDocument : 'default', allChangesForCluster : true } }")]
        public void ChangeStream_with_allChangesForCluster_should_return_the_expected_result(bool? allChangesForCluster, string expectedStage)
        {
            var options = new ChangeStreamStageOptions
            {
                AllChangesForCluster = allChangesForCluster
            };

            var result = PipelineStageDefinitionBuilder.ChangeStream<BsonDocument>(options);

            var stage = RenderStage(result);
            stage.Document.Should().Be(expectedStage);
        }

        [Theory]
        [InlineData(ChangeStreamFullDocumentOption.Default, "{ $changeStream : { fullDocument : 'default' } }")]
        [InlineData(ChangeStreamFullDocumentOption.UpdateLookup, "{ $changeStream : { fullDocument : 'updateLookup' } }")]
        public void ChangeStream_with_fullDocument_should_return_the_expected_result(ChangeStreamFullDocumentOption fullDocument, string expectedStage)
        {
            var options = new ChangeStreamStageOptions
            {
                FullDocument = fullDocument
            };

            var result = PipelineStageDefinitionBuilder.ChangeStream<BsonDocument>(options);

            var stage = RenderStage(result);
            stage.Document.Should().Be(expectedStage);
        }

        [Theory]
        [InlineData(null, "{ $changeStream : { fullDocument : 'default' } }")]
        [InlineData("{ x : 1 }", "{ $changeStream : { fullDocument : 'default', resumeAfter : { x : 1 } } }")]
        public void ChangeStream_with_resumeAfter_should_return_the_expected_result(string resumeAfterJson, string expectedStage)
        {
            var resumeAfter = resumeAfterJson == null ? null : BsonDocument.Parse(resumeAfterJson);
            var options = new ChangeStreamStageOptions
            {
                ResumeAfter = resumeAfter
            };

            var result = PipelineStageDefinitionBuilder.ChangeStream<BsonDocument>(options);

            var stage = RenderStage(result);
            stage.Document.Should().Be(expectedStage);
        }

        [Theory]
        [InlineData(null, null, "{ $changeStream : { fullDocument : 'default' } }")]
        [InlineData(1, 2, "{ $changeStream : { fullDocument : 'default', startAtOperationTime : { $timestamp: { t : 1, i : 2 } } } }")]
        public void ChangeStream_with_startAtOperationTime_should_return_the_expected_result(int? t, int? i, string expectedStage)
        {
            var startAtOperationTime = t.HasValue ? new BsonTimestamp(t.Value, i.Value) : null;
            var options = new ChangeStreamStageOptions
            {
                StartAtOperationTime = startAtOperationTime
            };

            var result = PipelineStageDefinitionBuilder.ChangeStream<BsonDocument>(options);

            var stage = RenderStage(result);
            stage.Document.Should().Be(expectedStage);
        }

        [Fact]
        public void ChangeStream_should_return_the_expected_result_when_options_isNull()
        {
            ChangeStreamStageOptions options = null;

            var result = PipelineStageDefinitionBuilder.ChangeStream<BsonDocument>(options);

            var stage = RenderStage(result);
            stage.Document.Should().Be("{ $changeStream : { fullDocument : \"default\" } }");
        }

        [Fact]
        public void Lookup_with_let_should_return_the_expected_result()
        {
            var client = new MongoClient(CoreTestConfiguration.ConnectionString.ToString());
            var warehousesCollection = client.GetDatabase("test").GetCollection<BsonDocument>("warehouses");

            var pipeline = new EmptyPipelineDefinition<BsonDocument>()
                .Match(new BsonDocument("$expr",
                    new BsonDocument("$and", new BsonArray
                    {
                        new BsonDocument("$eq", new BsonArray { "$stock_item", "$$order_item" }),
                        new BsonDocument("$gte", new BsonArray { "$instock", "$$order_qty" })
                    })))
                .Project(Builders<BsonDocument>.Projection
                    .Exclude("_id")
                    .Exclude("stock_item"));

            var result = PipelineStageDefinitionBuilder.Lookup<BsonDocument, BsonDocument, BsonDocument>
                (
                    warehousesCollection,
                    pipeline,
                    new StringFieldDefinition<BsonDocument>("stockdata"),
                    new BsonDocument
                    {
                        { "order_item", "$item" },
                        { "order_qty", "$ordered" }
                    }
                );

            RenderStage(result).Document.Should().Be(@"
                {
                    '$lookup': 
                    {
                        'from': 'warehouses',
                        'let': 
                        {
                            'order_item': '$item',
                            'order_qty': '$ordered'
                        },
                        'pipeline': [
                        {
                            '$match': 
                                { '$expr': 
                                    { '$and': 
                                        [ 
                                            { '$eq': ['$stock_item', '$$order_item'] }, 
                                            { '$gte': ['$instock', '$$order_qty'] }
                                        ]
                                    } 
                                }
                        }, 
                        { '$project': { '_id': 0, 'stock_item': 0 } }],
                        'as': 'stockdata'
                    }
                }");
        }

        [Fact]
        public void Lookup_with_empty_params_should_throw_expected_exception()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                PipelineStageDefinitionBuilder.Lookup<BsonDocument, BsonDocument, BsonDocument>
                (
                    null,
                    new EmptyPipelineDefinition<BsonDocument>(), 
                    new StringFieldDefinition<BsonDocument>("stockdata")
                );
            });

            Assert.Throws<ArgumentNullException>(() =>
            {
                var client = new MongoClient(CoreTestConfiguration.ConnectionString.ToString());
                var warehousesCollection = client.GetDatabase("test").GetCollection<BsonDocument>("warehouses");
                PipelineStageDefinitionBuilder.Lookup<BsonDocument, BsonDocument, BsonDocument>
                (
                    warehousesCollection,
                    null,
                    new StringFieldDefinition<BsonDocument>("stockdata")
                );
            });

            Assert.Throws<ArgumentNullException>(() =>
            {
                var client = new MongoClient(CoreTestConfiguration.ConnectionString.ToString());
                var warehousesCollection = client.GetDatabase("test").GetCollection<BsonDocument>("warehouses");
                PipelineStageDefinitionBuilder.Lookup<BsonDocument, BsonDocument, BsonDocument>
                (
                    warehousesCollection,
                    new EmptyPipelineDefinition<BsonDocument>(),
                    null
                );
            });
        }

        [Fact]
        public void Lookup_without_optional_let_should_return_the_expected_result()
        {
            var client = new MongoClient(CoreTestConfiguration.ConnectionString.ToString());
            var warehousesCollection = client.GetDatabase("test").GetCollection<BsonDocument>("warehouses");

            var pipeline = new EmptyPipelineDefinition<BsonDocument>();

            var result = PipelineStageDefinitionBuilder.Lookup<BsonDocument, BsonDocument, BsonDocument>
                (
                    warehousesCollection,
                    pipeline,
                    new StringFieldDefinition<BsonDocument>("stockdata")
                );

            RenderStage(result).Document.Should().Be(@"
                {
                    '$lookup': 
                    {
                        'from': 'warehouses',
                        'pipeline': [ ],
                        'as': 'stockdata'
                    }
                }");
        }

        // private methods
        private RenderedPipelineStageDefinition<ChangeStreamDocument<BsonDocument>> RenderStage(PipelineStageDefinition<BsonDocument, ChangeStreamDocument<BsonDocument>> stage)
        {
            return stage.Render(BsonDocumentSerializer.Instance, BsonSerializer.SerializerRegistry);
        }

        private RenderedPipelineStageDefinition<TOutput> RenderStage<TInput, TOutput>(PipelineStageDefinition<TInput, TOutput> stage)
        {
            var registry = BsonSerializer.SerializerRegistry;
            var serializer = registry.GetSerializer<TInput>();
            return stage.Render(serializer, registry);
        }
    }
}
