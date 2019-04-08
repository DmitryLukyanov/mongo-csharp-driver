/* Copyright 2017-present MongoDB Inc.
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
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MongoDB.Bson.TestHelpers;
using Xunit;

namespace MongoDB.Driver.Core.Operations
{
    public class ChangeStreamOperationTests : OperationTestBase
    {
        [Fact]
        public void ChangeStreamOperation_should_not_calculate_effective_options_for_non_resume_process()
        {
            var pipeline = new BsonDocument[0];
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();

            var resumeAfter = new BsonDocument("a", 1);
            var startAfter = new BsonDocument("b", 2);
            var startAtOperationTime = BsonTimestamp.Create(3L);

            ChangeStreamOperation<ChangeStreamDocument<BsonDocument>> subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                ResumeAfter = resumeAfter,
                StartAfter = startAfter,
                StartAtOperationTime = startAtOperationTime
            };

            var result = subject.CreateChangeStreamStage(false);

            var changeStream = result.GetValue("$changeStream").AsBsonDocument;
            changeStream.GetValue("resumeAfter").Should().Be(resumeAfter);
            changeStream.GetValue("startAfter").Should().Be(startAfter);
            changeStream.GetValue("startAtOperationTime").Should().Be(startAtOperationTime);
        }

        [Fact]
        public void constructor_with_database_should_initialize_instance()
        {
            var databaseNamespace = new DatabaseNamespace("foo");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();

            var subject = new ChangeStreamOperation<BsonDocument>(databaseNamespace, pipeline, resultSerializer, messageEncoderSettings);

            subject.BatchSize.Should().NotHaveValue();
            subject.Collation.Should().BeNull();
            subject.CollectionNamespace.Should().BeNull();
            subject.DatabaseNamespace.Should().Be(databaseNamespace);
            subject.FullDocument.Should().Be(ChangeStreamFullDocumentOption.Default);
            subject.MaxAwaitTime.Should().NotHaveValue();
            subject.MessageEncoderSettings.Should().BeSameAs(messageEncoderSettings);
            subject.Pipeline.Should().Equal(pipeline);
            subject.ReadConcern.Should().Be(ReadConcern.Default);
            subject.ResultSerializer.Should().BeSameAs(resultSerializer);
            subject.ResumeAfter.Should().BeNull();
            subject.StartAfter.Should().BeNull();
            subject.StartAtOperationTime.Should().BeNull();
        }

        [Fact]
        public void constructor_with_database_should_throw_when_databaseNamespace_is_null()
        {
            DatabaseNamespace databaseNamespace = null;
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(databaseNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("databaseNamespace");
        }

        [Fact]
        public void constructor_with_database_should_throw_when_pipeline_is_null()
        {
            var databaseNamespace = new DatabaseNamespace("foo");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            IBsonSerializer<BsonDocument> resultSerializer = null;
            var messageEncoderSettings = new MessageEncoderSettings();


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(databaseNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("resultSerializer");
        }

        [Fact]
        public void constructor_with_database_should_throw_when_messageEncoderSettings_is_null()
        {
            var databaseNamespace = new DatabaseNamespace("foo");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            MessageEncoderSettings messageEncoderSettings = null;


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(databaseNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("messageEncoderSettings");
        }

        [Fact]
        public void constructor_with_database_should_throw_when_resultSerializer_is_null()
        {
            var databaseNamespace = new DatabaseNamespace("foo");
            List<BsonDocument> pipeline = null;
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(databaseNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("pipeline");
        }

        [Fact]
        public void constructor_with_collection_should_initialize_instance()
        {
            var collectionNamespace = new CollectionNamespace(new DatabaseNamespace("foo"), "bar");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();

            var subject = new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings);

            subject.BatchSize.Should().NotHaveValue();
            subject.Collation.Should().BeNull();
            subject.CollectionNamespace.Should().BeSameAs(collectionNamespace);
            subject.DatabaseNamespace.Should().BeNull();
            subject.FullDocument.Should().Be(ChangeStreamFullDocumentOption.Default);
            subject.MaxAwaitTime.Should().NotHaveValue();
            subject.MessageEncoderSettings.Should().BeSameAs(messageEncoderSettings);
            subject.Pipeline.Should().Equal(pipeline);
            subject.ReadConcern.Should().Be(ReadConcern.Default);
            subject.ResultSerializer.Should().BeSameAs(resultSerializer);
            subject.ResumeAfter.Should().BeNull();
            subject.StartAfter.Should().BeNull();
            subject.StartAtOperationTime.Should().BeNull();
        }

        [Fact]
        public void constructor_with_collection_should_throw_when_collectionNamespace_is_null()
        {
            CollectionNamespace collectionNamespace = null;
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("collectionNamespace");
        }

        [Fact]
        public void constructor_with_collection_should_throw_when_pipeline_is_null()
        {
            var collectionNamespace = new CollectionNamespace(new DatabaseNamespace("foo"), "bar");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            IBsonSerializer<BsonDocument> resultSerializer = null;
            var messageEncoderSettings = new MessageEncoderSettings();


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("resultSerializer");
        }

        [Fact]
        public void constructor_with_collection_should_throw_when_messageEncoderSettings_is_null()
        {
            var collectionNamespace = new CollectionNamespace(new DatabaseNamespace("foo"), "bar");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            MessageEncoderSettings messageEncoderSettings = null;


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("messageEncoderSettings");
        }

        [Fact]
        public void constructor_with_collection_should_throw_when_resultSerializer_is_null()
        {
            var collectionNamespace = new CollectionNamespace(new DatabaseNamespace("foo"), "bar");
            List<BsonDocument> pipeline = null;
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();


            var exception = Record.Exception(() => new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings));

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("pipeline");
        }

        [Theory]
        [ParameterAttributeData]
        public void BatchSize_get_and_set_should_work(
            [Values(null, 1, 2)] int? value)
        {
            var subject = CreateSubject();

            subject.BatchSize = value;
            var result = subject.BatchSize;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void Collation_get_and_set_should_work(
            [Values(null, "a", "b")] string locale)
        {
            var value = locale == null ? null : new Collation(locale);
            var subject = CreateSubject();

            subject.Collation = value;
            var result = subject.Collation;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void CollectionNamespace_get_should_work(
            [Values("a", "b")] string collectionName)
        {
            var value = new CollectionNamespace(new DatabaseNamespace("foo"), collectionName);
            var subject = CreateSubject(collectionNamespace: value);

            var result = subject.CollectionNamespace;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void FullDocument_get_and_set_should_work(
            [Values(ChangeStreamFullDocumentOption.Default, ChangeStreamFullDocumentOption.UpdateLookup)] ChangeStreamFullDocumentOption value)
        {
            var subject = CreateSubject();

            subject.FullDocument = value;
            var result = subject.FullDocument;

            result.Should().Be(value);
        }

        public static IEnumerable<object[]> GetEffectiveResumeStartValuesTestCases =>
            new List<object[]>
            {
                // the batch is empty
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(isEmpty: true, postBatchResumeToken: "{ c : 3 }"), "{ c : 3 }", null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(isEmpty: true, lastProcessedId: "{ d : 4 }"), "{ d : 4 }", null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(isEmpty: true), "{ a : 1 }", null),
                Case(new TestResumeOptions(resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(isEmpty: true), "{ b : 2 }", null),
                Case(new TestResumeOptions(), new TestCursorBatchInfo(isEmpty: true), null, null),

                // the batch has been iterated to the last document.
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.Completed, postBatchResumeToken: "{ c : 3 }"), "{ c : 3 }", null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.Completed, lastProcessedId: "{ d : 4 }"), "{ d : 4 }", null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.Completed), "{ a : 1 }", null),
                Case(new TestResumeOptions(resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.Completed), "{ b : 2 }", null),
                Case(new TestResumeOptions(), new TestCursorBatchInfo(iterationState:IterationState.Completed), null, null),

                // the batch has been iterated up to but not including the last element.
                Case(new TestResumeOptions(), new TestCursorBatchInfo(iterationState:IterationState.InProgress, lastProcessedId: "{ d : 4 }"), "{ d : 4 }", null),

                // The batch is not empty, but hasn't been iterated at all.
                Case(new TestResumeOptions(resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: false), "{ b : 2 }", null),
                Case(new TestResumeOptions(), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: false), null, null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: true, postBatchResumeToken:"{ c : 3 }"), "{ c : 3 }", null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: true, lastProcessedId: "{ d : 4 }"), "{ d : 4 }", null),
                Case(new TestResumeOptions(startAfter: "{ a : 1 }", resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: true), "{ a : 1 }", null),
                Case(new TestResumeOptions(resumeAfter: "{ b : 2 }"), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: true), "{ b : 2 }", null),
                Case(new TestResumeOptions(), new TestCursorBatchInfo(iterationState:IterationState.NotStarted, hasGetMoreBeenCalled: true), null, null),

                // operation time
                Case(new TestResumeOptions(startAtOperationTime: 3L), new TestCursorBatchInfo(), null, 3L),
                Case(new TestResumeOptions(initialOperationTime: 3L), new TestCursorBatchInfo(), null, 3L)
            };

        [Theory]
        [MemberData(nameof(GetEffectiveResumeStartValuesTestCases))]
        public void GetEffectiveResumeStartValues_should_have_expected_change_stream_operation_options_for_resume_process_after_resumable_error(
            TestResumeOptions resumeOptions,
            CursorBatchProcessingInfo batchInfo,
            string expectedResumeAfter,
            object expectedStartAtOperationTimeValue)
        {
            var pipeline = new BsonDocument[0];
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();

            ChangeStreamOperation<ChangeStreamDocument<BsonDocument>> subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(pipeline, resultSerializer, messageEncoderSettings, batchInfo)
            {
                ResumeAfter = resumeOptions.ResumeAfter,
                StartAfter = resumeOptions.StartAfter,
                StartAtOperationTime = resumeOptions.StartAtOperationTime
            };

            if (resumeOptions.InitialOperationTime != null)
            {
                subject._initialOperationTime(resumeOptions.InitialOperationTime);
            }

            var result = ChangeStreamResumeHelper.GetEffectiveResumeStartValues(subject, true);

            result.StartAfter.Should().BeNull();
            result.ResumeAfter.Should().Be(expectedResumeAfter != null ? BsonDocument.Parse(expectedResumeAfter) : null);
            result.StartAtOperationTime.Should().Be(expectedStartAtOperationTimeValue != null ? BsonTimestamp.Create(expectedStartAtOperationTimeValue) : null);
        }

        [Theory]
        [ParameterAttributeData]
        public void MaxAwaitTime_get_and_set_should_work(
            [Values(null, 1, 2)] int? maxAwaitTimeMS)
        {
            var value = maxAwaitTimeMS == null ? (TimeSpan?)null : TimeSpan.FromMilliseconds(maxAwaitTimeMS.Value);
            var subject = CreateSubject();

            subject.MaxAwaitTime = value;
            var result = subject.MaxAwaitTime;

            result.Should().Be(value);
        }

        [Fact]
        public void MessageEncoderSettings_get_should_work()
        {
            var value = new MessageEncoderSettings();
            var subject = CreateSubject(messageEncoderSettings: value);

            var result = subject.MessageEncoderSettings;

            result.Should().BeSameAs(value);
        }

        [Fact]
        public void Pipeline_get_should_work()
        {
            var value = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var subject = CreateSubject(pipeline: value);

            var result = subject.Pipeline;

            result.Should().Equal(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void ReadConcern_get_and_set_should_work(
            [Values(ReadConcernLevel.Local, ReadConcernLevel.Majority)] ReadConcernLevel? level)
        {
            var subject = CreateSubject();
            var value = new ReadConcern(level);

            subject.ReadConcern = value;
            var result = subject.ReadConcern;

            result.Should().Be(value);
        }


        [Fact]
        public void ReadConcern_set_should_throw_when_value_is_null()
        {
            var subject = CreateSubject();

            var exception = Record.Exception(() => subject.ReadConcern = null);

            var argumentNullException = exception.Should().BeOfType<ArgumentNullException>().Subject;
            argumentNullException.ParamName.Should().Be("value");
        }

        [Fact]
        public void ResultSerializer_get_should_work()
        {
            var value = new Mock<IBsonSerializer<BsonDocument>>().Object;
            var subject = CreateSubject(resultSerializer: value);

            var result = subject.ResultSerializer;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void ResumeAfter_get_and_set_should_work(
            [Values(null, "{ a : 1 }", "{ a : 2 }")] string valueString)
        {
            var subject = CreateSubject();
            var value = valueString == null ? null : BsonDocument.Parse(valueString);

            subject.ResumeAfter = value;
            var result = subject.ResumeAfter;

            result.Should().Be(value);
        }

        [SkippableTheory]
        [InlineData(null)]
        [InlineData("{ '_data' : 'testValue' }")]
        public void StartAfter_get_and_set_should_work(string startAfter)
        {
            var subject = CreateSubject();

            subject.StartAfter = startAfter != null ? BsonDocument.Parse(startAfter) : null;
            var result = subject.StartAfter;

            result.Should().Be(startAfter);
        }

        [Theory]
        [InlineData(null, null)]
        [InlineData(1, 2)]
        public void StartAtOperationTime_get_and_set_should_work(int? t, int? i)
        {
            var subject = CreateSubject();
            var value = t.HasValue ? new BsonTimestamp(t.Value, i.Value) : null;

            subject.StartAtOperationTime = value;
            var result = subject.StartAtOperationTime;

            result.Should().Be(value);
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_fill_batchProcessingInfo_according_to_iteration_steps_as_expected_after_resuming(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new BsonDocument[0];
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var initialChangeStreamOperation = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = 2
            };
            EnsureDatabaseExists();
            DropCollection();

            BsonDocument resumeToken = null;
            using (var cursor = ExecuteOperation(initialChangeStreamOperation, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Insert("{ _id : 1, x : 'x1' }");
                Insert("{ _id : 2, x : 'x2' }");
                Insert("{ _id : 3, x : 'x3' }");
                Insert("{ _id : 4, x : 'x4' }");
                Insert("{ _id : 5, x : 'x5' }");
                Insert("{ _id : 6, x : 'x6' }");

                initialChangeStreamOperation.BatchProcessingInfo.IterationState.Should().Be(IterationState.NotStarted);

                AssertChangeStreamIteration(enumerator, initialChangeStreamOperation, "{ _id : 1, x: 'x1' }", IterationState.InProgress, false);
                AssertChangeStreamIteration(enumerator, initialChangeStreamOperation, "{ _id : 2, x: 'x2' }", IterationState.Completed, false);
                AssertChangeStreamIteration(enumerator, initialChangeStreamOperation, "{ _id : 3, x: 'x3' }", IterationState.InProgress, true);

                resumeToken = enumerator.Current.ResumeToken;
            }

            var resumingChangeStreamOperation = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = 2,
                ResumeAfter = resumeToken
            };
            using (var cursor = ExecuteOperation(resumingChangeStreamOperation, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                resumingChangeStreamOperation.BatchProcessingInfo.IterationState.Should().Be(IterationState.NotStarted);
                AssertChangeStreamIteration(enumerator, initialChangeStreamOperation, "{ _id : 4, x: 'x4' }", IterationState.InProgress, true);
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_fill_batchProcessingInfo_according_to_iteration_steps_as_expected_after_resuming_from_initial_aggregate(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new BsonDocument[0];
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var initialChangeStreamOperation = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = 2
            };
            EnsureDatabaseExists();
            DropCollection();

            BsonDocument resumeToken = null;
            using (var cursor = ExecuteOperation(initialChangeStreamOperation, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Insert("{ _id : 1, x : 'x1' }");
                Insert("{ _id : 2, x : 'x2' }");
                Insert("{ _id : 3, x : 'x3' }");
                Insert("{ _id : 4, x : 'x4' }");

                initialChangeStreamOperation.BatchProcessingInfo.IterationState.Should().Be(IterationState.NotStarted);

                AssertChangeStreamIteration(enumerator, initialChangeStreamOperation, "{ _id : 1, x: 'x1' }", IterationState.InProgress, false);
                resumeToken = enumerator.Current.ResumeToken;
            }

            resumeToken.Should().NotBeNull();
            resumeToken.Should().Be(initialChangeStreamOperation.BatchProcessingInfo.LastIteratedDocumentId);

            var resumingChangeStreamOperation = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = 2,
                ResumeAfter = resumeToken
            };
            using (var cursor = ExecuteOperation(resumingChangeStreamOperation, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                resumingChangeStreamOperation.BatchProcessingInfo.IterationState.Should().Be(IterationState.NotStarted);
                AssertChangeStreamIteration(enumerator, resumingChangeStreamOperation, "{ _id : 2, x: 'x2' }", IterationState.InProgress, false);
                AssertChangeStreamIteration(enumerator, resumingChangeStreamOperation, "{ _id : 3, x: 'x3' }", IterationState.Completed, false);
                AssertChangeStreamIteration(enumerator, resumingChangeStreamOperation, "{ _id : 4, x: 'x4' }", IterationState.Completed, true);
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_fill_batchProcessingInfo_according_to_iteration_steps_as_expected_if_the_batch_iteration_is_over_a_server_batch(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new BsonDocument[0];
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = 2
            };
            EnsureDatabaseExists();
            DropCollection();

            using (var cursor = ExecuteOperation(subject, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                (cursor as INotifyBatchDocumentIterated)._iterateOverCachedBatch(false);
                Insert("{ _id : 1, x : 'x1' }");
                Insert("{ _id : 2, x : 'x2' }");
                Insert("{ _id : 3, x : 'x3' }");
                Insert("{ _id : 4, x : 'x4' }");
                Insert("{ _id : 5, x : 'x5' }");

                subject.BatchProcessingInfo.IterationState.Should().Be(IterationState.NotStarted);

                AssertChangeStreamIteration(enumerator, subject, "{ _id : 1, x: 'x1' }", IterationState.Completed, false);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 2, x: 'x2' }", IterationState.Completed, false);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 3, x: 'x3' }", IterationState.Completed, true);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 4, x: 'x4' }", IterationState.Completed, true);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 5, x: 'x5' }", IterationState.Completed, true);
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_fill_batchProcessingInfo_according_to_iteration_steps_as_expected_if_the_batch_iteration_is_over_cached_documents(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new BsonDocument[0];
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = 2
            };
            EnsureDatabaseExists();
            DropCollection();

            using (var cursor = ExecuteOperation(subject, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Insert("{ _id : 1, x : 'x1' }");
                Insert("{ _id : 2, x : 'x2' }");
                Insert("{ _id : 3, x : 'x3' }");
                Insert("{ _id : 4, x : 'x4' }");
                Insert("{ _id : 5, x : 'x5' }");

                subject.BatchProcessingInfo.IterationState.Should().Be(IterationState.NotStarted);

                AssertChangeStreamIteration(enumerator, subject, "{ _id : 1, x: 'x1' }", IterationState.InProgress, false);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 2, x: 'x2' }", IterationState.Completed, false);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 3, x: 'x3' }", IterationState.InProgress, true);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 4, x: 'x4' }", IterationState.Completed, true);
                AssertChangeStreamIteration(enumerator, subject, "{ _id : 5, x: 'x5' }", IterationState.Completed, true);
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_return_expected_results_for_drop_collection(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new[] { BsonDocument.Parse("{ $match : { operationType : \"invalidate\" } }") };
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings);
            EnsureDatabaseExists();
            DropCollection();

            using (var cursor = ExecuteOperation(subject, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Insert("{ _id : 1, x : 1 }");
                DropCollection();

                enumerator.MoveNext().Should().BeTrue();
                var change = enumerator.Current;
                change.OperationType.Should().Be(ChangeStreamOperationType.Invalidate);
                change.CollectionNamespace.Should().BeNull();
                change.DocumentKey.Should().BeNull();
                change.FullDocument.Should().BeNull();
                change.ResumeToken.Should().NotBeNull();
                change.UpdateDescription.Should().BeNull();
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_return_expected_results_for_deletes(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new[] { BsonDocument.Parse("{ $match : { operationType : \"delete\" } }") };
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings);
            EnsureDatabaseExists();
            DropCollection();

            using (var cursor = ExecuteOperation(subject, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Insert("{ _id : 1, x : 1 }");
                Delete("{ _id : 1 }");

                enumerator.MoveNext().Should().BeTrue();
                var change = enumerator.Current;
                change.OperationType.Should().Be(ChangeStreamOperationType.Delete);
                change.CollectionNamespace.Should().Be(_collectionNamespace);
                change.DocumentKey.Should().Be("{ _id : 1 }");
                change.FullDocument.Should().BeNull();
                change.ResumeToken.Should().NotBeNull();
                change.UpdateDescription.Should().BeNull();
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_return_expected_results_for_inserts(
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet, ClusterType.Sharded);
            var pipeline = new[] { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings);
            EnsureDatabaseExists();
            DropCollection();
            Insert("{ _id : 1, x : 1 }");

            using (var cursor = ExecuteOperation(subject, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Update("{ _id : 1 }", "{ $set : { x : 2  } }");
                Insert("{ _id : 2, x : 2 }");

                enumerator.MoveNext().Should().BeTrue();
                var change = enumerator.Current;
                change.OperationType.Should().Be(ChangeStreamOperationType.Insert);
                change.CollectionNamespace.Should().Be(_collectionNamespace);
                change.DocumentKey.Should().Be("{ _id : 2 }");
                change.FullDocument.Should().Be("{ _id : 2, x : 2 }");
                change.ResumeToken.Should().NotBeNull();
                change.UpdateDescription.Should().BeNull();
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_return_expected_results_for_large_batch(
            [Values(1, 2, 3)] int numberOfChunks,
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet, ClusterType.Sharded);
            EnsureDatabaseExists();
            DropCollection();

            var pipeline = new[] { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
            };
            using (var cursor = ExecuteOperation(subject, async))
            {
                var filler = new string('x', (numberOfChunks - 1) * 65536);
                var document = new BsonDocument { { "_id", 1 }, { "filler", filler } };
                Insert(document);

                ChangeStreamDocument<BsonDocument> changeStreamDocument;
                do
                {
                    if (async)
                    {
                        cursor.MoveNextAsync().GetAwaiter().GetResult();
                    }
                    else
                    {
                        cursor.MoveNext();
                    }

                    changeStreamDocument = cursor.Current.FirstOrDefault();
                }
                while (changeStreamDocument == null);

                changeStreamDocument.FullDocument.Should().Be(document);
            }
        }

        [SkippableTheory]
        [ParameterAttributeData]
        public void Execute_should_return_expected_results_for_updates(
            [Values(ChangeStreamFullDocumentOption.Default, ChangeStreamFullDocumentOption.UpdateLookup)] ChangeStreamFullDocumentOption fullDocument,
            [Values(false, true)] bool async)
        {
            RequireServer.Check().Supports(Feature.ChangeStreamStage).ClusterTypes(ClusterType.ReplicaSet);
            var pipeline = new[] { BsonDocument.Parse("{ $match : { operationType : \"update\" } }") };
            var resultSerializer = new ChangeStreamDocumentSerializer<BsonDocument>(BsonDocumentSerializer.Instance);
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<ChangeStreamDocument<BsonDocument>>(_collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                FullDocument = fullDocument
            };
            EnsureDatabaseExists();
            DropCollection();

            using (var cursor = ExecuteOperation(subject, async))
            using (var enumerator = new AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>>(cursor, CancellationToken.None))
            {
                Insert("{ _id : 1, x : 1 }");
                Update("{ _id : 1 }", "{ $set : { x : 2  } }");

                enumerator.MoveNext().Should().BeTrue();
                var change = enumerator.Current;
                change.OperationType.Should().Be(ChangeStreamOperationType.Update);
                change.CollectionNamespace.Should().Be(_collectionNamespace);
                change.DocumentKey.Should().Be("{ _id : 1 }");
                change.FullDocument.Should().Be(fullDocument == ChangeStreamFullDocumentOption.Default ? null : "{ _id : 1, x : 2 }");
                change.ResumeToken.Should().NotBeNull();
                change.UpdateDescription.RemovedFields.Should().BeEmpty();
                change.UpdateDescription.UpdatedFields.Should().Be("{ x : 2 }");
            }
        }

        [Theory]
        [ParameterAttributeData]
        public void Execute_should_throw_when_binding_does_not_implement_IReadBindingHandle(
            [Values(false, true)] bool async)
        {
            var subject = CreateSubject();
            var binding = new Mock<IReadBinding>().Object;

            Exception exception;
            if (async)
            {
                exception = Record.Exception(() => subject.ExecuteAsync(binding, CancellationToken.None).GetAwaiter().GetResult());
            }
            else
            {
                exception = Record.Exception(() => subject.Execute(binding, CancellationToken.None));
            }

            var argumentException = exception.Should().BeOfType<ArgumentException>().Subject;
            argumentException.ParamName.Should().Be("binding");
        }

        [Theory]
        [InlineData(null, null, ChangeStreamFullDocumentOption.Default, null, ReadConcernLevel.Local, null, null, "{ $changeStream : { fullDocument : \"default\" } }")]
        [InlineData(1, null, ChangeStreamFullDocumentOption.Default, null, ReadConcernLevel.Local, null, null, "{ $changeStream : { fullDocument : \"default\" } }")]
        [InlineData(null, "locale", ChangeStreamFullDocumentOption.Default, null, ReadConcernLevel.Local, null, null, "{ $changeStream : { fullDocument : \"default\" } }")]
        [InlineData(null, null, ChangeStreamFullDocumentOption.UpdateLookup, null, ReadConcernLevel.Local, null, null, "{ $changeStream : { fullDocument : \"updateLookup\" } }")]
        [InlineData(null, null, ChangeStreamFullDocumentOption.Default, 1, ReadConcernLevel.Local, null, null, "{ $changeStream : { fullDocument : \"default\" } }")]
        [InlineData(null, null, ChangeStreamFullDocumentOption.Default, null, ReadConcernLevel.Majority, null, null, "{ $changeStream : { fullDocument : \"default\" } }")]
        [InlineData(null, null, ChangeStreamFullDocumentOption.Default, null, ReadConcernLevel.Local, "{ a : 1 }", null, "{ $changeStream: { fullDocument: \"default\", resumeAfter : { a : 1 } } }")]
        [InlineData(1, "locale", ChangeStreamFullDocumentOption.UpdateLookup, 2, ReadConcernLevel.Majority, "{ a : 1 }", null, "{ $changeStream: { fullDocument: \"updateLookup\", resumeAfter : { a : 1 } } }")]
        [InlineData(null, null, ChangeStreamFullDocumentOption.Default, null, ReadConcernLevel.Local, "{ a : 1 }", "{ b : 2 }", "{ $changeStream: { fullDocument: \"default\", startAfter : { b : 2 }, resumeAfter : { a : 1 } } }")]
        [InlineData(1, "locale", ChangeStreamFullDocumentOption.UpdateLookup, 2, ReadConcernLevel.Majority, "{ a : 1 }", "{ b : 2 }", "{ $changeStream: { fullDocument: \"updateLookup\", startAfter : { b : 2 }, resumeAfter : { a : 1 } } }")]
        public void CreateAggregateOperation_should_return_expected_result(
            int? batchSize,
            string locale,
            ChangeStreamFullDocumentOption fullDocument,
            int? maxAwaitTimeMS,
            ReadConcernLevel level,
            string resumeAferJson,
            string startAfterJson,
            string expectedChangeStreamStageJson)
        {
            var collation = locale == null ? null : new Collation(locale);
            var maxAwaitTime = maxAwaitTimeMS == null ? (TimeSpan?)null : TimeSpan.FromMilliseconds(maxAwaitTimeMS.Value);
            var readConcern = new ReadConcern(level);
            var resumeAfter = resumeAferJson == null ? null : BsonDocument.Parse(resumeAferJson);
            var startAfter = startAfterJson == null ? null : BsonDocument.Parse(startAfterJson);
            var expectedChangeStreamStage = BsonDocument.Parse(expectedChangeStreamStageJson);
            var collectionNamespace = new CollectionNamespace(new DatabaseNamespace("foo"), "bar");
            var pipeline = new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            var resultSerializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();
            var subject = new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings)
            {
                BatchSize = batchSize,
                Collation = collation,
                FullDocument = fullDocument,
                MaxAwaitTime = maxAwaitTime,
                ReadConcern = readConcern,
                ResumeAfter = resumeAfter,
                StartAfter = startAfter
            };
            var expectedPipeline = new BsonDocument[]
            {
                expectedChangeStreamStage,
                pipeline[0]
            };

            var result = subject.CreateAggregateOperation(resuming: false);

            result.AllowDiskUse.Should().NotHaveValue();
            result.BatchSize.Should().Be(batchSize);
            result.Collation.Should().Be(collation);
            result.CollectionNamespace.Should().Be(collectionNamespace);
            result.MaxAwaitTime.Should().Be(maxAwaitTime);
            result.MaxTime.Should().NotHaveValue();
            result.MessageEncoderSettings.Should().BeSameAs(messageEncoderSettings);
            result.Pipeline.Should().Equal(expectedPipeline);
            result.ReadConcern.Should().Be(readConcern);
            result.ResultSerializer.Should().Be(RawBsonDocumentSerializer.Instance);
        }

        // private methods
        private void AssertChangeStreamIteration(AsyncCursorEnumerator<ChangeStreamDocument<BsonDocument>> enumerator, ChangeStreamOperation<ChangeStreamDocument<BsonDocument>> changeStreamOperation, string expectedDocument, IterationState expectedIterationState, bool expectedHasGetMoreBeenCalled)
        {
            enumerator.MoveNext().Should().BeTrue();
            var value = enumerator.Current;
            value.FullDocument.Should().Be(expectedDocument);

            if (Feature.ChangeStreamPostBatchResumeToken.IsSupported(CoreTestConfiguration.ServerVersion))
            {
                changeStreamOperation.BatchProcessingInfo.PostBatchResumeToken.Should().NotBeNull();
            }

            changeStreamOperation.BatchProcessingInfo.IsEmpty.Should().BeFalse();
            changeStreamOperation.BatchProcessingInfo.HasGetMoreBeenCalled.Should().Be(expectedHasGetMoreBeenCalled);
            changeStreamOperation.BatchProcessingInfo.IterationState.Should().Be(expectedIterationState);
        }

        private static object[] Case(params object[] @params)
        {
            return @params;
        }

        private ChangeStreamOperation<BsonDocument> CreateSubject(
            CollectionNamespace collectionNamespace = null,
            List<BsonDocument> pipeline = null,
            IBsonSerializer<BsonDocument> resultSerializer = null,
            MessageEncoderSettings messageEncoderSettings = null)
        {
            collectionNamespace = collectionNamespace ?? new CollectionNamespace(new DatabaseNamespace("foo"), "bar");
            pipeline = pipeline ?? new List<BsonDocument> { BsonDocument.Parse("{ $match : { operationType : \"insert\" } }") };
            resultSerializer = resultSerializer ?? BsonDocumentSerializer.Instance;
            messageEncoderSettings = messageEncoderSettings ?? new MessageEncoderSettings();
            return new ChangeStreamOperation<BsonDocument>(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings);
        }

        // nested types
        public class TestResumeOptions
        {
            public TestResumeOptions(string startAfter = null, string resumeAfter = null, long? startAtOperationTime = null, long? initialOperationTime = null)
            {
                InitialOperationTime = initialOperationTime.HasValue ? BsonTimestamp.Create(initialOperationTime) : null;
                ResumeAfter = resumeAfter != null ? BsonDocument.Parse(resumeAfter) : null;
                StartAfter = startAfter != null ? BsonDocument.Parse(startAfter) : null;
                StartAtOperationTime = startAtOperationTime.HasValue ? BsonTimestamp.Create(startAtOperationTime) : null;
            }

            public BsonTimestamp InitialOperationTime { get; set; }
            public BsonDocument ResumeAfter { get; set; }
            public BsonDocument StartAfter { get; set; }
            public BsonTimestamp StartAtOperationTime { get; set; }
        }

        public class TestCursorBatchInfo : CursorBatchProcessingInfo
        {
            public TestCursorBatchInfo(bool hasGetMoreBeenCalled = false, bool isEmpty = false, IterationState iterationState = IterationState.NotStarted, string lastProcessedId = null, string postBatchResumeToken = null)
            {
                HasGetMoreBeenCalled = hasGetMoreBeenCalled;
                IsEmpty = isEmpty;
                IterationState = iterationState;
                LastIteratedDocumentId = lastProcessedId != null ? BsonDocument.Parse(lastProcessedId) : null;
                PostBatchResumeToken = postBatchResumeToken != null ? BsonDocument.Parse(postBatchResumeToken) : null;
            }
        }
    }

    internal static class ChangeStreamOperationReflector
    {
        public static AggregateOperation<RawBsonDocument> CreateAggregateOperation(this ChangeStreamOperation<BsonDocument> subject, bool resuming)
        {
            return (AggregateOperation<RawBsonDocument>)Reflector.Invoke(subject, nameof(CreateAggregateOperation), resuming);
        }

        public static BsonDocument CreateChangeStreamStage(this ChangeStreamOperation<ChangeStreamDocument<BsonDocument>> subject, bool resuming)
        {
            return (BsonDocument)Reflector.Invoke(subject, nameof(CreateChangeStreamStage), resuming);
        }

        public static void _initialOperationTime(this ChangeStreamOperation<ChangeStreamDocument<BsonDocument>> subject, BsonTimestamp value)
        {
            Reflector.SetFieldValue(subject, nameof(_initialOperationTime), value);
        }
    }

    internal static class INotifyBatchDocumentIteratedReflector
    {
        public static void _iterateOverCachedBatch(this INotifyBatchDocumentIterated subject, bool value)
        {
            Reflector.SetFieldValue(subject, nameof(_iterateOverCachedBatch), value);
        }
    }
}