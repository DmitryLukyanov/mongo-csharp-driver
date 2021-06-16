/* Copyright 2021-present MongoDB Inc.
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

using System.Reflection;
using System.Threading;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.TestHelpers;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Operations;
using Xunit;

namespace MongoDB.Driver.Tests
{
    public class LoadBalancedTests
    {
        //[Fact]
        //public void Test()
        //{
        //    using (var client = DriverTestConfiguration.CreateDisposableClient(settings => settings.LoadBalanced = true))
        //    {
        //        //client.DropDatabase("db");
        //        var database = client.GetDatabase("db");
        //        var collection = database.GetCollection<BsonDocument>("coll");
        //        //collection.InsertOne(new BsonDocument());
        //        //collection.InsertOne(new BsonDocument());
        //        using (var session = client.StartSession())
        //        {
        //            var cursor = collection.FindSync(session, "{}", options: new FindOptions<BsonDocument, BsonDocument>() { BatchSize = 1 });
        //            var asyncCursor = (AsyncCursor<BsonDocument>)cursor;
        //            asyncCursor.MoveNext(CancellationToken.None);
        //            asyncCursor.MoveNext(CancellationToken.None);
        //            //session.WrappedCoreSession.Pinned.Connection.Should().NotBeNull();
        //            asyncCursor.Close();
        //        }
        //    }
        //}

        [Fact]
        public void RetryableReadTest()
        {
            var collectionName = CollectionNamespace.FromFullName("db.coll");
            var client = DriverTestConfiguration.Client;
            client.DropDatabase(collectionName.DatabaseNamespace.DatabaseName);
            var database = client.GetDatabase(collectionName.DatabaseNamespace.DatabaseName);
            var coll = database.GetCollection<BsonDocument>(collectionName.CollectionName);
            coll.InsertOne(new BsonDocument());
            coll.InsertOne(new BsonDocument());

            var cluster = CoreTestConfiguration.Cluster;
            var readPreference = ReadPreference.Primary;

            var cancellationToken = CancellationToken.None;

            IAsyncCursor<BsonDocument> asyncCursor = null;

            using (var coreSession = cluster.StartSession())
            {
                GetCoreSessionReferenceCount(coreSession).Should().Be(1);
                using (var binding = new ReadBindingHandle(new ReadPreferenceBinding(cluster, readPreference, coreSession.Fork())))
                {
                    GetCoreSessionReferenceCount(binding.Session).Should().Be(2);
                    using (var context = RetryableReadContext.Create(binding, false, cancellationToken))
                    {
                        context.ChannelSource._reference_ReferenceCount().Should().Be(1); // one channel source is created
                        context.Channel._connection_reference_ReferenceCount().Should().Be(1); // one channel is created

                        context.PinConnectionIfRequired(cancellationToken);

                        //context.Channel._connection_reference_ReferenceCount().Should().Be(1); // still one active channel
                        context.ChannelSource._reference_ReferenceCount().Should().Be(1); // still one active channel source

                        var findOperation = new FindCommandOperation<BsonDocument>(
                            CollectionNamespace.FromFullName("db.coll"),
                            BsonDocumentSerializer.Instance,
                            new Core.WireProtocol.Messages.Encoders.MessageEncoderSettings())
                        {
                            BatchSize = 1
                        };

                        asyncCursor = findOperation.Execute(context, cancellationToken);

                        //context.Channel._connection_reference_ReferenceCount().Should().Be(2); // original one + cursor one
                        context.ChannelSource._reference_ReferenceCount().Should().Be(1); // still one active channel source
                    }
                }
            }

            int GetCoreSessionReferenceCount(ICoreSessionHandle coreSession) => ((ReferenceCountedCoreSession)((CoreSessionHandle)coreSession).Wrapped)._referenceCount();
        }
    }

    internal static class LoadBalancedReflector
    {
        public static int _connection_reference_ReferenceCount(this IChannelHandle channelHandle) // can be taken from private types
        {
            var connection = Reflector.GetFieldValue(channelHandle, "_connection");
            var reference = Reflector.GetFieldValue(connection, "_reference");
            return (int)Reflector.GetPropertyValue(reference, "ReferenceCount", BindingFlags.Public | BindingFlags.Instance);
        }

        public static int _referenceCount(this ReferenceCountedCoreSession referenceCountedCoreSession)
        {
            return (int)Reflector.GetFieldValue(referenceCountedCoreSession, nameof(_referenceCount));
        }

        public static int _reference_ReferenceCount(this IChannelSourceHandle channelSourceHandle)
        {
            var reference = Reflector.GetFieldValue(channelSourceHandle, "_reference");
            return (int)Reflector.GetPropertyValue(reference, "ReferenceCount", BindingFlags.Public | BindingFlags.Instance);
        }
    }
}
