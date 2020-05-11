/* Copyright 2020-present MongoDB Inc.
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

using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using MongoDB.Bson.Serialization;
using Xunit;

namespace MongoDB.Bson.Tests.Jira
{
    public class help15599
    {
        [Fact]
        public void Serialization_should_work_as_expected()
        {
            var arg1 = new InnerList(new[] { "item1", "item2" });
            var testCase = new MainClass(arg1);

            var json = testCase.ToJson();
            var result = BsonSerializer.Deserialize<MainClass>(json);

            result.InnerList.Items[0].Should().Be("item1");
            result.InnerList.Items[1].Should().Be("item2");
        }
    }

    public class MainClass
    {
        public MainClass(InnerList innerList)
        {
            InnerList = innerList;
        }

        public InnerList InnerList { get; private set; }
    }

    public class InnerList : ListValueObject<string>
    {
        // The first way to make the test pass (it's also what the user does):

        // Change the below line on:
        //
        //      public InnerList(IEnumerable<string> items1) : base(items1) { }
        //
        // It helps because "items1" is not equal to the property name in ListValueObject (which is 'Items'),
        // so in this line we won't add a new creatorMap for the InnerList: https://github.com/mongodb/mongo-csharp-driver/blob/2a716e19b631592c31ab4d55e4d03366c65d6be0/src/MongoDB.Bson/Serialization/Conventions/ImmutableTypeClassMapConvention.cs#L75
        // As a result, we will create this type dynamically here: https://github.com/mongodb/mongo-csharp-driver/blob/2a716e19b631592c31ab4d55e4d03366c65d6be0/src/MongoDB.Bson/Serialization/BsonClassMap.cs#L1279-L1303
        // and here: https://github.com/mongodb/mongo-csharp-driver/blob/2a716e19b631592c31ab4d55e4d03366c65d6be0/src/MongoDB.Bson/Serialization/Serializers/BsonClassMapSerializer.cs#L167-L184
        public InnerList(IEnumerable<string> items) : base(items) { }
    }

    public abstract class ListValueObject<T> 
    {
        // The second way to make the test pass (the user rejected this way):

        // Change the below line on:
        //
        //      public IEnumerable<T> Items { get; private set; }
        //
        // I described this case here: https://jira.mongodb.org/browse/HELP-15599?focusedCommentId=3079312&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-3079312
        // In two words, the property type and the constuctor argument type should be equal.
        // As a result, we will save the creatorMap in `ImmutableTypeClassMapConvention` and then
        // we will be able to use different code path in BsonClassMapSerializer: https://github.com/mongodb/mongo-csharp-driver/blob/2a716e19b631592c31ab4d55e4d03366c65d6be0/src/MongoDB.Bson/Serialization/Serializers/BsonClassMapSerializer.cs#L162-L165
        public IReadOnlyList<T> Items { get; private set; }

        protected ListValueObject(IEnumerable<T> items1)
        {
            Items = items1.ToList().AsReadOnly();
        }
    }
}
