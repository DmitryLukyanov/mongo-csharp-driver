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

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using FluentAssertions;
using MongoDB.Bson.Serialization;
using Xunit;

namespace MongoDB.Driver.Tests.Jira
{
    public class CSharp2816Tests
    {
        public interface IAnimal
        {
            int[] ArrayProperty { get; set; }
            bool BoolProperty { get; set; }
            List<int> CollectionProperty { get; set; }
            TestEnum EnumProperty { get; set; }
            int IntProperty { get; set; }
            NestedData NestedDataProperty { get; set; }
            INestedData NestedInterfaceProperty { get; set; }
            string StringProperty { get; set; }
            TestStruct StructProperty { get; set; }
        }

        public class Dog : IAnimal
        {
            public int[] ArrayProperty { get; set; }
            public bool BoolProperty { get; set; }
            public List<int> CollectionProperty { get; set; }
            public TestEnum EnumProperty { get; set; }
            public int IntProperty { get; set; }
            public NestedData NestedDataProperty { get; set; }
            public INestedData NestedInterfaceProperty { get; set; }
            public string StringProperty { get; set; }
            public TestStruct StructProperty { get; set; }
        }

        public interface INestedData
        {
            string Data { get; set; }
        }

        public class NestedData : INestedData
        {
            public string Data { get; set; }
        }

        public enum TestEnum
        {
            Data
        }

        public struct TestStruct
        {
            public TestStruct(string data)
            {
                Data = data;
            }

            public string Data { get; set; }
        }

        [Fact]
        public void Render_query_with_array()
        {
            Assert(x => x.ArrayProperty != null, "{ 'ArrayProperty' : { $ne : null } }");
        }

        [Fact]
        public void Render_query_with_bool()
        {
            Assert(x => x.BoolProperty, "{ 'BoolProperty' : true }");
        }

        [Fact]
        public void Render_query_with_collection()
        {
            Assert(x => x.CollectionProperty != null, "{ 'CollectionProperty' : { $ne : null } }");
        }

        [Fact]
        public void Render_query_with_enum()
        {
            Assert(x => x.EnumProperty == TestEnum.Data, "{ 'EnumProperty' : 0 }");
        }

        [Fact]
        public void Render_query_with_int()
        {
            Assert(x => x.IntProperty == 1, "{ 'IntProperty' : 1 }");
        }

        [Fact]
        public void Render_query_with_nested_class()
        {
            Assert(x => x.NestedDataProperty != null, "{ 'NestedDataProperty' : { $ne : null } }");
        }

        [Fact]
        public void Render_query_with_nested_interface()
        {
            Assert(x => x.NestedInterfaceProperty != null, "{ 'NestedInterfaceProperty' : { $ne : null } }");
        }

        [Fact]
        public void Render_query_with_property_from_nested_class()
        {
            Assert(x => x.NestedDataProperty.Data == "data", "{ 'NestedDataProperty.Data' : 'data' }");
        }

        [Fact]
        public void Render_query_with_property_from_nested_interface()
        {
            Assert(x => x.NestedInterfaceProperty.Data == "data", "{ 'NestedInterfaceProperty.Data' : 'data' }");
        }

        [Fact]
        public void Render_query_with_string()
        {
            Assert(x => x.StringProperty == "dog", "{ 'StringProperty' : 'dog' }");
        }

        [Fact]
        public void Render_query_with_struct()
        {
            Assert(x => x.StructProperty.Equals(new TestStruct("data")), "{ 'StructProperty' : { 'Data' : 'data' } }");
        }

        // private methods
        private void Assert(Expression<Func<IAnimal, bool>> expression, string expectedQuery)
        {
            var filter = Builders<IAnimal>.Filter.Where(expression);
            var registry = BsonSerializer.SerializerRegistry;
            var iAnimalSerializer = registry.GetSerializer<IAnimal>();
            var rendered = filter.Render(iAnimalSerializer, registry);
            rendered.Should().Be(expectedQuery);
        }
    }
}
