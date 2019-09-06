﻿/* Copyright 2019-present MongoDB Inc.
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
using MongoDB.Bson.TestHelpers;
using MongoDB.Driver.Encryption;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Xunit;

namespace MongoDB.Driver.Tests
{
    public class MongocryptdHelperTests
    {
        [Theory]
        [InlineData("mongocryptdURI", "mongodb://localhost:11111", "mongodb://localhost:11111")]
        [InlineData("mongocryptdURI1", "mongodb://localhost:27020", "mongodb://localhost:27020")]
        [InlineData(null, null, "mongodb://localhost:27020")]
        public void CreateMongocryptdConnectionStringTest(string optionKey, string optionValue, string expectedConnectionString)
        {
            var subject = new MongocryptdHelper();
            var extraOptions = new Dictionary<string, object>();
            if (optionKey != null)
            {
                extraOptions.Add(optionKey, optionValue);
            };
            subject._extraOptions(extraOptions);
            var connectionString = subject.CreateMongocryptdConnectionString();
            connectionString.Should().Be(expectedConnectionString);
        }

        [SkippableTheory]
        [InlineData("{ mongocryptdBypassSpawn : true }", null, null, false)]
        [InlineData(null, "mongocryptd.exe", "--idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdBypassSpawn : false }", "mongocryptd.exe", "--idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdBypassSpawn : false }", "mongocryptd.exe", "--idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdBypassSpawn : false, mongocryptdSpawnPath : 'c:/' }", "c:/mongocryptd.exe", "--idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdBypassSpawn : false, mongocryptdSpawnPath : 'c:/mgcr.exe' }", "c:/mgcr.exe", "--idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdBypassSpawn : false, mongocryptdSpawnPath : 'c:/mgcr.exe' }", "c:/mgcr.exe", "--idleShutdownTimeoutSecs 60", true)]
        // args string
        [InlineData("{ mongocryptdSpawnArgs : '--arg1 A --arg2 B' }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdSpawnArgs : '--arg1 A --arg2 B --idleShutdownTimeoutSecs 50' }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]
        // args dictionary
        [InlineData("{ mongocryptdSpawnArgs : { arg1 : 'A', arg2 : 'B' } }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdSpawnArgs : { arg1 : 'A', arg2 : 'B', idleShutdownTimeoutSecs : 50 } }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]
        [InlineData("{ mongocryptdSpawnArgs : { arg1 : 'A', arg2 : 'B', idleShutdownTimeoutSecs : 50 } }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]
        [InlineData("{ mongocryptdSpawnArgs : { '--arg1' : 'A', '--arg2' : 'B', '--idleShutdownTimeoutSecs' : 50 } }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]
        // args list
        [InlineData("{ mongocryptdSpawnArgs : [ 'arg1 A', 'arg2 B'] }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 60", true)]
        [InlineData("{ mongocryptdSpawnArgs : [ 'arg1 A', 'arg2 B', 'idleShutdownTimeoutSecs 50'] }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]
        [InlineData("{ mongocryptdSpawnArgs : [ '--arg1 A', '--arg2 B', '--idleShutdownTimeoutSecs 50'] }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]

        [InlineData("{ mongocryptdBypassSpawn : false, mongocryptdSpawnArgs : [ '--arg1 A', '--arg2 B', '--idleShutdownTimeoutSecs 50'] }", "mongocryptd.exe", "--arg1 A --arg2 B --idleShutdownTimeoutSecs 50", true)]
        public void Mongocryptd_should_be_spawned_with_correct_extra_arguments(
            string stringExtraOptions,
            string expectedPath,
            string expectedArgs,
            bool shouldBeSpawned)
        {
            var bsonDocumentExtraOptions =
                stringExtraOptions != null
                 ? BsonDocument.Parse(stringExtraOptions)
                 : new BsonDocument();

            object CreateTypedExtraOptions(BsonValue value)
            {
                if (value.IsBsonArray)
                {
                    return value.AsBsonArray.Select(c => c.ToString()); // IEnumerable
                }
                else if (value.IsBsonDocument)
                {
                    return value.ToBsonDocument().Elements.ToDictionary(k => k.Name, v => v.Value.ToString()); // Dictionary
                }
                else
                {
                    return value.ToString(); // string
                }
            }

            var extraOptions = bsonDocumentExtraOptions
                .Elements
                .ToDictionary(k => k.Name, v => CreateTypedExtraOptions(v.Value));

            var subject = new MongocryptdHelper();
            subject._extraOptions(new ReadOnlyDictionary<string, object>(extraOptions));

            var result = subject.ShouldMongocryptdBeSpawned(out var path, out var args);
            result.Should().Be(shouldBeSpawned);
            path.Should().Be(expectedPath);
            args.Should().Be(expectedArgs);
        }
    }
    internal static class MongocryptdHelperReflector
    {
        public static string CreateMongocryptdConnectionString(this MongocryptdHelper mongocryptdHelper)
        {
            return (string)Reflector.Invoke(mongocryptdHelper, nameof(CreateMongocryptdConnectionString));
        }

        public static void _extraOptions(this MongocryptdHelper mongocryptdHelper, IReadOnlyDictionary<string, object> extraOptions)
        {
            Reflector.SetFieldValue(mongocryptdHelper, nameof(_extraOptions), extraOptions);
        }

        public static bool ShouldMongocryptdBeSpawned(this MongocryptdHelper mongocryptdHelper, out string path, out string args)
        {
            return (bool)Reflector.Invoke(mongocryptdHelper, nameof(ShouldMongocryptdBeSpawned), out path, out args);
        }
    }
}
