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
using System.Linq;
using System.Net;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using Xunit;

namespace MongoDB.Driver.Tests.Communication.Security
{
    [Trait("Category", "Authentication")]
    [Trait("Category", "GssapiMechanism")]
    public class GssapiAuthenticationTests
    {
        private static readonly string __collectionName = "test";

        public string AuthHost => Environment.GetEnvironmentVariable("AUTH_HOST") ?? throw new Exception("AUTH_HOST has not been configured.");

        [SkippableFact]
        public void Authentication_with_canonicalize_host_name_and_ip_host_should_work_as_expected()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");

            var hostEntry = Dns.GetHostEntry(AuthHost);
            var ipAddress = hostEntry.AddressList.First().ToString();
            var connectionString = CreateGssapiConnectionString(ipAddress, "&authMechanismProperties=CANONICALIZE_HOST_NAME:true");
            var mongoUrl = new MongoUrl(connectionString);

            var client = new MongoClient(mongoUrl);
            var collection = GetTestCollection(client, mongoUrl.DatabaseName);
            var result = collection
                .FindSync(new BsonDocument())
                .ToList();

            result.Should().NotBeNull();
        }

        [SkippableFact]
        public void TestNoCredentials()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");

            var mongoUrl = CreateMongoUrl();
            var clientSettings = MongoClientSettings.FromUrl(mongoUrl);
            clientSettings.Credential = null;
            var client = new MongoClient(clientSettings);
            var collection = GetTestCollection(client, mongoUrl.DatabaseName);

#pragma warning disable 618
            var exception = Record.Exception(() => { collection.Count(new BsonDocument()); });
#pragma warning restore
            var e = exception.Should().BeOfType<MongoCommandException>().Subject;
            e.CodeName.Should().Be("Unauthorized");
        }


        [SkippableFact]
        public void TestSuccessfulAuthentication()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");

            var mongoUrl = CreateMongoUrl();
            var client = new MongoClient(mongoUrl);

            var collection = GetTestCollection(client, mongoUrl.DatabaseName);
            var result = collection
                .FindSync(new BsonDocument())
                .ToList();

            result.Should().NotBeNull();
        }

        [SkippableFact]
        public void TestBadPassword()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");

            var mongoUrl = CreateMongoUrl();
            var currentCredentialUsername = mongoUrl.Username;
            var clientSettings = MongoClientSettings.FromUrl(mongoUrl);
            clientSettings.Credential = MongoCredential.CreateGssapiCredential(currentCredentialUsername, "wrongPassword");

            var client = new MongoClient(clientSettings);
            var collection = GetTestCollection(client, mongoUrl.DatabaseName);

            var exception = Record.Exception(() => { collection.FindSync(new BsonDocument()).ToList(); });
            var e = exception.Should().BeOfType<MongoAuthenticationException>().Subject;
            e.InnerException.Message.Should().Be("The logon failed.");
        }

        // private methods
        private string CreateGssapiConnectionString(string authHost, string mechanismProperty = null)
        {
            var authGssapi = Environment.GetEnvironmentVariable("AUTH_GSSAPI") ?? throw new Exception("AUTH_GSSAPI has not been configured.");

            return $"mongodb://{authGssapi}@{authHost}/kerberos?authMechanism=GSSAPI{mechanismProperty}";
        }

        private MongoUrl CreateMongoUrl()
        {
            var connectionString = CreateGssapiConnectionString(AuthHost);
            return MongoUrl.Create(connectionString);
        }

        private IMongoCollection<BsonDocument> GetTestCollection(MongoClient client, string databaseName)
        {
            return client
                .GetDatabase(databaseName)
                .GetCollection<BsonDocument>(__collectionName);
        }
    }
}