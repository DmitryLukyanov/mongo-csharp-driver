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

        private readonly MongoClientSettings _settings;

        public GssapiAuthenticationTests()
        {
            _settings = MongoClientSettings.FromUrl(new MongoUrl(CoreTestConfiguration.ConnectionString.ToString()));
        }

        [SkippableFact]
        public void Authentication_with_canonicalize_host_name_should_work_as_expected()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");

            var authHost = Environment.GetEnvironmentVariable("AUTH_HOST") ?? throw new Exception("AUTH_HOST has not been configured.");
            var authGssapi = Environment.GetEnvironmentVariable("AUTH_GSSAPI") ?? throw new Exception("AUTH_GSSAPI has not been configured.");
            var hostEntry = Dns.GetHostEntry(authHost);
            var ipAddress = hostEntry.AddressList.First();
            var connectionString = $"mongodb://{authGssapi}@{ipAddress}/kerberos?authMechanism=GSSAPI&authMechanismProperties=CANONICALIZE_HOST_NAME:true";

            var client = new MongoClient(connectionString);
            var db = client.GetDatabase("db");
            db.RunCommand<BsonDocument>(new BsonDocument("ping", 1));
        }

        [SkippableFact]
        public void TestNoCredentials()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");
            _settings.Credential = null;
            var client = new MongoClient(_settings);

            Assert.Throws<MongoCommandException>(() =>
            {
#pragma warning disable 618
                client
                    .GetDatabase(DriverTestConfiguration.DatabaseNamespace.DatabaseName)
                    .GetCollection<BsonDocument>(__collectionName)
                    .Count(new BsonDocument());
#pragma warning restore
            });
        }


        [SkippableFact]
        public void TestSuccessfulAuthentication()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");
            var client = new MongoClient(_settings);

            var result = client
                .GetDatabase(DriverTestConfiguration.DatabaseNamespace.DatabaseName)
                .GetCollection<BsonDocument>(__collectionName)
                .FindSync(new BsonDocument())
                .ToList();

            Assert.NotNull(result);
        }

        [SkippableFact]
        public void TestBadPassword()
        {
            RequireEnvironment.Check().EnvironmentVariable("EXPLICIT");
            var currentCredentialUsername = _settings.Credential.Username;
            _settings.Credential = MongoCredential.CreateGssapiCredential(currentCredentialUsername, "wrongPassword");

            var client = new MongoClient(_settings);

            Assert.Throws<MongoAuthenticationException>(() =>
            {
                client
                    .GetDatabase(DriverTestConfiguration.DatabaseNamespace.DatabaseName)
                    .GetCollection<BsonDocument>(__collectionName)
                    .FindSync(new BsonDocument())
                    .ToList();
            });
        }
    }
}