/* Copyright 2020–present MongoDB Inc.
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
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Authentication
{
    /// <summary>
    /// The Mongo AWS authenticator.
    /// </summary>
    public class MongoAWSAuthenticator : SaslAuthenticator
    {
        // private constants
        private const int randomLength = 32;

        // static properties
        /// <summary>
        /// Gets the name of the mechanism.
        /// </summary>
        /// <value>
        /// The name of the mechanism.
        /// </value>
        public static string MechanismName
        {
            get { return "MONGODB-AWS"; }
        }

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="GssapiAuthenticator"/> class.
        /// </summary>
        /// <param name="credential">The credentials.</param>
        /// <param name="properties">The properties.</param>
        public MongoAWSAuthenticator(UsernamePasswordCredential credential, IEnumerable<KeyValuePair<string, string>> properties)
            : base(CreateMechanism(credential, properties, new DefaultRandomByteGenerator()))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GssapiAuthenticator"/> class.
        /// </summary>
        /// <param name="username">The username.</param>
        /// <param name="properties">The properties.</param>
        public MongoAWSAuthenticator(string username, IEnumerable<KeyValuePair<string, string>> properties)
            : base(CreateMechanism(username, null, properties, new DefaultRandomByteGenerator()))
        {
        }

        /// <inheritdoc/>
        public override string DatabaseName
        {
            get { return "$external"; }
        }

        private static MongoAWSMechanism CreateMechanism(
            UsernamePasswordCredential credential,
            IEnumerable<KeyValuePair<string, string>> properties,
            IRandomByteGenerator randomByteGenerator)
        {
            if (credential.Source != "$external")
            {
                throw new ArgumentException("MONGO AWS authentication may only use the $external source.", "credential");
            }

            return CreateMechanism(credential.Username, credential.Password, properties, randomByteGenerator);
        }

        private static MongoAWSMechanism CreateMechanism(
            string username,
            SecureString securePassword,
            IEnumerable<KeyValuePair<string, string>> properties,
            IRandomByteGenerator randomByteGenerator)
        {
            var awsCredentialsCreators = new Func<AwsCredentials>[]
            {
                () => CreateAwsCredentialsFromMongoCredentials(username, securePassword, properties),
                () => CreateAwsCredentialsFromEnvironmentVariables(),
                () => CreateAwsCredentialsFromEcsResponse(),
                () => CreateAwsCredentialsFromEc2Response()
            };

            foreach (var awsCredentialsCreator in awsCredentialsCreators)
            {
                var awsCredentials = awsCredentialsCreator();
                if (awsCredentials.UserName == null)
                {
                    continue;
                }
                ValidateCredentials(awsCredentials);
                if (awsCredentials != null)
                {
                    var credentials = new UsernamePasswordCredential("$external", awsCredentials.UserName, awsCredentials.Password);
                    return new MongoAWSMechanism(credentials, awsCredentials.SessionToken, randomByteGenerator);
                }
            }

            throw new ArgumentException("A MONGODB-AWS must have access key ID.");
        }

        private static AwsCredentials CreateAwsCredentialsFromMongoCredentials(
            string username,
            SecureString securePassword,
            IEnumerable<KeyValuePair<string, string>> properties)
        {
            var sessionToken = ExtractSessionTokenFromMechanismProperties(properties);
            return new AwsCredentials()
            {
                UserName = username,
                Password = securePassword,
                SessionToken = sessionToken
            };
        }

        private static AwsCredentials CreateAwsCredentialsFromEnvironmentVariables()
        {
            var username = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
            var password = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
            var sessionToken = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");
            return new AwsCredentials()
            {
                UserName = username,
                Password = UsernamePasswordCredential.ConvertPasswordToSecureString(password),
                SessionToken = sessionToken
            };
        }

        private static AwsCredentials CreateAwsCredentialsFromEcsResponse()
        {
            var response = HttpClientHelper.Instance.GetEcsResponse().GetAwaiter().GetResult();
            var parsedReponse = BsonDocument.Parse(response);
            var username = parsedReponse.GetValue("AccessKeyId", null)?.AsString;
            var password = parsedReponse.GetValue("SecretAccessKey", null)?.AsString;
            var sessionToken = parsedReponse.GetValue("Token", null)?.AsString;

            return new AwsCredentials()
            {
                UserName = username,
                Password = UsernamePasswordCredential.ConvertPasswordToSecureString(password),
                SessionToken = sessionToken
            };
        }

        private static AwsCredentials CreateAwsCredentialsFromEc2Response()
        {
            var response = HttpClientHelper.Instance.GetEcs2Response().GetAwaiter().GetResult();
            var parsedReponse = BsonDocument.Parse(response);
            var username = parsedReponse.GetValue("AccessKeyId", null)?.AsString;
            var password = parsedReponse.GetValue("SecretAccessKey", null)?.AsString;
            var sessionToken = parsedReponse.GetValue("Token", null)?.AsString;

            return new AwsCredentials()
            {
                UserName = username,
                Password = UsernamePasswordCredential.ConvertPasswordToSecureString(password),
                SessionToken = sessionToken
            };
        }

        private static string ExtractSessionTokenFromMechanismProperties(IEnumerable<KeyValuePair<string, string>> properties)
        {
            string sessionToken = null;
            if (properties != null)
            {
                foreach (var pair in properties)
                {
                    switch (pair.Key.ToUpperInvariant())
                    {
                        case "AWS_SESSION_TOKEN":
                            sessionToken = pair.Value;
                            break;
                        default:
                            throw new ArgumentException($"Unknown AWS property '{pair.Key}'.", "properties");
                    }
                }
            }

            return sessionToken;
        }

        private static void ValidateCredentials(AwsCredentials awsCredentials)
        {
            if (awsCredentials.UserName == null && (awsCredentials.Password != null || awsCredentials.SessionToken != null))
            {
                throw new ArgumentException("A MONGODB-AWS must have access key id.");
            }
            if (awsCredentials.UserName != null && awsCredentials.Password == null)
            {
                throw new ArgumentException("A MONGODB-AWS must have secret access key.");
            }
        }

        // nested classes
        private class AwsCredentials
        {
            public string UserName;
            public SecureString Password;
            public string SessionToken;
        }

        private class MongoAWSMechanism : ISaslMechanism
        {
            private readonly UsernamePasswordCredential _credential;
            private readonly IRandomByteGenerator _randomByteGenerator;
            private readonly string _sessionToken;

            public MongoAWSMechanism(
                UsernamePasswordCredential credential,
                string sessionToken,
                IRandomByteGenerator randomByteGenerator)
            {
                _credential = Ensure.IsNotNull(credential, nameof(credential));
                _sessionToken = sessionToken;
                _randomByteGenerator = Ensure.IsNotNull(randomByteGenerator, nameof(randomByteGenerator));
            }

            public string Name
            {
                get { return MechanismName; }
            }

            public ISaslStep Initialize(IConnection connection, SaslConversation conversation, ConnectionDescription description)
            {
                Ensure.IsNotNull(connection, nameof(connection));
                Ensure.IsNotNull(description, nameof(description));

                var nonce = GenerateRandomBytes();

                var document = new BsonDocument()
                    .Add("r", new BsonBinaryData(nonce))
                    .Add("p", new BsonInt32('n'));

                var clientFirstMessageBytes = ToBytes(document);

                return new ClientFirst(clientFirstMessageBytes, nonce, _credential, _sessionToken);
            }

            private byte[] GenerateRandomBytes()
            {
                return _randomByteGenerator.Generate(randomLength);
            }
        }

        private static BsonDocument ToDocument(byte[] bytes)
        {
            var stream = new MemoryStream(bytes);
            using (var jsonReader = new BsonBinaryReader(stream))
            {
                var context = BsonDeserializationContext.CreateRoot(jsonReader);
                return BsonDocumentSerializer.Instance.Deserialize(context);
            }
        }

        private static byte[] ToBytes(BsonDocument doc)
        {
            BsonBinaryWriterSettings settings = new BsonBinaryWriterSettings()
            {
#pragma warning disable 618
                GuidRepresentation = GuidRepresentation.Standard
#pragma warning restore 618
            };
            return doc.ToBson(null, settings);
        }

        private class ClientFirst : ISaslStep
        {
            private readonly byte[] _bytesToSendToServer;
            private readonly byte[] _nonce;
            private readonly UsernamePasswordCredential _credential;
            private readonly string _sessionToken;

            public ClientFirst(
                byte[] bytesToSendToServer,
                byte[] nonce,
                UsernamePasswordCredential credential,
                string sessionToken)
            {
                _bytesToSendToServer = bytesToSendToServer;
                _nonce = nonce;
                _credential = credential;
                _sessionToken = sessionToken;
            }

            public byte[] BytesToSendToServer
            {
                get { return _bytesToSendToServer; }
            }

            public bool IsComplete
            {
                get { return false; }
            }

            public ISaslStep Transition(SaslConversation conversation, byte[] bytesReceivedFromServer)
            {
                var serverFirstMessageDoc = ToDocument(bytesReceivedFromServer);
                var serverNonce = serverFirstMessageDoc["s"].AsByteArray;
                var host = serverFirstMessageDoc["h"].AsString;

                if (serverNonce.Length != randomLength * 2 || !serverNonce.Take(randomLength).SequenceEqual(_nonce))
                {
                    throw new MongoAuthenticationException(conversation.ConnectionId, message: "Server sent an invalid nonce.");
                }

                var tuple = AwsSignatureVersion4.SignRequest(
                    _credential.Username,
                    _credential.GetInsecurePassword(),
                    _sessionToken,
                    serverNonce,
                    host);

                var document = new BsonDocument()
                    .Add("a", tuple.Item1)
                    .Add("d", tuple.Item2);

                if (_sessionToken != null)
                {
                    document.Add("t", _sessionToken);
                }

                var clientSecondMessageBytes = ToBytes(document);

                return new ClientLast(clientSecondMessageBytes);
            }
        }

        private class ClientLast : ISaslStep
        {
            private readonly byte[] _bytesToSendToServer;

            public ClientLast(byte[] bytesToSendToServer)
            {
                _bytesToSendToServer = bytesToSendToServer;
            }

            public byte[] BytesToSendToServer
            {
                get { return _bytesToSendToServer; }
            }

            public bool IsComplete
            {
                get { return false; }
            }

            public ISaslStep Transition(SaslConversation conversation, byte[] bytesReceivedFromServer)
            {
                return new CompletedStep();
            }
        }

        private class HttpClientHelper
        {
            // private constants
            private readonly Uri ecsBaseUri = new Uri("http://169.254.170.2");
            private readonly Uri ec2BaseUri = new Uri("http://169.254.169.254");

            #region static
            private static Lazy<HttpClientHelper> __instance = new Lazy<HttpClientHelper>(() => new HttpClientHelper(), isThreadSafe: true);
            public static HttpClientHelper Instance => __instance.Value;
            #endregion

            private readonly Lazy<HttpClient> _httpClientInstance = new Lazy<HttpClient>(() =>
            {
                var httpClient = new HttpClient
                {
                    Timeout = TimeSpan.FromSeconds(10)
                };
                return httpClient;
            }, isThreadSafe: true);

            private HttpClientHelper() { }

            public async Task<string> GetEcsResponse()
            {
                var relativeUri = Environment.GetEnvironmentVariable("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
                var credentialsRequest = new HttpRequestMessage
                {
                    RequestUri = new Uri(ecsBaseUri, relativeUri),
                    Method = HttpMethod.Get
                };
                var credentials = await Send(credentialsRequest, "Failed to acquire EC2 credentials.").ConfigureAwait(false);

                return credentials;
            }

            public async Task<string> GetEcs2Response()
            {
                var tokenRequest = CreateTokenRequest(ec2BaseUri);
                var token = await Send(tokenRequest, "Failed to acquire EC2 token.").ConfigureAwait(false);

                var roleRequest = CreateRoleRequest(ec2BaseUri, token);
                var role = await Send(roleRequest, "Failed to acquire EC2 role name.").ConfigureAwait(false);

                var credentialsRequest = CreateCreadentialRequest(ec2BaseUri, role, token);
                var credentials = await Send(credentialsRequest, "Failed to acquire EC2 credentials.").ConfigureAwait(false);

                return credentials;
            }

            // private methods
            private HttpRequestMessage CreateTokenRequest(Uri baseUri)
            {
                var tokenRequest = new HttpRequestMessage
                {
                    RequestUri = new Uri(baseUri, "latest/api/token"),
                    Method = HttpMethod.Put,
                };
                tokenRequest.Headers.Add("X-aws-ec2-metadata-token-ttl-seconds", "30");
                return tokenRequest;
            }

            private HttpRequestMessage CreateRoleRequest(Uri baseUri, string token)
            {
                var roleRequest = new HttpRequestMessage
                {
                    RequestUri = new Uri(baseUri, "latest/meta-data/iam/security-credentials/"),
                    Method = HttpMethod.Get
                };
                roleRequest.Headers.Add("X-aws-ec2-metadata-token", token);
                return roleRequest;
            }

            private HttpRequestMessage CreateCreadentialRequest(Uri baseUri, string roleName, string token)
            {
                var credentialsUri = new Uri(baseUri, "latest/meta-data/iam/security-credentials/");
                var credentialsRequest = new HttpRequestMessage
                {
                    RequestUri = new Uri(credentialsUri, roleName),
                    Method = HttpMethod.Get
                };
                credentialsRequest.Headers.Add("X-aws-ec2-metadata-token", token);
                return credentialsRequest;
            }

            private async Task<string> Send(HttpRequestMessage request, string exceptionMessage)
            {
                HttpResponseMessage response;
                try
                {
                    response = await _httpClientInstance.Value.SendAsync(request).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                }
                catch (Exception ex) when (ShouldWrapException(ex))
                {
                    throw new MongoClientException(exceptionMessage, ex);
                }

                return await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                bool ShouldWrapException(Exception ex)
                {
                    return ex is HttpRequestException || ex is HttpRequestException;
                }
            }
        }
    }
}
