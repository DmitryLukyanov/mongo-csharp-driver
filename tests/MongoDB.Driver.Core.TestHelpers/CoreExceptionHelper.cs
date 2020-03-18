/* Copyright 2018-present MongoDB Inc.
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
using System.IO;
using System.Net;
using MongoDB.Bson;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Operations;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Core.TestHelpers
{
    public static class CoreExceptionHelper
    {
        public static Exception CreateException(Type exceptionType)
        {
            Exception exception;
            switch (exceptionType.Name)
            {
                case "IOException":
                    exception = new IOException("Fake IOException.");
                    break;

                case "MongoConnectionException":
                    {
                        var clusterId = new ClusterId(1);
                        var serverId = new ServerId(clusterId, new DnsEndPoint("localhost", 27017));
                        var connectionId = new ConnectionId(serverId, 1);
                        var message = "Fake MongoConnectionException";
                        var innerException = new Exception();
                        exception = new MongoConnectionException(connectionId, message, innerException);
                    }
                    break;

                case "MongoCursorNotFoundException":
                    {
                        var clusterId = new ClusterId(1);
                        var serverId = new ServerId(clusterId, new DnsEndPoint("localhost", 27017));
                        var connectionId = new ConnectionId(serverId, 1);
                        var cursorId = 1L;
                        var query = new BsonDocument();
                        exception = new MongoCursorNotFoundException(connectionId, cursorId, query);
                    }
                    break;

                case "MongoNodeIsRecoveringException":
                    {
                        var clusterId = new ClusterId(1);
                        var serverId = new ServerId(clusterId, new DnsEndPoint("localhost", 27017));
                        var connectionId = new ConnectionId(serverId, 1);
                        var result = BsonDocument.Parse("{ code : 11600 }"); // InterruptedAtShutdown;
                        exception = new MongoNodeIsRecoveringException(connectionId, null, result);
                    }
                    break;

                case "MongoNotPrimaryException":
                    {
                        var clusterId = new ClusterId(1);
                        var serverId = new ServerId(clusterId, new DnsEndPoint("localhost", 27017));
                        var connectionId = new ConnectionId(serverId, 1);
                        var result = BsonDocument.Parse("{ code : 10107 }"); // NotMaster;
                        exception = new MongoNotPrimaryException(connectionId, null, result);
                    }
                    break;

                default:
                    throw new ArgumentException($"Unexpected exception type: {exceptionType.Name}.", nameof(exceptionType));
            }

            if (exception is MongoException mongoException)
            {
                RetryabilityHelper.AddRetryableWriteErrorLabelIfRequired(mongoException);
                RetryabilityHelper.AddResumableChangeStreamErrorLabelIfRequired(mongoException);
            }
            return exception;
        }

        public static MongoCommandException CreateMongoCommandException(int code = 1, string label = null)
        {
            var clusterId = new ClusterId(1);
            var endPoint = new DnsEndPoint("localhost", 27017);
            var serverId = new ServerId(clusterId, endPoint);
            var connectionId = new ConnectionId(serverId);
            var message = "Fake MongoCommandException";
            var command = BsonDocument.Parse("{ command : 1 }");
            var result = BsonDocument.Parse($"{{ ok: 0, code : {code} }}");
            var commandException = new MongoCommandException(connectionId, message, command, result);
            if (label != null)
            {
                commandException.AddErrorLabel(label);
            }

            return commandException;
        }

        public static MongoCommandException CreateMongoWriteConcernException(BsonDocument writeConcernResultDocument, string label = null)
        {
            var clusterId = new ClusterId(1);
            var endPoint = new DnsEndPoint("localhost", 27017);
            var serverId = new ServerId(clusterId, endPoint);
            var connectionId = new ConnectionId(serverId);
            var message = "Fake MongoWriteConcernException";
            var writeConcernResult = new WriteConcernResult(writeConcernResultDocument);
            var writeConcernException = new MongoWriteConcernException(connectionId, message, writeConcernResult);
            if (label != null)
            {
                writeConcernException.AddErrorLabel(label);
            }

            return writeConcernException;
        }
    }
}
