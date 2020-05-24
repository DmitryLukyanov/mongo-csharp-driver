﻿/* Copyright 2018–present MongoDB Inc.
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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Authentication;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.WireProtocol;

namespace MongoDB.Driver.Core.Connections
{
    internal static class IsMasterHelper
    {
        internal static BsonDocument AddClientDocumentToCommand(BsonDocument command, BsonDocument clientDocument)
        {
            return command.Add("client", clientDocument, clientDocument != null);
        }

        internal static BsonDocument AddCompressorsToCommand(BsonDocument command, IEnumerable<CompressorConfiguration> compressors)
        {
            var compressorsArray = new BsonArray(compressors.Select(x => x.Type.ToString().ToLowerInvariant()));

            return command.Add("compression", compressorsArray);
        }

        internal static BsonDocument CreateCommand(TopologyVersion topologyVersion, TimeSpan? maxAwaitTime)
        {
            Ensure.That(
                (topologyVersion != null && maxAwaitTime.HasValue) || (topologyVersion == null && !maxAwaitTime.HasValue),
                "Both topologyVersion and maxAwaitTime must be filled or null."); // TODO: message?

            var doc = new BsonDocument
            {
                { "isMaster", 1 },
                { "topologyVersion", topologyVersion?.ToBsonDocument(), topologyVersion != null },
                { "maxAwaitTimeMS", maxAwaitTime?.TotalMilliseconds, maxAwaitTime.HasValue }
            };
            return doc;
        }

        internal static BsonDocument CustomizeCommand(BsonDocument command, IReadOnlyList<IAuthenticator> authenticators)
        {
            return authenticators.Count == 1 ? authenticators[0].CustomizeInitialIsMasterCommand(command) : command;
        }

        internal static CommandWireProtocol<BsonDocument> CreateProtocol(
            BsonDocument isMasterCommand,
            bool isRequestExpected = true,
            int? responseTo = null)
        {
            var requestHandling = isRequestExpected ? CommandRequestHandling.Send : CommandRequestHandling.Skip;
            return new CommandWireProtocol<BsonDocument>(
                databaseNamespace: DatabaseNamespace.Admin,
                command: isMasterCommand,
                slaveOk: true,
                requestHandling,
                resultSerializer: BsonDocumentSerializer.Instance,
                messageEncoderSettings: null)
            {
                PreviousRequestId = responseTo
            };
        }

        //internal static CommandWireProtocol<IsMasterResult> CreateProtocol2(BsonDocument isMasterCommand)
        //{
        //    return new CommandWireProtocol<IsMasterResult>(
        //        databaseNamespace: DatabaseNamespace.Admin,
        //        command: isMasterCommand,
        //        slaveOk: true,
        //        resultSerializer: BsonSerializer.SerializerRegistry.GetSerializer<IsMasterResult>(), // TODO: validate!
        //        messageEncoderSettings: null);
        //}

        internal static IsMasterResult GetResult(
            IConnection connection,
            CommandWireProtocol<BsonDocument> isMasterProtocol,
            CancellationToken cancellationToken)
        {
            try
            {
                var stopwatch = Stopwatch.StartNew();
                var isMasterResult = isMasterProtocol.Execute(connection, cancellationToken);
                stopwatch.Stop();
                return new IsMasterResult(isMasterResult, stopwatch.Elapsed);
            }
            catch (MongoCommandException ex) when (ex.Code == 11)
            {
                // If the isMaster command fails with error code 11 (UserNotFound), drivers must consider authentication
                // to have failed.In such a case, drivers MUST raise an error that is equivalent to what they would have
                // raised if the authentication mechanism were specified and the server responded the same way.
                throw new MongoAuthenticationException(connection.ConnectionId, "User not found.", ex);
            }
        }

        internal static async Task<IsMasterResult> GetResultAsync(
            IConnection connection,
            CommandWireProtocol<BsonDocument> isMasterProtocol,
            CancellationToken cancellationToken)
        {
            try
            {
                var stopwatch = Stopwatch.StartNew();
                var isMasterResult = await isMasterProtocol.ExecuteAsync(connection, cancellationToken).ConfigureAwait(false);
                stopwatch.Stop();
                return new IsMasterResult(isMasterResult, stopwatch.Elapsed);
            }
            catch (MongoCommandException ex) when (ex.Code == 11)
            {
                // If the isMaster command fails with error code 11 (UserNotFound), drivers must consider authentication
                // to have failed.In such a case, drivers MUST raise an error that is equivalent to what they would have
                // raised if the authentication mechanism were specified and the server responded the same way.
                throw new MongoAuthenticationException(connection.ConnectionId, "User not found.", ex);
            }
        }
    }
}
