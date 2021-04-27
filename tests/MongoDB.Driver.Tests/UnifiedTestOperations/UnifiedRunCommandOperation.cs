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

using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Tests.UnifiedTestOperations
{
    public class UnifiedRunCommandOperation : IUnifiedEntityTestOperation
    {
        private readonly IMongoDatabase _database;
        private readonly string _commandName;
        private readonly BsonDocument _command;
        private readonly TimeSpan? _timeout;

        public UnifiedRunCommandOperation(
            IMongoDatabase database,
            string commandName,
            BsonDocument command,
            TimeSpan? timeout)
        {
            _database = Ensure.IsNotNull(database, nameof(database));
            _commandName = Ensure.IsNotNull(commandName, nameof(commandName));
            _command = Ensure.IsNotNull(command, nameof(command));
            _timeout = timeout; // can be null
            if (_timeout.HasValue)
            {
                throw new Exception("test");
            }
        }

        public OperationResult Execute(CancellationToken cancellationToken)
        {
            try
            {
                var result = _database.RunCommand<BsonDocument>(_command, cancellationToken: cancellationToken);

                return OperationResult.FromResult(result);
            }
            catch (Exception exception)
            {
                return OperationResult.FromException(exception);
            }
        }

        public async Task<OperationResult> ExecuteAsync(CancellationToken cancellationToken)
        {
            try
            {
                var result = await _database.RunCommandAsync<BsonDocument>(_command, cancellationToken: cancellationToken);

                return OperationResult.FromResult(result);
            }
            catch (Exception exception)
            {
                return OperationResult.FromException(exception);
            }
        }
    }

    public class UnifiedRunCommandOperationBuilder
    {
        private readonly UnifiedEntityMap _entityMap;

        public UnifiedRunCommandOperationBuilder(UnifiedEntityMap entityMap)
        {
            _entityMap = entityMap;
        }

        public UnifiedRunCommandOperation Build(string targetDatabaseId, BsonDocument arguments)
        {
            var database = _entityMap.GetDatabase(targetDatabaseId);

            string commandName = null;
            BsonDocument command = null;
            TimeSpan? timeout = null;

            foreach (var argument in arguments)
            {
                switch (argument.Name)
                {
                    case "commandName":
                        commandName = argument.Value.AsString;
                        break;
                    case "command":
                        command = argument.Value.AsBsonDocument;
                        break;
                    case "timeoutMS":
                        timeout = TimeSpan.FromMilliseconds(argument.Value.ToInt32());
                        break;
                    default:
                        throw new FormatException($"Invalid RunCommandOperation argument name: '{argument.Name}'.");
                }
            }

            return new UnifiedRunCommandOperation(database, commandName, command, timeout);
        }
    }
}
