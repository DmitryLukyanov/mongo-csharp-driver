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

using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Tests.UnifiedTestOperations
{
    public class UnifiedListDatabasesOperation : IUnifiedEntityTestOperation
    {
        private readonly IMongoClient _client;
        private readonly ListDatabasesOptions _options;

        public UnifiedListDatabasesOperation(IMongoClient client, ListDatabasesOptions options)
        {
            _client = Ensure.IsNotNull(client, nameof(client));
            _options = options;
        }

        public OperationResult Execute(CancellationToken cancellationToken)
        {
            try
            {
                var cursor = _client.ListDatabases(_options, cancellationToken);
                var result = cursor.ToList();

                return OperationResult.FromResult(new BsonArray(result));
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
                var cursor = await _client.ListDatabasesAsync(_options, cancellationToken);
                var result = await cursor.ToListAsync();

                return OperationResult.FromResult(new BsonArray(result));
            }
            catch (Exception exception)
            {
                return OperationResult.FromException(exception);
            }
        }
    }

    public class UnifiedListDatabasesOperationBuilder
    {
        private readonly UnifiedEntityMap _entityMap;

        public UnifiedListDatabasesOperationBuilder(UnifiedEntityMap entityMap)
        {
            _entityMap = entityMap;
        }

        public UnifiedListDatabasesOperation Build(string targetClientId, BsonDocument arguments)
        {
            var client = _entityMap.GetClient(targetClientId);
            var options = new ListDatabasesOptions();

            foreach (var argument in arguments)
            {
                switch (argument.Name)
                {
                    case "filter":
                        options.Filter = argument.Value.AsBsonDocument;
                        break;
                    case "timeoutMS":
                        options.Timeout = TimeSpan.FromMilliseconds(argument.Value.ToInt32());
                        break;
                    default:
                        throw new FormatException($"Invalid {nameof(UnifiedListDatabasesOperation)} argument name: '{argument.Name}'.");
                }
            }

            return new UnifiedListDatabasesOperation(client, options);
        }
    }
}
