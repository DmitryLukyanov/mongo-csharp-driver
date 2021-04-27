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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Tests.UnifiedTestOperations
{
    public class UnifiedIterateOnceOperation : IUnifiedEntityTestOperation
    {
        private readonly IEnumerator<ChangeStreamDocument<BsonDocument>> _changeStream;

        public UnifiedIterateOnceOperation(IEnumerator<ChangeStreamDocument<BsonDocument>> changeStream)
        {
            _changeStream = Ensure.IsNotNull(changeStream, nameof(changeStream));
        }

        public OperationResult Execute(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
            //_changeStream.MoveNext();
            //var result = CreateResult(_changeStream.Current);
            //return OperationResult.FromChangeStream(result);
        }

        public Task<OperationResult> ExecuteAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
            //_changeStream.MoveNext();
            //var result = CreateResult(_changeStream.Current);
            //return Task.FromResult(OperationResult.FromChangeStream(result));
        }

        // private methods
        private IEnumerable<ChangeStreamDocument<BsonDocument>> CreateResult(ChangeStreamDocument<BsonDocument> result)
        {
            return new List<ChangeStreamDocument<BsonDocument>>();
        }
    }

    public class UnifiedIterateOnceOperationBuilder
    {
        private readonly UnifiedEntityMap _entityMap;

        public UnifiedIterateOnceOperationBuilder(UnifiedEntityMap entityMap)
        {
            _entityMap = entityMap;
        }

        public UnifiedIterateOnceOperation Build(string targetChangeStreamId, BsonDocument arguments)
        {
            var changeStream = _entityMap.GetChangeStream(targetChangeStreamId);

            if (arguments != null)
            {
                throw new FormatException($"{nameof(UnifiedIterateOnceOperation)} is not expected to contain arguments.");
            }

            return new UnifiedIterateOnceOperation(changeStream);
        }
    }
}
