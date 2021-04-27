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

using MongoDB.Bson;

namespace MongoDB.Driver.Tests.UnifiedTestOperations
{
    public class UnifiedCreateEntitiesOperation : IUnifiedSpecialTestOperation
    {
        private readonly BsonArray _entities;
        private readonly UnifiedEntityMapBuilder _entityMapBuilder;
        private readonly UnifiedEntityMap _entityMap;

        public UnifiedCreateEntitiesOperation(
            UnifiedEntityMapBuilder entityMapBuilder, BsonArray entities, UnifiedEntityMap entityMap)  // TODO:
        {
            _entities = entities;
            _entityMapBuilder = entityMapBuilder;
            _entityMap = entityMap;
        }

        public void Execute()
        {
            var operationEntityMap = _entityMapBuilder.Build(_entities);
            _entityMap.MergeOrThrowIfKeyConflict(operationEntityMap);
        }
    }

    public class UnifiedCreateEntitiesOperationBuilder
    {
        private readonly UnifiedEntityMap _entityMap;
        private readonly UnifiedEntityMapBuilder _unifiedEntityMapBuilder;

        public UnifiedCreateEntitiesOperationBuilder(UnifiedEntityMap entityMap, UnifiedEntityMapBuilder unifiedEntityMapBuilder)
        {
            _entityMap = entityMap;
            _unifiedEntityMapBuilder = unifiedEntityMapBuilder;
        }

        public UnifiedCreateEntitiesOperation Build(BsonDocument arguments)
        {
            var entities = arguments.GetValue("entities").AsBsonArray;
            return new UnifiedCreateEntitiesOperation(_unifiedEntityMapBuilder, entities, _entityMap);
        }
    }
}
