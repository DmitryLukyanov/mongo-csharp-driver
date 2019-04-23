/* Copyright 2019-present MongoDB Inc.
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

namespace MongoDB.Driver
{
    internal static class AggregateOutHelper
    {
        public static bool CanUseLegacyOutSyntax(AggregateOutStageOptions options)
        {
            return
                options.Mode == AggregateOutMode.ReplaceCollection &&
                string.IsNullOrWhiteSpace(options.DataBase) &&
                options.DataBase == null;
        }

        public static string GetCollection(BsonDocument outStage)
        {
            var outValue = outStage.GetElement(0).Value;
            if (outValue is BsonDocument document)
            {
                return document.GetElement("to").Value.AsString;
            }
            else
            {
                return outValue.AsString;
            }
        }

        public static bool IsOutDocument(BsonDocument document)
        {
            return document != null && document.GetElement(0).Name == "$out";
        }
    }
}
