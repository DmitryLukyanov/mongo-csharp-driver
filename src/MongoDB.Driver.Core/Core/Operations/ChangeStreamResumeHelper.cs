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

namespace MongoDB.Driver.Core.Operations
{
    internal static class ChangeStreamResumeHelper
    {
        public static ResumeStartValue GetEffectiveResumeStartValues<TResult>(ChangeStreamOperation<TResult> operation, bool resuming)
        {
            if (resuming)
            {
                var resumeToken = GetResumeToken(operation);
                if (resumeToken != null)
                {
                    return new ResumeStartValue { ResumeAfter = resumeToken };
                }

                if (operation.StartAtOperationTime != null || operation.InitialOperationTime != null)
                {
                    return new ResumeStartValue { StartAtOperationTime = operation.StartAtOperationTime ?? operation.InitialOperationTime };
                }
            }

            return new ResumeStartValue { ResumeAfter = operation.ResumeAfter, StartAfter = operation.StartAfter, StartAtOperationTime = operation.StartAtOperationTime };
        }

        private static BsonDocument GetInitialAggregateOptionOrNull<TResult>(ChangeStreamOperation<TResult> operation)
        {
            if (operation.StartAfter != null)
            {
                return operation.StartAfter;
            }
            else if (operation.ResumeAfter != null)
            {
                return operation.ResumeAfter;
            }
            else
            {
                return null;
            }
        }

        public static BsonDocument GetResumeToken<TResult>(ChangeStreamOperation<TResult> operation)
        {
            if (operation.BatchProcessingInfo.IsEmpty || operation.BatchProcessingInfo.IterationState == IterationState.Completed)
            {
                if (operation.BatchProcessingInfo.PostBatchResumeToken != null)
                {
                    return operation.BatchProcessingInfo.PostBatchResumeToken;
                }
                else
                {
                    if (operation.BatchProcessingInfo.LastIteratedDocumentId != null)
                    {
                        return operation.BatchProcessingInfo.LastIteratedDocumentId;
                    }
                    else
                    {
                        return GetInitialAggregateOptionOrNull(operation);
                    }
                }
            }

            if (!operation.BatchProcessingInfo.IsEmpty && operation.BatchProcessingInfo.IterationState == IterationState.InProgress)
            {
                return operation.BatchProcessingInfo.LastIteratedDocumentId;
            }

            if (!operation.BatchProcessingInfo.IsEmpty && operation.BatchProcessingInfo.IterationState == IterationState.NotStarted)
            {
                if (!operation.BatchProcessingInfo.HasGetMoreBeenCalled)
                {
                    return GetInitialAggregateOptionOrNull(operation);
                }
                else
                {
                    if (operation.BatchProcessingInfo.PostBatchResumeToken != null)
                    {
                        return operation.BatchProcessingInfo.PostBatchResumeToken;
                    }
                    else
                    {
                        if (operation.BatchProcessingInfo.LastIteratedDocumentId != null)
                        {
                            return operation.BatchProcessingInfo.LastIteratedDocumentId;
                        }
                        else
                        {
                            return GetInitialAggregateOptionOrNull(operation);
                        }
                    }
                }
            }

            return null;
        }

        // nested types
        internal struct ResumeStartValue
        {
            public BsonDocument ResumeAfter { get; set; }
            public BsonDocument StartAfter { get; set; }
            public BsonTimestamp StartAtOperationTime { get; set; }
        }
    }
}