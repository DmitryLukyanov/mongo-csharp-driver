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
    /// <summary>
    /// Provides processing information about the current batch.
    /// </summary>
    public class CursorBatchProcessingInfo : IBatchInfo
    {
        /// <summary>
        /// Provides information whether a `getMore` command has been called or not.
        /// </summary>
        public bool HasGetMoreBeenCalled { get; protected set; }

        /// <summary>
        /// Provides information whether the processed batch is empty or not.
        /// </summary>
        public bool IsEmpty { get; protected set; }

        /// <summary>
        /// Provides information about a processing stage of the current batch.
        /// </summary>
        public IterationState IterationState { get; protected set; }

        /// <summary>
        /// Provides _id of the last iterated document.
        /// </summary>
        public BsonDocument LastIteratedDocumentId { get; protected set; }

        /// <summary>
        /// Provides a postBatchResumeToken from the current batch.
        /// </summary>
        public BsonDocument PostBatchResumeToken { get; protected set; }

        /// <summary>
        /// Save a flag that the current batch has been fully iterated.
        /// </summary>
        public void ConfirmCurrentIteration()
        {
            IterationState = IterationState.Completed;
        }

        /// <summary>
        /// Save a flag that a `getMore` command has been called.
        /// </summary>
        public void ConfirmGetMoreHasBeenCalled()
        {
            if (!HasGetMoreBeenCalled) // we need to know only whether a first attempt of the `getMore` command
                                       // has been called or not
            {
                HasGetMoreBeenCalled = true;
            }
        }

        /// <summary>
        /// Save information about the current batch.
        /// </summary>
        public void SaveCurrentBatchInfo(BsonDocument postBatchResumeToken, bool isEmpty)
        {
            IterationState = IterationState.NotStarted;
            PostBatchResumeToken = postBatchResumeToken;
            IsEmpty = isEmpty;
        }

        /// <summary>
        /// Save _id of the last iterated document in the batch.
        /// </summary>
        /// <param name="processedId">The document Id.</param>
        public void SaveIteratedDocumentId(BsonDocument processedId)
        {
            IterationState = IterationState.InProgress;
            LastIteratedDocumentId = processedId;
        }
    }

    /// <summary>
    /// Determines the batch iteration state.
    /// </summary>
    public enum IterationState
    {
        /// <summary>
        /// The batch has not been started at all.
        /// </summary>
        NotStarted,
        /// <summary>
        /// The batch has been iterated up to but not including the last element.
        /// </summary>
        InProgress,
        /// <summary>
        /// The batch has been iterated to the last document.
        /// </summary>
        Completed
    }
}