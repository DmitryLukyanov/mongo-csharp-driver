/* Copyright 2010-present MongoDB Inc.
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
using MongoDB.Bson;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Operations
{
    internal class BulkUpdateOperation : BulkUnmixedWriteOperationBase<UpdateRequest>
    {
        // constructors
        public BulkUpdateOperation(
            CollectionNamespace collectionNamespace,
            IEnumerable<UpdateRequest> requests,
            ClientSideTimeout clientSideTimeout,
            MessageEncoderSettings messageEncoderSettings)
            : base(collectionNamespace, requests, clientSideTimeout, messageEncoderSettings)
        {
        }

        // methods
        protected override IRetryableWriteOperation<BsonDocument> CreateBatchOperation(Batch batch)
        {
            return new RetryableUpdateCommandOperation(CollectionNamespace, batch.Requests, MessageEncoderSettings)
            {
                BypassDocumentValidation = BypassDocumentValidation,
                IsOrdered = IsOrdered,
                MaxBatchCount = MaxBatchCount,
                RetryRequested = RetryRequested,
                WriteConcern = WriteConcern
            };
        }

        protected override bool RequestHasCollation(UpdateRequest request)
        {
            return request.Collation != null;
        }

        protected override bool RequestHasHint(UpdateRequest request)
        {
            return request.Hint != null;
        }
    }
}
