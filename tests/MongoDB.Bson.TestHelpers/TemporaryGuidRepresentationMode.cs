﻿/* Copyright 2019-present MongoDB Inc.
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

namespace MongoDB.Bson.TestHelpers
{
    public class TemporaryGuidRepresentationMode
    {
        private readonly GuidRepresentationMode _guidRepresentationMode;
        private readonly GuidRepresentation _guidRepresentation;

        public TemporaryGuidRepresentationMode(GuidRepresentationMode guidRepresentationMode, GuidRepresentation guidRepresentation = GuidRepresentation.Unspecified)
        {
            _guidRepresentationMode = guidRepresentationMode;
            _guidRepresentation = guidRepresentation;
        }

        public GuidRepresentationMode GuidRepresentationMode => _guidRepresentationMode;

        public GuidRepresentation GuidRepresenation => _guidRepresentation;

        public IDisposable Set()
        {
#pragma warning disable 618
            var resetter = new Resetter();
            BsonDefaults.GuidRepresentationMode = _guidRepresentationMode;
            if (_guidRepresentationMode == GuidRepresentationMode.V2)
            {
                BsonDefaults.GuidRepresentation = _guidRepresentation;
            }
            return resetter;
#pragma warning restore 618
        }

        public override string ToString()
        {
            return _guidRepresentationMode == GuidRepresentationMode.V2 ? $"V2:{_guidRepresentation}" : "V3";
        }

        private class Resetter : IDisposable
        {
            private GuidRepresentationMode _originalGuidRepresentationMode;
            private GuidRepresentation _originalGuidRepresentation;

            public Resetter()
            {
#pragma warning disable 618
                _originalGuidRepresentationMode = BsonDefaults.GuidRepresentationMode;
                if (_originalGuidRepresentationMode == GuidRepresentationMode.V2)
                {
                    _originalGuidRepresentation = BsonDefaults.GuidRepresentation;
                }
#pragma warning restore 618
            }

            public void Dispose()
            {
#pragma warning disable 618
                BsonDefaults.GuidRepresentationMode = _originalGuidRepresentationMode;
                if (_originalGuidRepresentationMode == GuidRepresentationMode.V2)
                {
                    BsonDefaults.GuidRepresentation = _originalGuidRepresentation;
                }
#pragma warning restore 618
            }
        }
    }
}
