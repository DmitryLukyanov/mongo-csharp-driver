﻿/* Copyright 2019–present MongoDB Inc.
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

using System.Collections.Generic;
using MongoDB.Driver.Core.Compression;

namespace MongoDB.Driver.Core.Configuration
{
    /// <summary>
    /// Represents a compressor configuration.
    /// </summary>
    public sealed class CompressorConfiguration
    {
        /// <summary>
        /// Initializes an instance of <see cref="CompressorConfiguration"/>.
        /// </summary>
        /// <param name="type">The compressor type.</param>
        public CompressorConfiguration(CompressorType type)
        {
            Type = type;
            Properties = new Dictionary<string, object>();
        }

        /// <summary>
        /// Gets the compression properties.
        /// </summary>
        public IDictionary<string, object> Properties { get; }

        /// <summary>
        /// Gets the compressor type.
        /// </summary>
        public CompressorType Type { get; }
    }
}
