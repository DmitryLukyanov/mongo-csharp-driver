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


using System.IO;
using System.IO.Compression;
using MongoDB.Driver.Core.Compression.Zstd;

namespace MongoDB.Driver.Core.Compression
{
    internal class ZstandardCompressor : ICompressor
    {
        public CompressorType Type => CompressorType.Zstandard;

        public void Compress(Stream input, Stream output)
        {
            using (var memoryStream = new MemoryStream())
            {
                input.CopyTo(memoryStream);
                using (var compressionStream = new ZstandardStream(output, CompressionMode.Compress, leaveOpen: true))
                {
                    //compressionStream.CompressionLevel = 11;               // optional!!
                    //compressionStream.CompressionDictionary = dictionary;  // optional!!
                    compressionStream.Write(memoryStream.ToArray(), 0, (int)input.Length);
                    compressionStream.Close();
                }
            }
        }

        public void Decompress(Stream input, Stream output)
        {
            //sing (var memoryStream = new MemoryStream(compressed))
            using (var compressionStream = new ZstandardStream(input, CompressionMode.Decompress))
            //using (var temp = new MemoryStream())
            {
                //compressionStream.CompressionDictionary = dictionary;  // optional!!
                compressionStream.CopyTo(output);
                //output = temp.ToArray();
            }
        }
    }
}
