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
using System.Linq;
using System.Text;
using FluentAssertions;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Compression.Zstd;
using MongoDB.Driver.Core.Misc;
using Xunit;

namespace MongoDB.Driver.Core.Tests.Core.Compression
{
    public class ZstandardNativeTests
    {
        #region static
        private static readonly string __testMessage;

        static ZstandardNativeTests()
        {
            __testMessage = CreateTestMessage();
        }

        private static string CreateTestMessage()
        {
            var messagePortion = "abcdefghijklmnopqrstuvwxyz0123456789 ";
            var stringBuilder = new StringBuilder();
            for (int i = 0; i < 1000; i++)
            {
                stringBuilder.Append(messagePortion);
                stringBuilder.Append(messagePortion.Reverse());
                stringBuilder.Append(messagePortion.Insert(10, "!@#$%"));
            }
            return stringBuilder.ToString();
        }
        #endregion

        [Theory]
        [ParameterAttributeData]
        public void Compressor_should_decompress_the_previously_compressed_message([Range(1, 22)] int compressionLevel)
        {
            var messageBytes = Encoding.ASCII.GetBytes(__testMessage).ToArray();

            var compressedBytes = Compress(messageBytes, compressionLevel);
            compressedBytes.Length.Should().BeLessThan(messageBytes.Length / 3);

            var decompressedBytes = Decompress(compressedBytes);
            decompressedBytes.ShouldBeEquivalentTo(messageBytes);
        }

        [Fact]
        public void Compressed_size_with_low_compression_level_should_be_bigger_than_with_high()
        {
            var data = Encoding.ASCII.GetBytes(__testMessage);

            var compressedMin = Compress(data, 1);
            var compressedMax = Compress(data, ZstandardStream.MaxCompressionLevel);
            compressedMin.Length.Should().BeGreaterThan(compressedMax.Length);
        }

        // private methods
        private byte[] Compress(byte[] data, int compressionLevel)
        {
            using (var inputStream = new MemoryStream(data))
            using (var outputStream = new MemoryStream())
            {
                using (var zstdStream = new ZstandardStream(outputStream, CompressionMode.Compress, compressionLevel))
                {
                    inputStream.EfficientCopyTo(zstdStream);
                }
                return outputStream.ToArray();
            }
        }

        private byte[] Decompress(byte[] compressed)
        {
            using (var inputStream = new MemoryStream(compressed))
            using (var zstdStream = new ZstandardStream(inputStream, CompressionMode.Decompress))
            using (var outputStream = new MemoryStream())
            {
                zstdStream.CopyTo(outputStream);
                return outputStream.ToArray();
            }
        }
    }
}
