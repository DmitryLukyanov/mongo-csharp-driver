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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FluentAssertions;
using MongoDB.Bson.IO;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Compression;
using Moq;
using SharpCompress.IO;
using Xunit;

namespace MongoDB.Driver.Core.Tests.Core.Compression
{
    public class CompressorsTests
    {
        private static string __testMessage = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

        [Theory]
        [InlineData(CompressorType.Snappy)]
        public void Compressor_should_read_the_previously_written_big_message_or_throw_the_exception_if_the_current_platform_is_not_supported(CompressorType compressorType)
        {
            var message = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

            var compressedMessage = "158;2;100;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;254;26;0;254;26;0;254;26;0;254;26;0;1;26";
            var decompressedMessage = "97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122;97;98;99;100;101;102;103;104;105;106;107;108;109;110;111;112;113;114;115;116;117;118;119;120;121;122";

            string TestCase(string incomeMessage, string expectedCompressed, string expectedDecompressed)
            {
                var bytes = Encoding.ASCII.GetBytes(incomeMessage);
                var compressor = GetCompressor(compressorType);

                var inputStream = GetStreamFromBytes(bytes);
                var outpustStream = GetStreamFromBytes();

                compressor.Compress(inputStream, outpustStream);

                var compressed = GetBytesFromStream(outpustStream);
                var compressedStr = string.Join(";", compressed);
                compressedStr.Should().Be(expectedCompressed);

                var compressedBytes = GetBytesFromStream(outpustStream);
                var list = new List<byte>();
                list.AddRange(new byte[]
                {
                    // Filled here: https://github.com/robertvazan/snappy.net/blob/master/Snappy.NET/SnappyFrame.cs#L342
                    255, 6, 0, 0,   // `255` - shows the type for next header section. 255 - `StreamIdentifier`, the next 6 bytes will parsed as `StreamIdentifier` header.
                    // Contains `ASCII` bytes from `sNaPpY` string. So, it's constant bytes.
                    //https://github.com/robertvazan/snappy.net/blob/master/Snappy.NET/SnappyFrame.cs#L26
                    115, 78, 97, 80, 112, 89, // contant bytes
                    // NOTE: the above header bytes are constants!!



                    // Contains information about compressed data
                    // show the type of data after the headers. `0` - Compressed: https://github.com/robertvazan/snappy.net/blob/master/Snappy.NET/SnappyFrameType.cs#L16.
                    // It means that next block(after the checksum header) will be tried to be decompressed.
                    // `47` - the size of all content after the current header. Which includes a `checksum` header + compressed data.
                    0, 47, 0, 0,  // so the full size of compressed data is `43` (47 - 4)

                    33, 133, 232, 85 // The checkSum header. Calculated here: https://github.com/robertvazan/snappy.net/blob/master/Snappy.NET/SnappyFrame.cs#L493
                });
                list.AddRange(compressedBytes.ToList());
                compressedBytes = list.ToArray();
                var outStream = GetStreamFromBytes(compressedBytes);
                var decStream = GetStreamFromBytes();
                outpustStream.Position = 0;
                compressor.Decompress(outStream, decStream);
                var decompressed = GetBytesFromStream(decStream);
                var decompressedStr = string.Join(";", decompressed);
                decompressedStr.Should().Be(expectedDecompressed);
                List<byte> resultBytes = new List<byte>();
                resultBytes.AddRange(decompressed);
                return Encoding.ASCII.GetString(resultBytes.ToArray());
            }

#if NET452 || NETSTANDARD2_0
            TestCase(message, compressedMessage, decompressedMessage)
                .Should().Be(message);
#else
            Record.Exception(() => TestCase(message, compressedMessage, decompressedMessage))
                .Should().BeOfType<NotSupportedException>();
#endif
        }

        private Stream GetStreamFromBytes(byte[] bytes = null)
        {
            return bytes != null ? new MemoryStream(bytes) : new MemoryStream();
        }

        private byte[] GetBytesFromStream(Stream str)
        {
            str.Position = 0;
            if (str is MemoryStream ms)
            {
                return ms.ToArray();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        [Theory]
        [InlineData(CompressorType.Snappy)]
        public void Compressor_should_read_the_previously_written_message_or_throw_the_exception_if_the_current_platform_is_not_supported(CompressorType compressorType)
        {
            var bytes = Encoding.ASCII.GetBytes(__testMessage);
            var compressor = GetCompressor(compressorType);
#if NET452 || NETSTANDARD2_0
            Assert(
                bytes,
                (input, output) =>
                {
                    compressor.Compress(input, output);
                    input.Length.Should().BeGreaterThan(output.Length);
                    input.Position = 0;
                    input.SetLength(0);
                    output.Position = 0;
                    compressor.Decompress(output, input);
                },
                (input, output) =>
                {
                    input.Position = 0;
                    var result = Encoding.ASCII.GetString(input.ReadBytes((int)input.Length));
                    result.Should().Be(__testMessage);
                });
#else
            var exception = Record.Exception(() => { compressor.Compress(Mock.Of<Stream>(), Mock.Of<Stream>()); });

            exception.Should().BeOfType<NotSupportedException>();

            exception = Record.Exception(() => { compressor.Decompress(Mock.Of<Stream>(), Mock.Of<Stream>()); });

            exception.Should().BeOfType<NotSupportedException>();
#endif
        }

        [Fact]
        public void Zlib_should_generate_expected_compressed_bytes()
        {
            var bytes = Encoding.ASCII.GetBytes(__testMessage);
            Assert(
                bytes,
                (input, output) =>
                {
                    var compressor = GetCompressor(CompressorType.Zlib, 6);
                    compressor.Compress(input, output);
                },
                (input, output) =>
                {
                    var resultBytes = output.ToArray();
                    var result = string.Join(",", resultBytes);
                    result
                        .Should()
                        .Be("120,156,74,76,74,78,73,77,75,207,200,204,202,206,201,205,203,47,40,44,42,46,41,45,43,175,168,172,74,36,67,6,0,0,0,255,255,3,0,21,79,33,94");
                });
        }

        [Theory]
        [InlineData(CompressorType.Zlib, -1)]
        [InlineData(CompressorType.Zlib, 0)]
        [InlineData(CompressorType.Zlib, 1)]
        [InlineData(CompressorType.Zlib, 2)]
        [InlineData(CompressorType.Zlib, 3)]
        [InlineData(CompressorType.Zlib, 4)]
        [InlineData(CompressorType.Zlib, 5)]
        [InlineData(CompressorType.Zlib, 6)]
        [InlineData(CompressorType.Zlib, 7)]
        [InlineData(CompressorType.Zlib, 8)]
        [InlineData(CompressorType.Zlib, 9)]
        public void Zlib_should_read_the_previously_written_message(CompressorType compressorType, int compressionOption)
        {
            var bytes = Encoding.ASCII.GetBytes(__testMessage);
            int zlibHeaderSize = 21;

            Assert(
                bytes,
                (input, output) =>
                {
                    var compressor = GetCompressor(compressorType, compressionOption);
                    compressor.Compress(input, output);
                    if (compressionOption != 0)
                    {
                        input.Length.Should().BeGreaterThan(output.Length);
                    }
                    else
                    {
                        output.Length.Should().Be(input.Length + zlibHeaderSize);
                    }
                    input.Position = 0;
                    input.SetLength(0);
                    output.Position = 0;
                    compressor.Decompress(output, input);
                },
                (input, output) =>
                {
                    input.Position = 0;
                    var result = Encoding.ASCII.GetString(input.ReadBytes((int)input.Length));
                    result.Should().Be(__testMessage);
                });
        }

        [Theory]
        [ParameterAttributeData]
        public void Zlib_should_throw_exception_if_the_level_is_out_of_range([Values(-2, 10)] int compressionOption)
        {
            var exception = Record.Exception(() => GetCompressor(CompressorType.Zlib, compressionOption));

            var e = exception.Should().BeOfType<ArgumentOutOfRangeException>().Subject;
            e.ParamName.Should().Be("compressionLevel");
        }

        private void Assert(byte[] bytes, Action<ByteBufferStream, MemoryStream> test, Action<ByteBufferStream, MemoryStream> assertResult = null)
        {
            using (var buffer = new ByteArrayBuffer(bytes))
            {
                var memoryStream = new MemoryStream();
                var byteBufferStream = new ByteBufferStream(buffer);
                using (new NonDisposingStream(memoryStream))
                using (new NonDisposingStream(byteBufferStream))
                {
                    test(byteBufferStream, memoryStream);
                    assertResult?.Invoke(byteBufferStream, memoryStream);
                }
            }
        }

        private ICompressor GetCompressor(CompressorType compressorType, object option = null)
        {
            switch (compressorType)
            {
                case CompressorType.Snappy:
                    return new SnappyCompressor();
                case CompressorType.Zlib:
                    return new ZlibCompressor((int)option);
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
