/* Copyright 2019–present MongoDB Inc.
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
using System.IO;
using System.Runtime.InteropServices;
using MongoDB.Driver.Core.Compression.Native;
using MongoDB.Driver.Core.Compression.Snappy.Native;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Compression.Snappy
{
    internal static class SnappyCodec
    {
        public static int Compress(byte[] input, int offset, int length, byte[] output, int outOffset)
        {
            Ensure.IsNotNull(input, nameof(input));
            Ensure.IsNotNull(output, nameof(output));
            EnsureInputRangeValid(offset, length, input.Length);
            EnsureOutputRangeValid(outOffset, output.Length, throwIfOutOffsetIsEqualToOutputLength: true);

            int outLength = output.Length - outOffset;
            if (offset == input.Length)
            {
                input = new byte[1];
                offset = 0;
            }

            // The array must be pinned by using a GCHandle before it is passed to UnsafeAddrOfPinnedArrayElement.
            // For maximum performance, this method does not validate the array passed to it; this can result in unexpected behavior.
            var inputHandle = GCHandle.Alloc(input, GCHandleType.Pinned);
            var outputHandle = GCHandle.Alloc(output, GCHandleType.Pinned);
            try
            {
                var inputPtr = Marshal.UnsafeAddrOfPinnedArrayElement(input, offset);
                var outputPtr = Marshal.UnsafeAddrOfPinnedArrayElement(output, outOffset);

                var status = SnappyNativeMethods.snappy_compress(inputPtr, length, outputPtr, ref outLength);
                switch (status)
                {
                    case SnappyStatus.Ok:
                        return outLength;
                    case SnappyStatus.BufferTooSmall:
                        throw new ArgumentOutOfRangeException("Output array is too small.");
                    default:
                        throw new InvalidDataException("Invalid input.");
                }
            }
            finally
            {
                inputHandle.Free();
                outputHandle.Free();
            }
        }

        public static byte[] Compress(byte[] input)
        {
            Ensure.IsNotNull(input, nameof(input));

            var max = GetMaxCompressedLength(input.Length);

            var output = new byte[max];
            var outLength = Compress(input, 0, input.Length, output, 0);
            if (outLength == max)
                return output;
            var truncated = new byte[outLength];
            Array.Copy(output, truncated, outLength);
            return truncated;
        }

        public static int GetMaxCompressedLength(int inLength)
        {
            return SnappyNativeMethods.snappy_max_compressed_length(inLength);
        }

        public static int GetUncompressedLength(byte[] input, int offset, int length)
        {
            Ensure.IsNotNull(input, nameof(input));
            EnsureInputRangeValid(offset, length, input.Length);
            if (length == 0)
            {
                throw new InvalidDataException("Compressed block cannot be empty.");
            }

            var inputHandle = GCHandle.Alloc(input, GCHandleType.Pinned);
            try
            {
                var inputPtr = Marshal.UnsafeAddrOfPinnedArrayElement(input, offset);
                var status = SnappyNativeMethods.snappy_uncompressed_length(inputPtr, length, out var outLength);
                switch (status)
                {
                    case SnappyStatus.Ok:
                        return outLength;
                    default:
                        throw new InvalidDataException("Input is not a valid snappy-compressed block.");
                }
            }
            finally
            {
                inputHandle.Free();
            }
        }

        public static int GetUncompressedLength(byte[] input)
        {
            Ensure.IsNotNull(input, nameof(input));

            return GetUncompressedLength(input, 0, input.Length);
        }

        public static int Uncompress(byte[] input, int offset, int length, byte[] output, int outOffset)
        {
            Ensure.IsNotNull(input, nameof(input));
            Ensure.IsNotNull(output, nameof(output));
            EnsureInputRangeValid(offset, length, input.Length);
            if (length == 0)
                throw new InvalidDataException("Compressed block cannot be empty.");
            EnsureOutputRangeValid(outOffset, output.Length);

            var outLength = output.Length - outOffset;
            if (outOffset == output.Length)
            {
                output = new byte[1];
                outOffset = 0;
            }

            var inputHandle = GCHandle.Alloc(input, GCHandleType.Pinned);
            var outputHandle = GCHandle.Alloc(output, GCHandleType.Pinned);
            try
            {
                var inputPtr = Marshal.UnsafeAddrOfPinnedArrayElement(input, offset);
                var outputPtr = Marshal.UnsafeAddrOfPinnedArrayElement(output, outOffset);

                var status = SnappyNativeMethods.snappy_uncompress(inputPtr, length, outputPtr, ref outLength);
                switch (status)
                {
                    case SnappyStatus.Ok:
                        return outLength;
                    case SnappyStatus.BufferTooSmall:
                        throw new ArgumentOutOfRangeException("Output array is too small.");
                    default:
                        throw new InvalidDataException("Input is not a valid snappy-compressed block.");
                }
            }
            finally
            {
                inputHandle.Free();
                outputHandle.Free();
            }
        }

        public static byte[] Uncompress(byte[] input)
        {
            var max = GetUncompressedLength(input);
            var output = new byte[max];
            var outLength = Uncompress(input, 0, input.Length, output, 0);
            if (outLength == max)
                return output;

            var truncated = new byte[outLength];
            Array.Copy(output, truncated, outLength);
            return truncated;
        }

        public static bool Validate(byte[] input, int offset, int length)
        {
            Ensure.IsNotNull(input, nameof(input));
            EnsureInputRangeValid(offset, length, input.Length);
            if (length == 0)
            {
                return false;
            }

            var inputHandle = GCHandle.Alloc(input, GCHandleType.Pinned);
            try
            {
                var inputPtr = Marshal.UnsafeAddrOfPinnedArrayElement(input, offset);
                return SnappyNativeMethods.snappy_validate_compressed_buffer(inputPtr, length) == SnappyStatus.Ok;
            }
            finally
            {
                inputHandle.Free();
            }
        }

        public static bool Validate(byte[] input)
        {
            Ensure.IsNotNull(input, nameof(input));

            return Validate(input, 0, input.Length);
        }

        // private static methods
        private static void EnsureInputRangeValid(int offset, int length, int inputLength)
        {
            if (offset < 0 || length < 0 || offset + length > inputLength)
                throw new ArgumentOutOfRangeException("Selected range is outside the bounds of the input array.");
        }

        private static void EnsureOutputRangeValid(int outOffset, int outputLength, bool throwIfOutOffsetIsEqualToOutputLength = false)
        {
            if (outOffset < 0 || outOffset > outputLength || (throwIfOutOffsetIsEqualToOutputLength && outOffset == outputLength))
                throw new ArgumentOutOfRangeException("Output offset is outside the bounds of the output array.");
        }
    }
}
