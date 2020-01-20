/* Copyright 2020–present MongoDB Inc.
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
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Zstd
{
    internal static class ZstandardNativeAdapter
    {
        public static void ZSTD_compressStream(
            IntPtr zstreamPointer,
            NativeBufferInfo outputBuffer,
            PinnedBuffer compressedPinnedBuffer,
            int compressedSize,
            NativeBufferInfo inputBuffer,
            PinnedBuffer uncompressedPinnedBuffer,
            int uncompressedSize,
            out int compressedBufferPosition,
            out int uncompressedBufferPosition)
        {
            ConfigureNativeBuffer(
                outputBuffer,
                compressedPinnedBuffer, // operation result
                (uint)compressedSize);

            // uncompressed data
            ConfigureNativeBuffer(
                inputBuffer,
                uncompressedPinnedBuffer,
                (uint)uncompressedSize);

            // compress _inputNativeBuffer to _outputNativeBuffer
            Zstandard64NativeMethods.ZSTD_compressStream(
                zstreamPointer,
                outputBuffer: outputBuffer,
                inputBuffer: inputBuffer);

            uncompressedBufferPosition = (int)inputBuffer.Position;
            compressedBufferPosition = (int)outputBuffer.Position;
        }

        public static int ZSTD_CStreamInSize()
        {
            return (int)Zstandard64NativeMethods.ZSTD_CStreamInSize().ToUInt64();
        }

        public static int ZSTD_CStreamOutSize()
        {
            return (int)Zstandard64NativeMethods.ZSTD_CStreamOutSize().ToUInt64();
        }

        public static void ZSTD_decompressStream(
            IntPtr zstreamPointer,
            NativeBufferInfo inputBuffer,
            PinnedBuffer compressedPinnedBuffer,
            int compressedSize,
            NativeBufferInfo outputBuffer,
            PinnedBuffer uncompressedPinnedBuffer,
            int uncompressedSize,
            out int uncompressedBufferPosition,
            out int compressedBufferPosition)
        {
            // compressed data
            ConfigureNativeBuffer(
                inputBuffer,
                compressedPinnedBuffer,
                (uint)compressedSize);

            // uncompressed data
            ConfigureNativeBuffer(
                outputBuffer,
                uncompressedPinnedBuffer, // operation result
                (uint)uncompressedSize);

            Zstandard64NativeMethods.ZSTD_decompressStream(
                zstreamPointer,
                outputBuffer: outputBuffer,
                inputBuffer: inputBuffer);

            compressedBufferPosition = (int)inputBuffer.Position;
            uncompressedBufferPosition = (int)outputBuffer.Position;
        }

        public static int ZSTD_DStreamInSize()
        {
            return (int)Zstandard64NativeMethods.ZSTD_DStreamInSize().ToUInt64();
        }

        public static int ZSTD_DStreamOutSize()
        {
            return (int)Zstandard64NativeMethods.ZSTD_DStreamOutSize().ToUInt64();
        }

        public static int ZSTD_endStream(IntPtr zstreamPointer, NativeBufferInfo outputBuffer, PinnedBuffer compressedPinnedBuffer, int recommendedOutputSize)
        {
            ConfigureNativeBuffer(
                outputBuffer,
                compressedPinnedBuffer,
                (uint)recommendedOutputSize);
            Zstandard64NativeMethods.ZSTD_endStream(zstreamPointer, outputBuffer);
            return (int)outputBuffer.Position;
        }

        public static int ZSTD_flushStream(IntPtr zstreamPointer, NativeBufferInfo outputBuffer, PinnedBuffer compressedPinnedBuffer, int recommendedOutputSize)
        {
            ConfigureNativeBuffer(
                outputBuffer,
                compressedPinnedBuffer,
                (uint)recommendedOutputSize);
            Zstandard64NativeMethods.ZSTD_flushStream(zstreamPointer, outputBuffer);
            return (int)outputBuffer.Position;
        }

        // private static methods
        private static void ConfigureNativeBuffer(NativeBufferInfo buffer, PinnedBuffer pinnedBuffer, uint size)
        {
            buffer.DataPointer = pinnedBuffer?.IntPtr ?? IntPtr.Zero;
            buffer.Size = size;
            buffer.Position = 0;
        }
    }
}
