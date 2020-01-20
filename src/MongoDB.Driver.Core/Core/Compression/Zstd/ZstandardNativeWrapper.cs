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
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Zstd
{
    internal class ZstandardNativeWrapper : IDisposable
    {
        #region static
        public static int MaxCompressionLevel => Zstandard64NativeMethods.ZSTD_maxCLevel();
        #endregion

        private readonly int _compressionLevel;
        private readonly CompressionMode _compressionMode;
        private readonly NativeBufferInfo _inputNativeBuffer = new NativeBufferInfo();
        private readonly NativeBufferInfo _outputNativeBuffer = new NativeBufferInfo();
        private bool _operationInitialized;
        private readonly int _recommendedZstreamInputSize;
        private readonly int _recommendedZstreamOutputSize;
        private IntPtr _zstreamPointer;

        public ZstandardNativeWrapper(CompressionMode compressionMode, int compressionLevel)
        {
            _compressionLevel = compressionLevel;
            _compressionMode = compressionMode;

            switch (_compressionMode)
            {
                case CompressionMode.Compress:
                    _recommendedZstreamInputSize = ZstandardNativeAdapter
                        .ZSTD_CStreamInSize();  // calculate recommended size for input buffer;
                    _recommendedZstreamOutputSize = ZstandardNativeAdapter
                        .ZSTD_CStreamOutSize();  // calculate recommended size for output buffer. Guarantee to successfully flush at least one complete compressed block
                    _zstreamPointer = Zstandard64NativeMethods.ZSTD_createCStream(); // create resource
                    break;
                case CompressionMode.Decompress:
                    _recommendedZstreamInputSize = ZstandardNativeAdapter
                        .ZSTD_DStreamInSize();  // calculate recommended size for input buffer
                    _recommendedZstreamOutputSize = ZstandardNativeAdapter
                        .ZSTD_DStreamOutSize();  // calculate recommended size for output buffer. Guarantee to successfully flush at least one complete block in all circumstances
                    _zstreamPointer = Zstandard64NativeMethods.ZSTD_createDStream(); // create resource
                    break;
            }
        }

        public int RecommendedInputSize => _recommendedZstreamInputSize;
        public int RecommendedOutputSize => _recommendedZstreamOutputSize;

        // public methods
        public void Compress(
            OperationContext operationContext,
            int compressedSize,
            int uncompressedSize,
            out int compressedBufferPosition,
            out int uncompressedBufferPosition)
        {
            Ensure.IsNotNull(operationContext, nameof(operationContext));

            InitializeIfNotAlreadyInitialized();

            ZstandardNativeAdapter.ZSTD_compressStream(
                zstreamPointer: _zstreamPointer,
                outputBuffer: _outputNativeBuffer,
                compressedPinnedBuffer: operationContext.CompressedPinnedBuffer,
                compressedSize: compressedSize,
                inputBuffer: _inputNativeBuffer,
                uncompressedPinnedBuffer: operationContext.UncompressedPinnedBuffer,
                uncompressedSize: uncompressedSize,
                compressedBufferPosition: out compressedBufferPosition,
                uncompressedBufferPosition: out uncompressedBufferPosition);

            // calculate progress in _inputNativeBuffer
            operationContext.UncompressedPinnedBuffer.Offset += uncompressedBufferPosition;
            // CompressedPinnedBuffer.Offset is always 0
        }

        public void Decompress(
            OperationContext operationContext,
            int compressedOffset,
            int compressedSize,
            int uncompressedSize,
            out int compressedBufferPosition,
            out int uncompressedBufferPosition)
        {
            Ensure.IsNotNull(operationContext, nameof(operationContext));

            InitializeIfNotAlreadyInitialized();

            // apply reading progress on CompressedPinnedBuffer
            operationContext.CompressedPinnedBuffer.Offset = compressedOffset;

            ZstandardNativeAdapter.ZSTD_decompressStream(
                zstreamPointer: _zstreamPointer,
                inputBuffer: _inputNativeBuffer,
                compressedPinnedBuffer: operationContext.CompressedPinnedBuffer,
                compressedSize: compressedSize <= 0 ? 0 : compressedSize,
                outputBuffer: _outputNativeBuffer,
                uncompressedPinnedBuffer: operationContext.UncompressedPinnedBuffer,
                uncompressedSize: uncompressedSize,
                uncompressedBufferPosition: out uncompressedBufferPosition,
                compressedBufferPosition: out compressedBufferPosition);

            operationContext.UncompressedPinnedBuffer.Offset += uncompressedBufferPosition;
            // CompressedPinnedBuffer.Offset will be calculated on stream side
        }

        public void Dispose()
        {
            switch (_compressionMode)
            {
                case CompressionMode.Compress:
                    Zstandard64NativeMethods.ZSTD_freeCStream(_zstreamPointer);
                    break;
                case CompressionMode.Decompress:
                    Zstandard64NativeMethods.ZSTD_freeDStream(_zstreamPointer);
                    break;
            }
        }

        public OperationContext InitializeOperationContext(
            BufferInfo compressedBufferInfo,
            BufferInfo uncompressedBufferInfo = null)
        {
            var compressedPinnedBuffer = new PinnedBuffer(compressedBufferInfo.Bytes, compressedBufferInfo.Offset);
            PinnedBuffer uncompressedPinnedBuffer = null;
            if (uncompressedBufferInfo != null)
            {
                uncompressedPinnedBuffer = new PinnedBuffer(uncompressedBufferInfo.Bytes, uncompressedBufferInfo.Offset);
            }

            return new OperationContext(uncompressedPinnedBuffer, compressedPinnedBuffer);
        }

        public IEnumerable<int> FlushBySteps(BufferInfo compressedBufferInfo)
        {
            if (_compressionMode != CompressionMode.Compress)
            {
                throw new InvalidDataException("FlushBySteps must be called only from Compress mode.");
            }

            using (var operationContext = InitializeOperationContext(compressedBufferInfo))
            {
                yield return ZstandardNativeAdapter.ZSTD_flushStream(
                    _zstreamPointer,
                    _outputNativeBuffer,
                    operationContext.CompressedPinnedBuffer,
                    _recommendedZstreamOutputSize);
            }

            using (var operationContext = InitializeOperationContext(compressedBufferInfo))
            {
                yield return ZstandardNativeAdapter.ZSTD_endStream(
                    _zstreamPointer,
                    _outputNativeBuffer,
                    operationContext.CompressedPinnedBuffer,
                    _recommendedZstreamOutputSize);
            }
        }

        // private methods
        private void InitializeIfNotAlreadyInitialized()
        {
            if (!_operationInitialized)
            {
                _operationInitialized = true;

                switch (_compressionMode)
                {
                    case CompressionMode.Compress:
                        Zstandard64NativeMethods.ZSTD_initCStream(_zstreamPointer, _compressionLevel); // start a new compression operation
                        break;
                    case CompressionMode.Decompress:
                        Zstandard64NativeMethods.ZSTD_initDStream(_zstreamPointer); // start a new decompression operation
                        break;
                }
            }
        }

        // nested types
        internal class OperationContext : IDisposable
        {
            private readonly PinnedBuffer _uncompressedPinnedBuffer;
            private readonly PinnedBuffer _compressedPinnedBuffer;

            public OperationContext(PinnedBuffer uncompressedPinnedBuffer, PinnedBuffer compressedPinnedBuffer)
            {
                _uncompressedPinnedBuffer = uncompressedPinnedBuffer; // can be null
                _compressedPinnedBuffer = Ensure.IsNotNull(compressedPinnedBuffer, nameof(compressedPinnedBuffer));
            }

            public PinnedBuffer UncompressedPinnedBuffer => _uncompressedPinnedBuffer; // external data
            public PinnedBuffer CompressedPinnedBuffer => _compressedPinnedBuffer; // internal data

            // public methods
            public void Dispose()
            {
                _uncompressedPinnedBuffer?.Dispose(); // PinnedBuffer.Dispose suppress all errors inside
                _compressedPinnedBuffer.Dispose();
            }
        }
    }

    internal class BufferInfo
    {
        public BufferInfo(byte[] bytes, int offset)
        {
            Bytes = bytes;
            Offset = offset;
        }

        public byte[] Bytes { get; }
        public int Offset { get; }
    }
}
