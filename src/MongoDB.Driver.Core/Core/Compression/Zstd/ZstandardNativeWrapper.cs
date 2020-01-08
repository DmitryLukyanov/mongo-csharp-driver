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
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Zstd
{
    internal class NativeWrapper : IDisposable
    {
        #region static
        public static int MaxCompressionLevel => ZstandardNativeMethods.ZSTD_maxCLevel();
        #endregion

        private readonly int _compressionLevel;
        private readonly CompressionMode _compressionMode;

        private readonly NativeBufferInfo _inputNativeBuffer = new NativeBufferInfo();
        private readonly NativeBufferInfo _outputNativeBuffer = new NativeBufferInfo();

        private bool _operationInitialized;

        private readonly uint _recommendedZstreamInputSize;
        private readonly uint _recommendedZstreamOutputSize;
        private IntPtr _zstreamPointer;

        public NativeWrapper(CompressionMode compressionMode, int compressionLevel)
        {
            _compressionLevel = compressionLevel;
            _compressionMode = compressionMode;

            switch (_compressionMode)
            {
                case CompressionMode.Compress:
                    _recommendedZstreamInputSize = ZstandardNativeMethods
                        .ZSTD_CStreamInSize()  // calculate recommended size for input buffer
                        .ToUInt32();
                    _recommendedZstreamOutputSize = ZstandardNativeMethods
                        .ZSTD_CStreamOutSize()  // calculate recommended size for output buffer. Guarantee to successfully flush at least one complete compressed block
                        .ToUInt32();
                    _zstreamPointer = ZstandardNativeMethods.ZSTD_createCStream(); // create resource
                    break;
                case CompressionMode.Decompress:
                    _recommendedZstreamInputSize = ZstandardNativeMethods
                        .ZSTD_DStreamInSize()  // calculate recommended size for input buffer
                        .ToUInt32();
                    _recommendedZstreamOutputSize = ZstandardNativeMethods
                        .ZSTD_DStreamOutSize()  // calculate recommended size for output buffer. Guarantee to successfully flush at least one complete block in all circumstances
                        .ToUInt32();
                    _zstreamPointer = ZstandardNativeMethods.ZSTD_createDStream(); // create resource
                    break;
            }
        }

        public int RecommendedInputSize => (int)_recommendedZstreamInputSize;
        public int RecommendedOutputSize => (int)_recommendedZstreamOutputSize;

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

            // compressed data
            ConfigureNativeBuffer(
                _outputNativeBuffer,
                operationContext.CompressedPinnedBuffer, // operation result
                (uint)compressedSize);

            // uncompressed data
            ConfigureNativeBuffer(
                _inputNativeBuffer,
                operationContext.UncompressedPinnedBuffer,
                (uint)uncompressedSize);

            // compress _inputNativeBuffer to _outputNativeBuffer
            ZstandardNativeMethods.ZSTD_compressStream(
                _zstreamPointer,
                outputBuffer: _outputNativeBuffer,
                inputBuffer: _inputNativeBuffer);

            compressedBufferPosition = (int)_outputNativeBuffer.Position.ToUInt32();

            // calculate progress in _inputNativeBuffer
            uncompressedBufferPosition = (int)_inputNativeBuffer.Position.ToUInt32();
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
            // compressed data
            ConfigureNativeBuffer(
                _inputNativeBuffer,
                compressedSize <= 0 ? null : operationContext.CompressedPinnedBuffer,
                compressedSize <= 0 ? 0 : (uint)compressedSize);

            // uncompressed data
            ConfigureNativeBuffer(
                _outputNativeBuffer,
                operationContext.UncompressedPinnedBuffer, // operation result
                (uint)uncompressedSize);

            // decompress _inputNativeBuffer to _outputNativeBuffer
            ZstandardNativeMethods.ZSTD_decompressStream(
                _zstreamPointer,
                outputBuffer: _outputNativeBuffer,
                inputBuffer: _inputNativeBuffer);

            // calculate progress in _outputNativeBuffer
            uncompressedBufferPosition = (int)_outputNativeBuffer.Position.ToUInt32();
            compressedBufferPosition = (int)_inputNativeBuffer.Position.ToUInt32();

            operationContext.UncompressedPinnedBuffer.Offset += uncompressedBufferPosition;
            // CompressedPinnedBuffer.Offset will be calculated on stream side
        }

        public void Dispose()
        {
            switch (_compressionMode)
            {
                case CompressionMode.Compress:
                    ZstandardNativeMethods.ZSTD_freeCStream(_zstreamPointer);
                    break;
                case CompressionMode.Decompress:
                    ZstandardNativeMethods.ZSTD_freeDStream(_zstreamPointer);
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
                yield return ProcessCompressedOutput(operationContext, (zcs, buffer) => ZstandardNativeMethods.ZSTD_flushStream(zcs, buffer));
            }

            using (var operationContext = InitializeOperationContext(compressedBufferInfo))
            {
                yield return ProcessCompressedOutput(operationContext, (zcs, buffer) => ZstandardNativeMethods.ZSTD_endStream(zcs, buffer));
            }

            int ProcessCompressedOutput(OperationContext context, Action<IntPtr, NativeBufferInfo> outputAction)
            {
                ConfigureNativeBuffer(
                    _outputNativeBuffer,
                    context.CompressedPinnedBuffer,
                    _recommendedZstreamOutputSize);

                outputAction(_zstreamPointer, _outputNativeBuffer);

                return (int)_outputNativeBuffer.Position.ToUInt32();
            }
        }

        // private methods
        private void ConfigureNativeBuffer(NativeBufferInfo buffer, PinnedBuffer pinnedBuffer, uint size)
        {
            buffer.DataPointer = pinnedBuffer?.IntPtr ?? IntPtr.Zero;
            buffer.Size = new UIntPtr(size);
            buffer.Position = UIntPtr.Zero;
        }

        private void InitializeIfNotAlreadyInitialized()
        {
            if (!_operationInitialized)
            {
                _operationInitialized = true;

                switch (_compressionMode)
                {
                    case CompressionMode.Compress:
                        ZstandardNativeMethods.ZSTD_initCStream(_zstreamPointer, _compressionLevel); // start a new compression operation
                        break;
                    case CompressionMode.Decompress:
                        ZstandardNativeMethods.ZSTD_initDStream(_zstreamPointer); // start a new decompression operation
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
                try
                {
                    _uncompressedPinnedBuffer?.Dispose();
                }
                finally
                {
                    _compressedPinnedBuffer.Dispose(); // ensure that both dispose methods were called
                }
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
