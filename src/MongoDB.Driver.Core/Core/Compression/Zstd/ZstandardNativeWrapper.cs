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

        private readonly Buffer _inputNativeBuffer = new Buffer();
        private readonly Buffer _outputNativeBuffer = new Buffer();

        private bool _operationInitialized;

        // calculate in constructor
        private readonly uint _recommendedZstreamInputSize;
        private readonly uint _recommendedZstreamOutputSize;
        // initialize in constructor
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
            int outputSize,
            int inputSize,
            out int outputBufferPosition,
            out int inputBufferPosition)
        {
            Ensure.IsNotNull(operationContext, nameof(operationContext));

            InitializeIfNotAlreadyInitialized();

            // compressed data
            ConfigureNativeBuffer(
                _outputNativeBuffer,
                operationContext.InternalDataPinnedBuffer, // operation result
                (uint)outputSize);

            // uncompressed data
            ConfigureNativeBuffer(
                _inputNativeBuffer,
                operationContext.ExternalArgumentPinnedBuffer,
                (uint)inputSize);

            // compress _inputNativeBuffer to _outputNativeBuffer
            ZstandardNativeMethods.ZSTD_compressStream(
                _zstreamPointer,
                outputBuffer: _outputNativeBuffer,
                inputBuffer: _inputNativeBuffer);

            outputBufferPosition = (int)_outputNativeBuffer.Position.ToUInt32();

            // calculate progress in _inputNativeBuffer
            inputBufferPosition = (int)_inputNativeBuffer.Position.ToUInt32();
            operationContext.ExternalArgumentPinnedBuffer.Offset += inputBufferPosition;
        }

        public void Decompress(
            OperationContext operationContext,
            int inputSize,
            int outputSize,
            out int inputBufferPosition,
            out int outputBufferPosition)
        {
            Ensure.IsNotNull(operationContext, nameof(operationContext));

            InitializeIfNotAlreadyInitialized();

            // compressed data
            ConfigureNativeBuffer(
                _inputNativeBuffer,
                inputSize <= 0 ? null : operationContext.InternalDataPinnedBuffer,
                inputSize <= 0 ? 0 : (uint)inputSize);

            // uncompressed data
            ConfigureNativeBuffer(
                _outputNativeBuffer,
                operationContext.ExternalArgumentPinnedBuffer, // operation result
                (uint)outputSize);

            // decompress _inputNativeBuffer to _outputNativeBuffer
            ZstandardNativeMethods.ZSTD_decompressStream(
                _zstreamPointer,
                outputBuffer: _outputNativeBuffer,
                inputBuffer: _inputNativeBuffer);

            // calculate progress in _outputNativeBuffer
            outputBufferPosition = (int)_outputNativeBuffer.Position.ToUInt32();
            inputBufferPosition = (int)_inputNativeBuffer.Position.ToUInt32();

            operationContext.ExternalArgumentPinnedBuffer.Offset += outputBufferPosition;
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
            BufferInfo internalDataInfo,
            BufferInfo externalArgumentBufferInfo = null)
        {
            var internalDataPinnedBuffer = new PinnedBuffer(internalDataInfo.Bytes, internalDataInfo.Offset);
            PinnedBuffer externalArgumentPinnedBuffer = null;
            if (externalArgumentBufferInfo != null)
            {
                externalArgumentPinnedBuffer = new PinnedBuffer(externalArgumentBufferInfo.Bytes, externalArgumentBufferInfo.Offset);
            }

            return new OperationContext(externalArgumentPinnedBuffer, internalDataPinnedBuffer);
        }

        public IEnumerable<int> FlushBySteps(byte[] dataBytes)
        {
            if (_compressionMode != CompressionMode.Compress)
            {
                throw new InvalidDataException("FlushBySteps must be called only from Compress mode.");
            }

            using (var operationContext = InitializeOperationContext(new BufferInfo(dataBytes, 0)))
            {
                yield return ProcessCompressedOutput(operationContext, (zcs, buffer) => ZstandardNativeMethods.ZSTD_flushStream(zcs, buffer));
            }

            using (var operationContext = InitializeOperationContext(new BufferInfo(dataBytes, 0)))
            {
                yield return ProcessCompressedOutput(operationContext, (zcs, buffer) => ZstandardNativeMethods.ZSTD_endStream(zcs, buffer));
            }

            int ProcessCompressedOutput(OperationContext context, Action<IntPtr, Buffer> outputAction)
            {
                ConfigureNativeBuffer(
                    _outputNativeBuffer,
                    context.InternalDataPinnedBuffer,
                    _recommendedZstreamOutputSize);

                outputAction(_zstreamPointer, _outputNativeBuffer);

                return (int)_outputNativeBuffer.Position.ToUInt32();
            }
        }

        // private methods
        private void ConfigureNativeBuffer(Buffer buffer, PinnedBuffer pinnedBuffer, uint size)
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
            private readonly PinnedBuffer _externalArgumentPinnedBuffer;
            private readonly PinnedBuffer _internalDataPinnedBuffer;

            public OperationContext(PinnedBuffer externalArgumentPinnedBuffer, PinnedBuffer internalDataPinnedBuffer)
            {
                _externalArgumentPinnedBuffer = externalArgumentPinnedBuffer; // can be null
                _internalDataPinnedBuffer = Ensure.IsNotNull(internalDataPinnedBuffer, nameof(internalDataPinnedBuffer));
            }

            public PinnedBuffer ExternalArgumentPinnedBuffer => _externalArgumentPinnedBuffer;
            public PinnedBuffer InternalDataPinnedBuffer => _internalDataPinnedBuffer;

            // public methods
            public void Dispose()
            {
                try
                {
                    _externalArgumentPinnedBuffer?.Dispose();
                }
                finally
                {
                    _internalDataPinnedBuffer.Dispose(); // ensure that both dispose methods were called
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
