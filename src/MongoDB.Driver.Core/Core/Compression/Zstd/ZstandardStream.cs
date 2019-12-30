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
using System.Buffers;
using System.IO;
using System.IO.Compression;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Compression.Zstd
{
    internal class ZstandardStream : Stream
    {
        private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;
        private readonly Stream _compressedStream; // input for decompress and output for compress
        private readonly bool _leaveOpen;

        private readonly StreamReadHelper _streamReadHelper;
        private readonly StreamWriteHelper _streamWriteHelper;
        private readonly CompressionMode _compressionMode;

        private bool _disposed;
        private readonly NativeWrapper _nativeWrapper;

        public ZstandardStream(
            Stream compressedStream,
            CompressionMode compressionMode,
            bool leaveOpen = true,
            Optional<int> compressionLevel = default)
        {
            _compressedStream = Ensure.IsNotNull(compressedStream, nameof(compressedStream));
            _compressionMode = EnsureCompressionModeIsValid(compressionMode);
            _leaveOpen = leaveOpen;

            _nativeWrapper = new NativeWrapper(_compressionMode, EnsureCompressionLevelIsValid(compressionLevel));
            switch (_compressionMode)
            {
                case CompressionMode.Compress:
                    _streamWriteHelper = new StreamWriteHelper(
                        compressedStream: compressedStream,
                        dataBuffer: _arrayPool.Rent(_nativeWrapper.RecommendedOutputSize));
                    break;
                case CompressionMode.Decompress:
                    _streamReadHelper = new StreamReadHelper(
                        compressedStream: compressedStream,
                        dataBuffer: _arrayPool.Rent(_nativeWrapper.RecommendedInputSize));
                    break;
            }
        }

        public override bool CanRead => _compressedStream.CanRead &&
                                        _compressionMode == CompressionMode.Decompress;

        public override bool CanWrite => _compressedStream.CanWrite &&
                                         _compressionMode == CompressionMode.Compress;

        public override bool CanSeek => false;

        public override long Length => throw new NotSupportedException();

        public static int MaxCompressionLevel => NativeWrapper.MaxCompressionLevel; // maximum compression level available

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (!_disposed)
            {
                switch (_compressionMode)
                {
                    case CompressionMode.Compress:
                        try
                        {
                            foreach (var outputBufferPosition in _nativeWrapper.RunFlushBySteps(_streamWriteHelper.DataBuffer))
                            {
                                _streamWriteHelper.WriteBufferToCompressedStream(outputBufferPosition);
                            }
                        }
                        finally
                        {
                            _nativeWrapper.Dispose();
                            _compressedStream.Flush();
                            _arrayPool.Return(_streamWriteHelper.DataBuffer);
                        }
                        break;
                    case CompressionMode.Decompress:
                        _nativeWrapper.Dispose();
                        _arrayPool.Return(_streamReadHelper.DataBuffer);
                        break;
                }

                if (!_leaveOpen)
                {
                    _compressedStream.Dispose();
                }
                _disposed = true;
            }
        }

        public override void Flush()
        {
            throw new NotSupportedException("Use Dispose instead.");
        }

        public override int Read(byte[] uncompressedOutputBytes, int outputOffset, int count) // Decompress
        {
            if (!CanRead) throw new InvalidDataException("Read is not accessible.");

            var internalDataBuffer = new BufferInfo(
                _streamReadHelper.DataBuffer,
                _streamReadHelper.StartDataBufferPosition);
            var outputBuffer = new BufferInfo(uncompressedOutputBytes, outputOffset);
            using (var operationContext = _nativeWrapper.InitializeOperation(internalDataBuffer, outputBuffer))
            {
                var length = 0; // the result

                while (count > 0)
                {
                    operationContext.InternalDataPinnedBuffer.Offset = 
                        _streamReadHelper.TryReadCompressedStreamToBufferAndUpdatePosition(
                        currentDataPosition: operationContext.InternalDataPinnedBuffer.Offset, // 0 - if it's the first method call
                        _nativeWrapper.RecommendedInputSize,
                        out int remainingBufferSize);

                    // decompress input to output
                    _nativeWrapper.Decompress(
                        operationContext,
                        inputSize: remainingBufferSize,
                        outputSize: count,
                        out int inputBufferPosition,
                        out int outputBufferPosition);

                    if (_streamReadHelper.ShouldStopNewReadingAttempt(outputBufferPosition))
                    {
                        break;
                    }

                    // calculate progress in input buffer
                    operationContext.InternalDataPinnedBuffer.Offset += inputBufferPosition;

                    length += outputBufferPosition;
                    count -= outputBufferPosition;
                }

                // save the data position for next method calls
                _streamReadHelper.StartDataBufferPosition = operationContext.InternalDataPinnedBuffer.Offset;

                return length;
            }
        }

        public override void Write(byte[] uncompressedInputBytes, int inputOffset, int count) // Compress
        {
            if (!CanWrite) throw new InvalidDataException("Write is not accessible.");

            var dataBufferInfo = new BufferInfo(
                _streamWriteHelper.DataBuffer, // empty, since Write processes the whole message in one try
                offset: 0);
            var inputInfo = new BufferInfo(uncompressedInputBytes, inputOffset);

            using (var operationContext = _nativeWrapper.InitializeOperation(dataBufferInfo, inputInfo))
            {
                while (count > 0)
                {
                    var currentAttemptSize = Math.Min(count, _nativeWrapper.RecommendedInputSize);

                    // compress input to output
                    _nativeWrapper.Compress(
                        operationContext,
                        outputSize: _nativeWrapper.RecommendedOutputSize,
                        inputSize: currentAttemptSize,
                        out var outputBufferPosition,
                        out var inputBufferPosition);

                    _streamWriteHelper.WriteBufferToCompressedStream(offset: outputBufferPosition);

                    // calculate progress in input buffer
                    count -= inputBufferPosition;
                }
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        // private methods
        private int EnsureCompressionLevelIsValid(Optional<int> compressionLevel)
        {
            if (compressionLevel.HasValue && _compressionMode != CompressionMode.Compress)
            {
                throw new ArgumentException("Compression level can be specified only in Compress mode");
            }

            if (compressionLevel.HasValue && compressionLevel.Value > NativeWrapper.MaxCompressionLevel) // add validation on min level
            {
                throw new ArgumentOutOfRangeException(nameof(compressionLevel));
            }

            return compressionLevel.WithDefault(6); // 6 - is default value
        }

        private CompressionMode EnsureCompressionModeIsValid(CompressionMode compressionMode)
        {
            if (compressionMode != CompressionMode.Compress && compressionMode != CompressionMode.Decompress)
            {
                throw new ArgumentException($"Invalid compression mode {compressionMode}.");
            }
            return compressionMode;
        }

        // nested types
        private class StreamReadHelper
        {
            private readonly Stream _compressedStream;
            private readonly byte[] _dataBuffer;
            private readonly ReadingState _readingState;

            public StreamReadHelper(
                Stream compressedStream,
                byte[] dataBuffer)
            {
                _compressedStream = Ensure.IsNotNull(compressedStream, nameof(compressedStream));
                _dataBuffer = Ensure.IsNotNull(dataBuffer, nameof(dataBuffer));
                _readingState = new ReadingState();
            }

            public byte[] DataBuffer => _dataBuffer; // will be implicitly updated via calls of native methods

            public int StartDataBufferPosition
            {
                get => _readingState.StartDataPosition;
                set => _readingState.StartDataPosition = value;
            }

            // public methods
            public bool ShouldStopNewReadingAttempt(int outputBufferPosition)
            {
                if (outputBufferPosition == 0) // 0 - when a frame is completely decoded and fully flushed
                {
                    // the internal buffer is depleted, we're either done
                    if (_readingState.IsDataDepleted)
                    {
                        return true;
                    }

                    // or we need more bytes
                    _readingState.SkipDataReading = false;
                }

                return false;
            }

            public int TryReadCompressedStreamToBufferAndUpdatePosition(int currentDataPosition, int recommendedInputSize, out int remainingSize)
            {
                remainingSize = _readingState.LastReadDataSize - currentDataPosition;

                // read data from inputBytes _compressedStream 
                if (remainingSize <= 0 && !_readingState.IsDataDepleted && !_readingState.SkipDataReading)
                {
                    var readSize = _compressedStream.Read(_dataBuffer, 0, recommendedInputSize);
                    UpdateStateAfterReading(readSize, out remainingSize);
                    return 0; // reset current position
                }
                return currentDataPosition;
            }

            // private methods
            private void UpdateStateAfterReading(int readSize, out int remainingSize)
            {
                _readingState.LastReadDataSize = readSize;
                _readingState.IsDataDepleted = readSize <= 0;

                // skip _compressedStream.Read until the internal buffer is depleted
                // avoids a Read timeout for applications that know the exact number of bytes in the _compressedStream
                _readingState.SkipDataReading = true;
                remainingSize = _readingState.IsDataDepleted ? 0 : _readingState.LastReadDataSize;
            }

            // nested types
            private class ReadingState
            {
                /// <summary>
                /// Shows whether the last attempt to read data from the stream contained records or no. <c>false</c> if no.
                /// </summary>
                public bool IsDataDepleted { get; set; }
                /// <summary>
                /// The size of the last fetched data from stream.
                /// </summary>
                public int LastReadDataSize { get; set; }
                /// <summary>
                /// Determinate whether _compressedStream.Read should be skipped until the internal buffer is depleted.
                /// </summary>
                public bool SkipDataReading { get; set; }
                /// <summary>
                /// Used to save position between different Read calls.
                /// </summary>
                public int StartDataPosition { get; set; }
            }
        }

        private class StreamWriteHelper
        {
            private readonly Stream _compressedStream;
            private readonly byte[] _dataBuffer;

            public StreamWriteHelper(Stream compressedStream, byte[] dataBuffer)
            {
                _compressedStream = Ensure.IsNotNull(compressedStream, nameof(compressedStream));
                _dataBuffer = dataBuffer;
            }

            public byte[] DataBuffer => _dataBuffer; // will be implicitly updated via calls of native methods

            public void WriteBufferToCompressedStream(int offset)
            {
                _compressedStream.Write(_dataBuffer, 0, offset);
            }
        }
    }
}
