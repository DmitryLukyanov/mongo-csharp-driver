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
using System.Runtime.InteropServices;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Compression.Zstd
{
    internal class ZstandardStream : Stream
    {
        private readonly int _compressionLevel = 6;
        private Stream stream;
        private CompressionMode mode;
        private Boolean leaveOpen; // todo remove
#pragma warning disable 649 //todo
        private Boolean isClosed;
#pragma warning restore 649
        private Boolean isDisposed;
        private Boolean isInitialized;

        private IntPtr zstream;
        private uint zstreamInputSize;
        private uint zstreamOutputSize;

        private byte[] data;
        private bool dataDepleted;
        private bool dataSkipRead;
        private int dataPosition;
        private int dataSize;

        private readonly Buffer outputBuffer = new Buffer();
        private readonly Buffer inputBuffer = new Buffer();
        private readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Shared; //todo

        public ZstandardStream(Stream stream, CompressionMode mode, bool leaveOpen = false)
        {
            this.stream = Ensure.IsNotNull(stream, nameof(stream));
            this.mode = mode;
            this.leaveOpen = leaveOpen;

            if (mode == CompressionMode.Compress)
            {
                zstreamInputSize = ZstandardAdapter.ZSTD_CStreamInSize();
                zstreamOutputSize = ZstandardAdapter.ZSTD_CStreamOutSize();
                zstream = ZstandardAdapter.ZSTD_createCStream(); // todo
                data = arrayPool.Rent((int)this.zstreamOutputSize);
            }

            if (mode == CompressionMode.Decompress)
            {
                zstreamInputSize = ZstandardAdapter.ZSTD_DStreamInSize();
                zstreamOutputSize = ZstandardAdapter.ZSTD_DStreamOutSize();
                zstream = ZstandardAdapter.ZSTD_createDStream();
                data = arrayPool.Rent((int)zstreamInputSize);
            }
        }

        public ZstandardStream(Stream stream, int compressionLevel, bool leaveOpen = false)
            : this(stream, CompressionMode.Compress, leaveOpen)
        {
            _compressionLevel = compressionLevel;
        }

        public static int MaxCompressionLevel => ZstandardAdapter.ZSTD_maxCLevel();

        public int CompressionLevel => _compressionLevel;

        //public ZstandardDictionary CompressionDictionary { get; set; } = null;

        /// <summary>
        /// Gets whether the current stream supports reading.
        /// </summary>
        public override bool CanRead => this.stream.CanRead && this.mode == CompressionMode.Decompress;

        /// <summary>
        ///  Gets whether the current stream supports writing.
        /// </summary>
        public override bool CanWrite => this.stream.CanWrite && this.mode == CompressionMode.Compress;

        /// <summary>
        ///  Gets whether the current stream supports seeking.
        /// </summary>
        public override bool CanSeek => false;

        /// <summary>
        /// Gets the length in bytes of the stream.
        /// </summary>
        public override long Length => throw new NotSupportedException();

        /// <summary>
        /// Gets or sets the position within the current stream.
        /// </summary>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (this.isDisposed == false)
            {
                if (!this.isClosed) ReleaseResources(flushStream: false);
                this.arrayPool.Return(this.data, clearArray: false);
                this.isDisposed = true;
                this.data = null;
            }
        }

#if NET452
        public override void Close()
#else
        public void Close()
#endif
        {
            if (this.isClosed) return;

            try
            {
                ReleaseResources(flushStream: true);
            }
            finally
            {
                this.isClosed = true;
#if NET452
                base.Close();
#else
                base.Dispose();
#endif
            }
        }

        private void ReleaseResources(bool flushStream)
        {
            if (this.mode == CompressionMode.Compress)
            {
                try
                {
                    if (flushStream)
                    {
                        this.ProcessStream((zcs, buffer) => ZstandardAdapter.ThrowIfError(ZstandardAdapter.ZSTD_flushStream(zcs, buffer)));
                        this.ProcessStream((zcs, buffer) => ZstandardAdapter.ThrowIfError(ZstandardAdapter.ZSTD_endStream(zcs, buffer)));
                        this.stream.Flush();
                    }
                }
                finally
                {
                    ZstandardAdapter.ZSTD_freeCStream(zstream);
#if NET452
                    if (!this.leaveOpen) this.stream.Close();
#else
                    if (!this.leaveOpen) this.stream.Dispose();
#endif
                }
            }
            else if (this.mode == CompressionMode.Decompress)
            {
                ZstandardAdapter.ZSTD_freeDStream(this.zstream);
#if NET452
                if (!this.leaveOpen) this.stream.Close();
#else
                if (!this.leaveOpen) this.stream.Dispose();
#endif
            }
        }

        public override void Flush()
        {
            if (this.mode == CompressionMode.Compress)
            {
                this.ProcessStream((zcs, buffer) => ZstandardAdapter.ThrowIfError(ZstandardAdapter.ZSTD_flushStream(zcs, buffer)));
                this.stream.Flush();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (this.CanRead == false) throw new NotSupportedException();

            // prevent the buffers from being moved around by the garbage collector
            var alloc1 = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            var alloc2 = GCHandle.Alloc(this.data, GCHandleType.Pinned);

            try
            {
                var length = 0;

                if (this.isInitialized == false)
                {
                    this.isInitialized = true;

                    //var result = this.CompressionDictionary == null
                    //    ? Interop.ZSTD_initDStream(this.zstream)
                    //    : Interop.ZSTD_initDStream_usingDDict(this.zstream, this.CompressionDictionary.GetDecompressionDictionary());
                    ZstandardAdapter.ZSTD_initDStream(this.zstream);
                }

                while (count > 0)
                {
                    var inputSize = this.dataSize - this.dataPosition;

                    // read data from input stream 
                    if (inputSize <= 0 && !this.dataDepleted && !this.dataSkipRead)
                    {
                        this.dataSize = this.stream.Read(this.data, 0, (int)this.zstreamInputSize);
                        this.dataDepleted = this.dataSize <= 0;
                        this.dataPosition = 0;
                        inputSize = this.dataDepleted ? 0 : this.dataSize;

                        // skip stream.Read until the internal buffer is depleted
                        // avoids a Read timeout for applications that know the exact number of bytes in the stream
                        this.dataSkipRead = true;
                    }

                    // configure the inputBuffer
                    this.inputBuffer.Data = inputSize <= 0 ? IntPtr.Zero : Marshal.UnsafeAddrOfPinnedArrayElement(this.data, this.dataPosition);
                    this.inputBuffer.Size = inputSize <= 0 ? UIntPtr.Zero : new UIntPtr((uint)inputSize);
                    this.inputBuffer.Position = UIntPtr.Zero;

                    // configure the outputBuffer
                    this.outputBuffer.Data = Marshal.UnsafeAddrOfPinnedArrayElement(buffer, offset);
                    this.outputBuffer.Size = new UIntPtr((uint)count);
                    this.outputBuffer.Position = UIntPtr.Zero;

                    // decompress inputBuffer to outputBuffer
                    ZstandardAdapter.ThrowIfError(ZstandardAdapter.ZSTD_decompressStream(this.zstream, this.outputBuffer, this.inputBuffer));

                    // calculate progress in outputBuffer
                    var outputBufferPosition = (int)this.outputBuffer.Position.ToUInt32();
                    if (outputBufferPosition == 0)
                    {
                        // the internal buffer is depleted, we're either done
                        if (this.dataDepleted) break;

                        // or we need more bytes
                        this.dataSkipRead = false;
                    }
                    length += outputBufferPosition;
                    offset += outputBufferPosition;
                    count -= outputBufferPosition;

                    // calculate progress in inputBuffer
                    var inputBufferPosition = (int)inputBuffer.Position.ToUInt32();
                    this.dataPosition += inputBufferPosition;
                }

                return length;
            }
            finally
            {
                alloc1.Free();
                alloc2.Free();
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (this.CanWrite == false) throw new NotSupportedException();

            // prevent the buffers from being moved around by the garbage collector
            var alloc1 = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            var alloc2 = GCHandle.Alloc(this.data, GCHandleType.Pinned);

            try
            {
                if (this.isInitialized == false)
                {
                    this.isInitialized = true;

                    //var result = this.CompressionDictionary == null
                    //    ? Interop.ZSTD_initCStream(this.zstream, this.CompressionLevel)
                    //    : Interop.ZSTD_initCStream_usingCDict(this.zstream, this.CompressionDictionary.GetCompressionDictionary(this.CompressionLevel));
                    var result = ZstandardAdapter.ZSTD_initCStream(this.zstream, this.CompressionLevel);

                    ZstandardAdapter.ThrowIfError(result);
                }

                while (count > 0)
                {
                    var inputSize = Math.Min((uint)count, this.zstreamInputSize);

                    // configure the outputBuffer
                    this.outputBuffer.Data = Marshal.UnsafeAddrOfPinnedArrayElement(this.data, 0);
                    this.outputBuffer.Size = new UIntPtr(this.zstreamOutputSize);
                    this.outputBuffer.Position = UIntPtr.Zero;

                    // configure the inputBuffer
                    this.inputBuffer.Data = Marshal.UnsafeAddrOfPinnedArrayElement(buffer, offset);
                    this.inputBuffer.Size = new UIntPtr((uint)inputSize);
                    this.inputBuffer.Position = UIntPtr.Zero;

                    // compress inputBuffer to outputBuffer
                    ZstandardAdapter.ThrowIfError(ZstandardAdapter.ZSTD_compressStream(this.zstream, this.outputBuffer, this.inputBuffer));

                    // write data to output stream
                    var outputBufferPosition = (int)this.outputBuffer.Position.ToUInt32();
                    this.stream.Write(this.data, 0, outputBufferPosition);

                    // calculate progress in inputBuffer
                    var inputBufferPosition = (int)this.inputBuffer.Position.ToUInt32();
                    offset += inputBufferPosition;
                    count -= inputBufferPosition;
                }
            }
            finally
            {
                alloc1.Free();
                alloc2.Free();
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

        private void ProcessStream(Action<IntPtr, Buffer> outputAction)
        {
            var alloc = GCHandle.Alloc(this.data, GCHandleType.Pinned);

            try
            {
                this.outputBuffer.Data = Marshal.UnsafeAddrOfPinnedArrayElement(this.data, 0);
                this.outputBuffer.Size = new UIntPtr(this.zstreamOutputSize);
                this.outputBuffer.Position = UIntPtr.Zero;

                outputAction(this.zstream, this.outputBuffer);

                var outputBufferPosition = (int)this.outputBuffer.Position.ToUInt32();
                this.stream.Write(this.data, 0, outputBufferPosition);
            }
            finally
            {
                alloc.Free();
            }
        }
    }
}
