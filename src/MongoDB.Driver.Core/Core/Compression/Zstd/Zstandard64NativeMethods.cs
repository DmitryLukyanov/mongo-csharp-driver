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
using System.IO;
using System.Runtime.InteropServices;
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Zstd
{
    internal class Zstandard64NativeMethods
    {
        // private static fields
        private static readonly Lazy<LibraryLoader> __libraryLoader;
        private static readonly Lazy<Delegates.ZSTD_CStreamInSize> __ZSTD_CStreamInSize;
        private static readonly Lazy<Delegates.ZSTD_CStreamOutSize> __ZSTD_CStreamOutSize;
        private static readonly Lazy<Delegates.ZSTD_createCStream> __ZSTD_createCStream;

        private static readonly Lazy<Delegates.ZSTD_DStreamInSize> __ZSTD_DStreamInSize;
        private static readonly Lazy<Delegates.ZSTD_DStreamOutSize> __ZSTD_DStreamOutSize;
        private static readonly Lazy<Delegates.ZSTD_createDStream> __ZSTD_createDStream;

        private static readonly Lazy<Delegates.ZSTD_maxCLevel> __ZSTD_maxCLevel;

        private static readonly Lazy<Delegates.ZSTD_flushStream> __ZSTD_flushStream;
        private static readonly Lazy<Delegates.ZSTD_endStream> __ZSTD_endStream;
        private static readonly Lazy<Delegates.ZSTD_freeCStream> __ZSTD_freeCStream;
        private static readonly Lazy<Delegates.ZSTD_freeDStream> __ZSTD_freeDStream;

        private static readonly Lazy<Delegates.ZSTD_initDStream> __ZSTD_initDStream;
        private static readonly Lazy<Delegates.ZSTD_decompressStream> __ZSTD_decompressStream;

        private static readonly Lazy<Delegates.ZSTD_initCStream> __ZSTD_initCStream;
        private static readonly Lazy<Delegates.ZSTD_compressStream> __ZSTD_compressStream;

        private static readonly Lazy<Delegates.ZSTD_isError> __ZSTD_isError;
        private static readonly Lazy<Delegates.ZSTD_getErrorName> __ZSTD_getErrorName;


        // static constructor
        static Zstandard64NativeMethods()
        {
            var zstandardLocator = new ZstandardLocator();
            __libraryLoader = new Lazy<LibraryLoader>(() => new LibraryLoader(zstandardLocator), isThreadSafe: true);

            __ZSTD_CStreamInSize = CreateLazyForDelegate<Delegates.ZSTD_CStreamInSize>(nameof(ZSTD_CStreamInSize));
            __ZSTD_CStreamOutSize = CreateLazyForDelegate<Delegates.ZSTD_CStreamOutSize>(nameof(ZSTD_CStreamOutSize));
            __ZSTD_createCStream = CreateLazyForDelegate<Delegates.ZSTD_createCStream>(nameof(ZSTD_createCStream));

            __ZSTD_DStreamInSize = CreateLazyForDelegate<Delegates.ZSTD_DStreamInSize>(nameof(ZSTD_DStreamInSize));
            __ZSTD_DStreamOutSize = CreateLazyForDelegate<Delegates.ZSTD_DStreamOutSize>(nameof(ZSTD_DStreamOutSize));
            __ZSTD_createDStream = CreateLazyForDelegate<Delegates.ZSTD_createDStream>(nameof(ZSTD_createDStream));

            __ZSTD_maxCLevel = CreateLazyForDelegate<Delegates.ZSTD_maxCLevel>(nameof(ZSTD_maxCLevel));

            __ZSTD_flushStream = CreateLazyForDelegate<Delegates.ZSTD_flushStream>(nameof(ZSTD_flushStream));
            __ZSTD_endStream = CreateLazyForDelegate<Delegates.ZSTD_endStream>(nameof(ZSTD_endStream));
            __ZSTD_freeCStream = CreateLazyForDelegate<Delegates.ZSTD_freeCStream>(nameof(ZSTD_freeCStream));
            __ZSTD_freeDStream = CreateLazyForDelegate<Delegates.ZSTD_freeDStream>(nameof(ZSTD_freeDStream));

            __ZSTD_initDStream = CreateLazyForDelegate<Delegates.ZSTD_initDStream>(nameof(ZSTD_initDStream));
            __ZSTD_decompressStream = CreateLazyForDelegate<Delegates.ZSTD_decompressStream>(nameof(ZSTD_decompressStream));

            __ZSTD_initCStream = CreateLazyForDelegate<Delegates.ZSTD_initCStream>(nameof(ZSTD_initCStream));
            __ZSTD_compressStream = CreateLazyForDelegate<Delegates.ZSTD_compressStream>(nameof(ZSTD_compressStream));

            __ZSTD_isError = CreateLazyForDelegate<Delegates.ZSTD_isError>(nameof(ZSTD_isError));
            __ZSTD_getErrorName = CreateLazyForDelegate<Delegates.ZSTD_getErrorName>(nameof(ZSTD_getErrorName));
        }

        // public static methods
        public static UIntPtr ZSTD_CStreamInSize()
        {
            return __ZSTD_CStreamInSize.Value();
        }

        public static UIntPtr ZSTD_CStreamOutSize()
        {
            return __ZSTD_CStreamOutSize.Value();
        }

        public static IntPtr ZSTD_createCStream()
        {
            return __ZSTD_createCStream.Value();
        }

        public static UIntPtr ZSTD_DStreamInSize()
        {
            return __ZSTD_DStreamInSize.Value();
        }

        public static UIntPtr ZSTD_DStreamOutSize()
        {
            return __ZSTD_DStreamOutSize.Value();
        }

        public static IntPtr ZSTD_createDStream()
        {
            return __ZSTD_createDStream.Value();
        }

        public static int ZSTD_maxCLevel()
        {
            return __ZSTD_maxCLevel.Value();
        }

        public static UIntPtr ZSTD_flushStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer)
        {
            var result = __ZSTD_flushStream.Value(zcs, outputBuffer);
            ThrowIfError(result);
            return result;
        }

        public static UIntPtr ZSTD_endStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer)
        {
            var result = __ZSTD_endStream.Value(zcs, outputBuffer);
            ThrowIfError(result);
            return result;
        }

        public static UIntPtr ZSTD_freeCStream(IntPtr zcs)
        {
            return __ZSTD_freeCStream.Value(zcs);
        }

        public static UIntPtr ZSTD_freeDStream(IntPtr zds)
        {
            return __ZSTD_freeDStream.Value(zds);
        }

        public static UIntPtr ZSTD_initDStream(IntPtr zds)
        {
            return __ZSTD_initDStream.Value(zds);
        }

        public static UIntPtr ZSTD_decompressStream(IntPtr zds, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo inputBuffer)
        {
            var result = __ZSTD_decompressStream.Value(zds, outputBuffer, inputBuffer);
            ThrowIfError(result);
            return result;
        }

        public static UIntPtr ZSTD_initCStream(IntPtr zcs, int compressionLevel)
        {
            var result = __ZSTD_initCStream.Value(zcs, compressionLevel);
            ThrowIfError(result);
            return result;
        }

        public static UIntPtr ZSTD_compressStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo inputBuffer)
        {
            var result = __ZSTD_compressStream.Value(zcs, outputBuffer, inputBuffer);
            ThrowIfError(result);
            return result;
        }

        // private static methods
        private static Lazy<TDelegate> CreateLazyForDelegate<TDelegate>(string name)
        {
            return new Lazy<TDelegate>(() => __libraryLoader.Value.GetDelegate<TDelegate>(name), isThreadSafe: true);
        }

        private static void ThrowIfError(UIntPtr code)
        {
            if (Zstandard64NativeMethods.ZSTD_isError(code))
            {
                var errorPtr = Zstandard64NativeMethods.ZSTD_getErrorName(code);
                var errorMsg = Marshal.PtrToStringAnsi(errorPtr);
                throw new IOException(errorMsg);
            }
        }

        private static bool ZSTD_isError(UIntPtr code)
        {
            return __ZSTD_isError.Value(code);
        }

        private static IntPtr ZSTD_getErrorName(UIntPtr code)
        {
            return __ZSTD_getErrorName.Value(code);
        }

        // nested types
        private class Delegates
        {
            public delegate UIntPtr ZSTD_CStreamInSize();
            public delegate UIntPtr ZSTD_CStreamOutSize();
            public delegate IntPtr ZSTD_createCStream();

            public delegate UIntPtr ZSTD_DStreamInSize();
            public delegate UIntPtr ZSTD_DStreamOutSize();
            public delegate IntPtr ZSTD_createDStream();

            public delegate int ZSTD_maxCLevel();

            public delegate UIntPtr ZSTD_flushStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer);
            public delegate UIntPtr ZSTD_endStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer);
            public delegate UIntPtr ZSTD_freeCStream(IntPtr zcs);
            public delegate UIntPtr ZSTD_freeDStream(IntPtr zds);

            public delegate UIntPtr ZSTD_initDStream(IntPtr zds);

            public delegate UIntPtr ZSTD_decompressStream(IntPtr zds, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo inputBuffer);

            public delegate UIntPtr ZSTD_initCStream(IntPtr zcs, int compressionLevel);

            public delegate UIntPtr ZSTD_compressStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo outputBuffer, [MarshalAs(UnmanagedType.LPStruct)] NativeBufferInfo inputBuffer);

            public delegate bool ZSTD_isError(UIntPtr code);
            public delegate IntPtr ZSTD_getErrorName(UIntPtr code);
        }

        private class ZstandardLocator : RelativeLibraryLocatorBase
        {
            public override string GetLibraryRelativePath(SupportedPlatform currentPlatform)
            {
                switch (currentPlatform)
                {
                    case SupportedPlatform.Windows:
                        return @"..\..\x64\native\windows\libzstd.dll";
                    case SupportedPlatform.Linux: // TODO: add support for Linux and MacOS later
                    case SupportedPlatform.MacOS:
                    default:
                        throw new InvalidOperationException($"Zstandard is not supported on the current platform: {currentPlatform}.");
                }
            }
        }
    }
}
