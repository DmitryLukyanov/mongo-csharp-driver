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

namespace MongoDB.Driver.Core.Compression.Zstd
{
    // todo: reimplement
    internal static class ZstandardAdapter
    {
        public static uint ZSTD_CStreamInSize()
        {
            return ZstandardNativeMethods.ZSTD_CStreamInSize().ToUInt32();
        }

        public static uint ZSTD_CStreamOutSize()
        {
            return ZstandardNativeMethods.ZSTD_CStreamOutSize().ToUInt32();
        }

        public static IntPtr ZSTD_createCStream()
        {
            return ZstandardNativeMethods.ZSTD_createCStream();
        }

        public static uint ZSTD_DStreamInSize()
        {
            return ZstandardNativeMethods.ZSTD_DStreamInSize().ToUInt32();
        }

        public static uint ZSTD_DStreamOutSize()
        {
            return ZstandardNativeMethods.ZSTD_DStreamOutSize().ToUInt32();
        }

        public static IntPtr ZSTD_createDStream()
        {
            return ZstandardNativeMethods.ZSTD_createDStream();
        }

        public static int ZSTD_maxCLevel()
        {
            return ZstandardNativeMethods.ZSTD_maxCLevel();
        }

        public static UIntPtr ZSTD_flushStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] Buffer outputBuffer)
        {
            return ZstandardNativeMethods.ZSTD_flushStream(zcs, outputBuffer);
        }

        public static UIntPtr ZSTD_endStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] Buffer outputBuffer)
        {
            return ZstandardNativeMethods.ZSTD_endStream(zcs, outputBuffer);
        }

        public static UIntPtr ZSTD_freeCStream(IntPtr zcs)
        {
            return ZstandardNativeMethods.ZSTD_freeCStream(zcs);
        }

        public static UIntPtr ZSTD_freeDStream(IntPtr zds)
        {
            return ZstandardNativeMethods.ZSTD_freeDStream(zds);
        }

        public static UIntPtr ZSTD_initDStream(IntPtr zds)
        {
            return ZstandardNativeMethods.ZSTD_initDStream(zds);
        }

        public static UIntPtr ZSTD_decompressStream(IntPtr zds, Buffer outputBuffer, Buffer inputBuffer)
        {
            return ZstandardNativeMethods.ZSTD_decompressStream(zds, outputBuffer, inputBuffer);
        }

        public static UIntPtr ZSTD_initCStream(IntPtr zcs, int compressionLevel)
        {
            return ZstandardNativeMethods.ZSTD_initCStream(zcs, compressionLevel);
        }

        public static UIntPtr ZSTD_compressStream(IntPtr zcs, [MarshalAs(UnmanagedType.LPStruct)] Buffer outputBuffer, [MarshalAs(UnmanagedType.LPStruct)] Buffer inputBuffer)
        {
            return ZstandardNativeMethods.ZSTD_compressStream(zcs, outputBuffer, inputBuffer);
        }

        public static void ThrowIfError(UIntPtr code)
        {
            if (ZstandardNativeMethods.ZSTD_isError(code))
            {
                var errorPtr = ZstandardNativeMethods.ZSTD_getErrorName(code);
                var errorMsg = Marshal.PtrToStringAnsi(errorPtr);
                throw new IOException(errorMsg);
            }
        }
    }
}
