﻿/* Copyright 2020–present MongoDB Inc.
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
using System.Runtime.InteropServices;
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Snappy
{
    internal static class Snappy32NativeMethods
    {
        // private static fields
        private static readonly Lazy<LibraryLoader> __libraryLoader;
        private static readonly Lazy<Delegates32.snappy_compress> __snappy_compress;
        private static readonly Lazy<Delegates32.snappy_max_compressed_length> __snappy_max_compressed_length;
        private static readonly Lazy<Delegates32.snappy_uncompress> __snappy_uncompress;
        private static readonly Lazy<Delegates32.snappy_uncompressed_length> __snappy_uncompressed_length;
        private static readonly Lazy<Delegates32.snappy_validate_compressed_buffer> __snappy_validate_compressed_buffer;

        // static constructor
        static Snappy32NativeMethods()
        {
            var snappyLocator = new SnappyLocator();
            __libraryLoader = new Lazy<LibraryLoader>(() => new LibraryLoader(snappyLocator), isThreadSafe: true);

            __snappy_compress = CreateLazyForDelegate<Delegates32.snappy_compress>(nameof(snappy_compress));
            __snappy_max_compressed_length = CreateLazyForDelegate<Delegates32.snappy_max_compressed_length>(nameof(snappy_max_compressed_length));
            __snappy_uncompress = CreateLazyForDelegate<Delegates32.snappy_uncompress>(nameof(snappy_uncompress));
            __snappy_uncompressed_length = CreateLazyForDelegate<Delegates32.snappy_uncompressed_length>(nameof(snappy_uncompressed_length));
            __snappy_validate_compressed_buffer = CreateLazyForDelegate<Delegates32.snappy_validate_compressed_buffer>(nameof(snappy_validate_compressed_buffer));
        }

        // public static methods
        public static SnappyStatus snappy_compress(IntPtr input, uint input_length, IntPtr output, ref uint output_length)
        {
            return __snappy_compress.Value(input, input_length, output, ref output_length);
        }

        public static uint snappy_max_compressed_length(uint input_length)
        {
            return __snappy_max_compressed_length.Value(input_length);
        }

        public static SnappyStatus snappy_uncompress(IntPtr input, uint input_length, IntPtr output, ref uint output_length)
        {
            return __snappy_uncompress.Value(input, input_length, output, ref output_length);
        }

        public static SnappyStatus snappy_uncompressed_length(IntPtr input, uint input_length, out uint output_length)
        {
            return __snappy_uncompressed_length.Value(input, input_length, out output_length);
        }

        public static SnappyStatus snappy_validate_compressed_buffer(IntPtr input, uint input_length)
        {
            return __snappy_validate_compressed_buffer.Value(input, input_length);
        }

        // private static methods
        private static Lazy<TDelegate> CreateLazyForDelegate<TDelegate>(string name)
        {
            return new Lazy<TDelegate>(() => __libraryLoader.Value.GetDelegate<TDelegate>(name), isThreadSafe: true);
        }

        // nested types
        private class Delegates32
        {
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            public delegate SnappyStatus snappy_compress(IntPtr input, uint input_length, IntPtr output, ref uint output_length);
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            public delegate uint snappy_max_compressed_length(uint input_length);
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            public delegate SnappyStatus snappy_uncompress(IntPtr input, uint input_length, IntPtr output, ref uint output_length);
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            public delegate SnappyStatus snappy_uncompressed_length(IntPtr input, uint input_length, out uint output_length);
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            public delegate SnappyStatus snappy_validate_compressed_buffer(IntPtr input, uint input_length);
        }

        private class SnappyLocator : RelativeLibraryLocatorBase
        {
            public override string GetLibraryRelativePath(SupportedPlatform currentPlatform)
            {
                switch (currentPlatform)
                {
                    case SupportedPlatform.Windows:
                        return @"runtimes\win\native\snappy32.dll";
                    case SupportedPlatform.Linux: // TODO: add support for Linux and MacOS later
                    case SupportedPlatform.MacOS:
                    default:
                        throw new InvalidOperationException($"Snappy is not supported on the current platform: {currentPlatform}.");
                }
            }
        }
    }
}