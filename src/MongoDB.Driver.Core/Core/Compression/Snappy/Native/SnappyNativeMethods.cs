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
using MongoDB.Driver.Core.Compression.Native;
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Snappy.Native
{
    internal static class SnappyNativeMethods
    {
        private static readonly IDictionary<SupportedPlatforms, string> __libraryPaths = new Dictionary<SupportedPlatforms, string>
        {
            { SupportedPlatforms.Windows, string.Empty }, // bin folder
            // On Linux, the snappy library depends on the Snappy package.
            // For Debian/Ubuntu: apt-get install libsnappy1 libsnappy-dev
            { SupportedPlatforms.Linux, "../../native/linux/" }, // todo: check
            { SupportedPlatforms.MacOS, "../../native/osx/" } // todo: check
        };

        private static readonly Delegates64.snappy_compress __snappy_compress;
        private static readonly Delegates64.snappy_max_compressed_length __snappy_max_compressed_length;
        private static readonly Delegates64.snappy_uncompress __snappy_uncompress;
        private static readonly Delegates64.snappy_uncompressed_length __snappy_uncompressed_length;
        private static readonly Delegates64.snappy_validate_compressed_buffer __snappy_validate_compressed_buffer;

        // private constructor
        static SnappyNativeMethods()
        {
            var nativeLibrary = new NativeLibrary(__libraryPaths, "snappy64.dll");

            __snappy_compress = nativeLibrary.GetFunction<Delegates64.snappy_compress>(nameof(snappy_compress));
            __snappy_max_compressed_length = nativeLibrary.GetFunction<Delegates64.snappy_max_compressed_length>(nameof(snappy_max_compressed_length));
            __snappy_uncompress = nativeLibrary.GetFunction<Delegates64.snappy_uncompress>(nameof(snappy_uncompress));
            __snappy_uncompressed_length = nativeLibrary.GetFunction<Delegates64.snappy_uncompressed_length>(nameof(snappy_uncompressed_length));
            __snappy_validate_compressed_buffer = nativeLibrary.GetFunction<Delegates64.snappy_validate_compressed_buffer>(nameof(snappy_validate_compressed_buffer));
        }

        // public methods
        public static SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length)
        {
            var ulongOutput_length = (ulong)output_length;
            var status = __snappy_compress(input, (ulong)input_length, output, ref ulongOutput_length);
            output_length = (int)ulongOutput_length;
            return status;
        }

        public static int snappy_max_compressed_length(int input_length)
        {
            return (int)__snappy_max_compressed_length((ulong)input_length);
        }

        public static SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length)
        {
            var ulongOutput_length = (ulong)output_length;
            var status = __snappy_uncompress(input, (ulong)input_length, output, ref ulongOutput_length);
            output_length = (int)ulongOutput_length;
            return status;
        }

        public static SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length)
        {
            var status = __snappy_uncompressed_length(input, (ulong)input_length, out var ulongOutput_length);
            output_length = (int)ulongOutput_length;
            return status;
        }

        public static SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length)
        {
            return __snappy_validate_compressed_buffer(input, (ulong)input_length);
        }

        // nested types
        private class Delegates64
        {
            public delegate SnappyStatus snappy_compress(IntPtr input, ulong input_length, IntPtr output, ref ulong output_length);
            public delegate ulong snappy_max_compressed_length(ulong input_length);
            public delegate SnappyStatus snappy_uncompress(IntPtr input, ulong input_length, IntPtr output, ref ulong output_length);
            public delegate SnappyStatus snappy_uncompressed_length(IntPtr input, ulong input_length, out ulong output_length);
            public delegate SnappyStatus snappy_validate_compressed_buffer(IntPtr input, ulong input_length);
        }
    }
}
