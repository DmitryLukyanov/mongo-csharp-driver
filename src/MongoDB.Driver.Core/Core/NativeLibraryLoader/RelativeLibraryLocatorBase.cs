/* Copyright 2019-present MongoDB Inc.
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
using System.Reflection;

namespace MongoDB.Driver.Core.NativeLibraryLoader
{
    internal abstract class RelativeLibraryLocatorBase : ILibraryLocator
    {
        // public methods
        public string GetLibraryAbsolutePath(SupportedPlatform currentPlatform)
        {
            var relativePath = GetLibraryRelativePath(currentPlatform);
            return GetAbsolutePath(relativePath);
        }

        public virtual Assembly GetLibraryBaseAssembly()
        {
            return typeof(RelativeLibraryLocatorBase).GetTypeInfo().Assembly;
        }

        public virtual string GetLibraryBasePath()
        {
            var assembly = GetLibraryBaseAssembly();
            var codeBase = assembly.CodeBase;
            var uri = new Uri(codeBase);
            var absolutePath = uri.AbsolutePath;
            return Path.GetDirectoryName(absolutePath);
        }

        public abstract string GetLibraryRelativePath(SupportedPlatform currentPlatform);

        // private methods
        private string GetAbsolutePath(string relativePath)
        {
            var libraryName = Path.GetFileName(relativePath);

            var basePath = GetLibraryBasePath();
            var absolutePath = Path.Combine(basePath, relativePath);

            var currentFolderAbsolutePath = Path.GetFullPath(libraryName);

            var locationsToCheck = new[]
            {
                currentFolderAbsolutePath,    // look at the current folder
                absolutePath
            };

            foreach (var location in locationsToCheck)
            {
                if (File.Exists(location))
                {
                    return location;
                }
            }
            
            throw new FileNotFoundException($"Could not find library {libraryName}. Checked {string.Join(";", locationsToCheck)}.");
        }
    }
}
