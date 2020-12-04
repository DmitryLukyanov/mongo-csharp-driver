/* Copyright 2020-present MongoDB Inc.
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
using System.Linq;
using Xunit;

namespace MongoDB.Driver.TestHelpers
{
    public enum SupportedTargetFramework
    {
        Net452,
        NetCoreApp11,
        NetCoreApp21,
        NetCoreApp30
    }

    public enum SupportedOperatingSystem
    {
        Windows,
        Linux,
        MacOS
    }

    public class RequireClient
    {
        #region static
        public static RequireClient Create() => new RequireClient();
        #endregion

        public RequireClient SkipWhen(SupportedOperatingSystem operatingSystem, params SupportedTargetFramework[] targetFrameworks)
        {
            var currentOperatingSystem = GetCurrentOperatingSystem();
            var currentTargetFramework = GetCurrentTargetFramework();
            if (operatingSystem == currentOperatingSystem && targetFrameworks.Contains(currentTargetFramework))
            {
                throw new SkipException($"Test skipped because it's not supported on {currentOperatingSystem} with {currentTargetFramework}.");
            }

            return this;
        }

        public RequireClient SkipWhen(Func<bool> condition, SupportedOperatingSystem operatingSystem, params SupportedTargetFramework[] targetFrameworks)
        {
            if (condition())
            {
                SkipWhen(operatingSystem, targetFrameworks);
            }

            return this;
        }

        private SupportedOperatingSystem GetCurrentOperatingSystem()
        {
#if WINDOWS
            return SupportedOperatingSystem.Windows;
#endif
#if LINUX
            return SupportedOperatingSystem.Linux;
#endif
#if MACOS
            return case SupportedOperatingSystem.MacOS.
#endif

            throw new InvalidOperationException($"Unable to determine current operating system.");
        }


        private SupportedTargetFramework GetCurrentTargetFramework()
        {
#if NET452
            return SupportedTargetFramework.Net452;
#endif
#if NETSTANDARD1_5
                    return SupportedTargetFramework.NetCoreApp11;
#endif
#if NETSTANDARD2_0
            return SupportedTargetFramework.NetCoreApp21;
#endif
#if NETSTANDARD2_1
            return SupportedTargetFramework.NetCoreApp30;
#endif

            throw new InvalidOperationException($"Unable to determine current target framework.");
        }
    }
}
