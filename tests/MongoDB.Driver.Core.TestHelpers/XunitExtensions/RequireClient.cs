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
using System.Collections.Generic;
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
            return SkipWhen(operatingSystem, () => true, targetFrameworks);
        }

        public RequireClient SkipWhen(SupportedOperatingSystem operatingSystem, Func<bool> condition, params SupportedTargetFramework[] targetFrameworks)
        {
            if (condition() && IsTheSameAsCurrentOperatingSystem(operatingSystem))
            {
                foreach (var targetFramework in targetFrameworks)
                {
                    if (IsTheSameAsCurrentTargetFramework(targetFramework))
                    {
                        throw new SkipException($"Test skipped because it's not supported on {targetFrameworks} with {targetFramework}.");
                    }
                }
            }
            return this;
        }

        private bool IsTheSameAsCurrentOperatingSystem(SupportedOperatingSystem operatingSystemPlatform)
        {
            var result = false;
            switch (operatingSystemPlatform)
            {
                case SupportedOperatingSystem.Windows:
#if WINDOWS
                    result = true;
#endif
                    break;
                case SupportedOperatingSystem.Linux:
#if LINUX
                    result = true;
#endif
                    break;
                case SupportedOperatingSystem.MacOS:
#if MACOS
                    result = true;
#endif
                    break;
                default:
                    throw new Exception($"Unsupported {nameof(operatingSystemPlatform)} {operatingSystemPlatform}.");
            }
            return result;
        }

        private bool IsTheSameAsCurrentTargetFramework(SupportedTargetFramework targetFramework)
        {
            var result = false;
            switch (targetFramework)
            {
                case SupportedTargetFramework.Net452:
#if NET452
                    result = true;
#endif
                    break;
                case SupportedTargetFramework.NetCoreApp11:
#if NETSTANDARD1_5
                    result = true;
#endif
                    break;
                case SupportedTargetFramework.NetCoreApp21:
#if NETSTANDARD2_0
                    result = true;
#endif
                    break;
                case SupportedTargetFramework.NetCoreApp30:
#if NETSTANDARD2_1
                    result = true;
#endif
                    break;
                default:
                    throw new Exception($"Unsupported {nameof(targetFramework)} {targetFramework}.");
            }
            return result;
        }
    }
}
