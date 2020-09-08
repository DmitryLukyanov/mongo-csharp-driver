/* Copyright 2013-present MongoDB Inc.
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
using System.Linq;
using MongoDB.Driver.Core.Authentication;
using MongoDB.Driver.Core.Misc;
using MongoDB.Shared;

namespace MongoDB.Driver.Core.Configuration
{
    /// <summary>
    /// Represents settings for a connection.
    /// </summary>
    public class ConnectionSettings
    {
        #region static
        // static fields
        private static readonly IReadOnlyList<IAuthenticator> __noAuthenticators = new IAuthenticator[0];
        #endregion

        // fields
        private readonly string _applicationName;
        private readonly Optional<IReadOnlyList<IAuthenticator>> _authenticators;
        private Optional<Func<IEnumerable<IAuthenticator>>> _authenticatorsConfigurator;
        private AuthenticatorsMode _authenticatorsMode;
        private readonly IReadOnlyList<CompressorConfiguration> _compressors;
        private readonly TimeSpan _maxIdleTime;
        private readonly TimeSpan _maxLifeTime;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionSettings" /> class.
        /// </summary>
        /// <param name="authenticators">The authenticators.</param>
        /// <param name="compressors">The compressors.</param>
        /// <param name="maxIdleTime">The maximum idle time.</param>
        /// <param name="maxLifeTime">The maximum life time.</param>
        /// <param name="applicationName">The application name.</param>
        /// <param name="authenticatorsConfigurator">The authenticator configurator.</param>
        public ConnectionSettings(
            Optional<IEnumerable<IAuthenticator>> authenticators = default(Optional<IEnumerable<IAuthenticator>>),
            Optional<IEnumerable<CompressorConfiguration>> compressors = default(Optional<IEnumerable<CompressorConfiguration>>),
            Optional<TimeSpan> maxIdleTime = default(Optional<TimeSpan>),
            Optional<TimeSpan> maxLifeTime = default(Optional<TimeSpan>),
            Optional<string> applicationName = default(Optional<string>),
            Optional<Func<IEnumerable<IAuthenticator>>> authenticatorsConfigurator = default)
        {
            _authenticatorsMode = EnsureAuthenticationModeIsValid(authenticators, authenticatorsConfigurator);

            _authenticators = EnsureThatAssignedAuthenticatorsAreNotNull(authenticators);
            _authenticatorsConfigurator = authenticatorsConfigurator;
            _compressors = Ensure.IsNotNull(compressors.WithDefault(Enumerable.Empty<CompressorConfiguration>()), nameof(compressors)).ToList();
            _maxIdleTime = Ensure.IsGreaterThanZero(maxIdleTime.WithDefault(TimeSpan.FromMinutes(10)), "maxIdleTime");
            _maxLifeTime = Ensure.IsGreaterThanZero(maxLifeTime.WithDefault(TimeSpan.FromMinutes(30)), "maxLifeTime");
            _applicationName = ApplicationNameHelper.EnsureApplicationNameIsValid(applicationName.WithDefault(null), nameof(applicationName));
        }

        // properties
        /// <summary>
        /// Gets the name of the application.
        /// </summary>
        /// <value>
        /// The name of the application.
        /// </value>
        public string ApplicationName
        {
            get { return _applicationName; }
        }

        /// <summary>
        /// Gets the authenticators.
        /// </summary>
        /// <value>
        /// The authenticators.
        /// </value>
        public IReadOnlyList<IAuthenticator> Authenticators
        {
            get { return _authenticators.WithDefault(__noAuthenticators); }
        }

        /// <summary>
        /// Gets the compressors.
        /// </summary>
        /// <value>
        /// The compressors.
        /// </value>
        public IReadOnlyList<CompressorConfiguration> Compressors
        {
            get { return _compressors; }
        }

        /// <summary>
        /// Gets the maximum idle time.
        /// </summary>
        /// <value>
        /// The maximum idle time.
        /// </value>
        public TimeSpan MaxIdleTime
        {
            get { return _maxIdleTime; }
        }

        /// <summary>
        /// Gets the maximum life time.
        /// </summary>
        /// <value>
        /// The maximum life time.
        /// </value>
        public TimeSpan MaxLifeTime
        {
            get { return _maxLifeTime; }
        }

        // internal methods
        internal ConnectionSettings Fork()
        {
            if (_authenticatorsMode == AuthenticatorsMode.Authenticators)
            {
                // the obsolete code path
                return this;
            }

            var forkedAuthenticators = _authenticatorsConfigurator.HasValue && _authenticatorsConfigurator.Value != null ? _authenticatorsConfigurator.Value() : Authenticators;
            var savedAuthenticationMode = _authenticatorsMode;
            var savedAuthenticationConfigurator = _authenticatorsConfigurator;

            ConnectionSettings forked;
            try
            {
                // temporally auth reset, it needs to pass the validation in the constructor
                // and will be restored at the end of the method
                _authenticatorsMode = AuthenticatorsMode.Authenticators;
                _authenticatorsConfigurator = new Optional<Func<IEnumerable<IAuthenticator>>>();

                forked = With(authenticators: Optional.Create(forkedAuthenticators));
                forked._authenticatorsMode = savedAuthenticationMode;
                forked._authenticatorsConfigurator = savedAuthenticationConfigurator;
            }
            finally
            {
                _authenticatorsMode = savedAuthenticationMode;
                _authenticatorsConfigurator = savedAuthenticationConfigurator;
            }

            return forked;
        }

        /// <summary>
        /// Returns a new ConnectionSettings instance with some settings changed.
        /// </summary>
        /// <param name="authenticators">The authenticators.</param>
        /// <param name="compressors">The compressors.</param>
        /// <param name="maxIdleTime">The maximum idle time.</param>
        /// <param name="maxLifeTime">The maximum life time.</param>
        /// <param name="applicationName">The application name.</param>
        /// <param name="authenticatorsConfigurator">The authenticator configurator.</param>
        /// <returns>A new ConnectionSettings instance.</returns>
        public ConnectionSettings With(
            Optional<IEnumerable<IAuthenticator>> authenticators = default(Optional<IEnumerable<IAuthenticator>>),
            Optional<IEnumerable<CompressorConfiguration>> compressors = default(Optional<IEnumerable<CompressorConfiguration>>),
            Optional<TimeSpan> maxIdleTime = default(Optional<TimeSpan>),
            Optional<TimeSpan> maxLifeTime = default(Optional<TimeSpan>),
            Optional<string> applicationName = default(Optional<string>),
            Optional<Func<IEnumerable<IAuthenticator>>> authenticatorsConfigurator = default)
        {
            EnsureAuthenticationModeIsValid(authenticators, authenticatorsConfigurator);

            var effectiveAuthenticators = authenticators.HasValue ? authenticators : (_authenticators.HasValue ? _authenticators.Value.ToList() : new Optional<IEnumerable<IAuthenticator>>());
            var effectiveAuthenticatorsConfigurator = authenticatorsConfigurator.HasValue ? authenticatorsConfigurator : (_authenticatorsConfigurator.HasValue ? _authenticatorsConfigurator.Value : new Optional<Func<IEnumerable<IAuthenticator>>>());

            return new ConnectionSettings(
                authenticators: effectiveAuthenticators,
                compressors: Optional.Enumerable(compressors.WithDefault(_compressors)),
                maxIdleTime: maxIdleTime.WithDefault(_maxIdleTime),
                maxLifeTime: maxLifeTime.WithDefault(_maxLifeTime),
                applicationName: applicationName.WithDefault(_applicationName),
                authenticatorsConfigurator: effectiveAuthenticatorsConfigurator);
        }

        // private methods
        private AuthenticatorsMode EnsureAuthenticationModeIsValid(
            Optional<IEnumerable<IAuthenticator>> authenticators,
            Optional<Func<IEnumerable<IAuthenticator>>> authenticatorsConfigurator)
        {
            if (authenticators.HasValue && authenticatorsConfigurator.HasValue)
            {
                throw new ArgumentException($"{nameof(authenticators)} and {nameof(authenticatorsConfigurator)} cannot both be configured.");
            }

            if (authenticators.HasValue)
            {
                if (_authenticatorsMode == AuthenticatorsMode.AuthenticatorsConfigurator)
                {
                    throw new InvalidOperationException($"{nameof(authenticators)} cannot be specified if {nameof(authenticatorsConfigurator)} has already been specified.");
                }
                else
                {
                    return AuthenticatorsMode.Authenticators;
                }
            }

            if (authenticatorsConfigurator.HasValue)
            {
                if (_authenticatorsMode == AuthenticatorsMode.Authenticators)
                {
                    throw new InvalidOperationException($"{nameof(authenticatorsConfigurator)} cannot be specified if {nameof(authenticators)} has already been specified.");
                }
                else
                {
                    return AuthenticatorsMode.AuthenticatorsConfigurator;
                }
            }

            return _authenticatorsMode;
        }

        private Optional<IReadOnlyList<IAuthenticator>> EnsureThatAssignedAuthenticatorsAreNotNull(Optional<IEnumerable<IAuthenticator>> authenticators)
        {
            if (authenticators.HasValue)
            {
                return new Optional<IReadOnlyList<IAuthenticator>>(Ensure.IsNotNull(authenticators.Value, nameof(authenticators)).ToList());
            }
            return new Optional<IReadOnlyList<IAuthenticator>>();
        }

        // nested type
        private enum AuthenticatorsMode
        {
            NotSet,
            Authenticators,
            AuthenticatorsConfigurator,
        }
    }
}
