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
using FluentAssertions;
using MongoDB.Bson.TestHelpers;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Authentication;
using MongoDB.Driver.Core.Compression;
using Xunit;

namespace MongoDB.Driver.Core.Configuration
{
    public class ConnectionSettingsTests
    {
        private static readonly ConnectionSettings __defaults = new ConnectionSettings();

        [Fact]
        public void constructor_should_initialize_instance()
        {
            var subject = new ConnectionSettings();

            subject.ApplicationName.Should().BeNull();
            subject.Authenticators.Should().BeEmpty();
            subject.Compressors.Should().BeEmpty();
            subject.MaxIdleTime.Should().Be(TimeSpan.FromMinutes(10));
            subject.MaxLifeTime.Should().Be(TimeSpan.FromMinutes(30));
        }

        [Fact]
        public void constructor_should_throw_when_applicationName_is_too_long()
        {
            var applicationName = new string('x', 129);

            var exception = Record.Exception(() => new ConnectionSettings(applicationName: applicationName));

            var argumentException = exception.Should().BeOfType<ArgumentException>().Subject;
            argumentException.ParamName.Should().Be("applicationName");
        }

        [Fact]
        public void constructor_should_throw_when_authenticators_is_null()
        {
            Action action = () => new ConnectionSettings(authenticators: null);

            action.ShouldThrow<ArgumentNullException>().And.ParamName.Should().Be("authenticators");
        }

        [Fact]
        public void constructor_should_throw_when_compressors_is_null()
        {
            var exception = Record.Exception(() => new ConnectionSettings(compressors: null));

            var e = exception.Should().BeOfType<ArgumentNullException>().Subject;
            e.ParamName.Should().Be("compressors");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_should_throw_when_maxIdleTime_is_negative_or_zero(
            [Values(-1, 0)]
            int maxIdleTime)
        {
            Action action = () => new ConnectionSettings(maxIdleTime: TimeSpan.FromSeconds(maxIdleTime));

            action.ShouldThrow<ArgumentException>().And.ParamName.Should().Be("maxIdleTime");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_should_throw_when_maxLifeTime_is_negative_or_zero(
            [Values(-1, 0)]
            int maxLifeTime)
        {
            Action action = () => new ConnectionSettings(maxLifeTime: TimeSpan.FromSeconds(maxLifeTime));

            action.ShouldThrow<ArgumentException>().And.ParamName.Should().Be("maxLifeTime");
        }

        [Fact]
        public void constructor_with_applicationName_should_initialize_instance()
        {
            var subject = new ConnectionSettings(applicationName: "app");

            subject.ApplicationName.Should().Be("app");
            subject.Authenticators.Should().Equal(__defaults.Authenticators);
            subject.Compressors.Should().Equal(__defaults.Compressors);
            subject.MaxIdleTime.Should().Be(__defaults.MaxIdleTime);
            subject.MaxLifeTime.Should().Be(__defaults.MaxLifeTime);
        }

        [Fact]
        public void constructor_with_authenticators_should_initialize_instance()
        {
#pragma warning disable 618
            var authenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username", "password")) };
#pragma warning restore 618

            var subject = new ConnectionSettings(authenticators: authenticators);

            subject.ApplicationName.Should().BeNull();
            subject.Authenticators.Should().Equal(authenticators);
            subject.Compressors.Should().BeEquivalentTo(__defaults.Compressors);
            subject.MaxIdleTime.Should().Be(__defaults.MaxIdleTime);
            subject.MaxLifeTime.Should().Be(__defaults.MaxLifeTime);
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_with_authenticatorsConfigurator_should_initialize_instance([Values(false, true)] bool forked)
        {
#pragma warning disable 618
            var authenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username", "password")) };
#pragma warning restore 618
            Func<IEnumerable<IAuthenticator>> authenticatorsConfigurator = () => authenticators;

            var subject = new ConnectionSettings(authenticatorsConfigurator: authenticatorsConfigurator);
            if (forked)
            {
                subject = subject.Fork();
            }

            subject.ApplicationName.Should().BeNull();
            if (forked)
            {
                subject.Authenticators.Should().Equal(authenticators);
            }
            else
            {
                subject.Authenticators.Should().BeEmpty();
            }
            subject.Compressors.Should().BeEquivalentTo(__defaults.Compressors);
            subject.MaxIdleTime.Should().Be(__defaults.MaxIdleTime);
            subject.MaxLifeTime.Should().Be(__defaults.MaxLifeTime);
        }

        [Fact]
        public void constructor_with_authenticators_and_authenticatorsConfigurator_should_throw()
        {
            var exception = Record.Exception(() => new ConnectionSettings(authenticatorsConfigurator: null, authenticators: null));
            var e = exception.Should().BeOfType<ArgumentException>().Subject;
            e.Message.EndsWith("authenticators and authenticatorsConfigurator cannot both be configured.");
        }

        [Fact]
        public void constructor_with_compressors_should_initialize_instance()
        {
            var compressors = new[] { new CompressorConfiguration(CompressorType.Zlib) };

            var subject = new ConnectionSettings(compressors: compressors);

            subject.ApplicationName.Should().BeNull();
            subject.Authenticators.Should().Equal(__defaults.Authenticators);
            subject.Compressors.Should().Equal(compressors);
            subject.MaxIdleTime.Should().Be(__defaults.MaxIdleTime);
            subject.MaxLifeTime.Should().Be(__defaults.MaxLifeTime);
        }

        [Fact]
        public void constructor_with_maxIdleTime_should_initialize_instance()
        {
            var maxIdleTime = TimeSpan.FromSeconds(123);

            var subject = new ConnectionSettings(maxIdleTime: maxIdleTime);

            subject.ApplicationName.Should().BeNull();
            subject.Authenticators.Should().Equal(__defaults.Authenticators);
            subject.Compressors.Should().Equal(__defaults.Compressors);
            subject.MaxIdleTime.Should().Be(maxIdleTime);
            subject.MaxLifeTime.Should().Be(__defaults.MaxLifeTime);
        }

        [Fact]
        public void constructor_with_maxLifeTime_should_initialize_instance()
        {
            var maxLifeTime = TimeSpan.FromSeconds(123);

            var subject = new ConnectionSettings(maxLifeTime: maxLifeTime);

            subject.ApplicationName.Should().BeNull();
            subject.Authenticators.Should().Equal(__defaults.Authenticators);
            subject.Compressors.Should().Equal(__defaults.Compressors);
            subject.MaxIdleTime.Should().Be(__defaults.MaxIdleTime);
            subject.MaxLifeTime.Should().Be(maxLifeTime);
        }

        [Fact]
        public void Fork_with_Authenticators_should_return_authenticators()
        {
#pragma warning disable CS0618 // Type or member is obsolete
            var authenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username", "password")) };
#pragma warning restore CS0618 // Type or member is obsolete

            var valid = new ConnectionSettings(authenticators: authenticators);
            var forked = valid.Fork();
            forked.Authenticators.Should().Equal(authenticators);
        }

        [Fact]
        public void Multiple_Forks_should_create_different_authenticators_instances()
        {
#pragma warning disable 618
            Func<IEnumerable<IAuthenticator>> authenticatorsConfigurator = () => new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username", "password")) };
#pragma warning restore 618
            var avoidInAsserting = new List<IAuthenticator>(authenticatorsConfigurator());

            var originalSubject = new ConnectionSettings(authenticatorsConfigurator: authenticatorsConfigurator);
            originalSubject.Authenticators.Should().BeEmpty();
            originalSubject._authenticatorsConfigurator().HasValue.Should().BeTrue();

            var sourceSubject = originalSubject;

            var forkedSubject = sourceSubject.Fork();
            Assert(withAuthenticatorsInSource: false, avoidInAsserting);

            avoidInAsserting.AddRange(forkedSubject.Authenticators);
            sourceSubject = forkedSubject;
            forkedSubject = forkedSubject.Fork();
            Assert(withAuthenticatorsInSource: true, avoidInAsserting);

            avoidInAsserting.AddRange(forkedSubject.Authenticators);
            sourceSubject = forkedSubject;
            forkedSubject = forkedSubject.Fork();
            Assert(withAuthenticatorsInSource: true, avoidInAsserting);

            void Assert(bool withAuthenticatorsInSource = true, params IEnumerable<IAuthenticator>[] previousAuthenticators)
            {
                originalSubject.Authenticators.Should().BeEmpty();
                originalSubject._authenticatorsConfigurator().HasValue.Should().BeTrue();
                AssertNonAuthSettings(originalSubject);

                sourceSubject._authenticatorsConfigurator().HasValue.Should().BeTrue();
                sourceSubject.Authenticators.Count.Should().Be(withAuthenticatorsInSource ? 1 : 0);
                AssertNonAuthSettings(sourceSubject);

                forkedSubject.ApplicationName.Should().BeNull();
                forkedSubject.Authenticators.Single().Name.Should().Be(avoidInAsserting.First().Name); // assert that the auth values are the same
                foreach (var previousAuthenticator in previousAuthenticators)
                {
                    forkedSubject.Authenticators.Should().NotBeSameAs(previousAuthenticator);
                }
                AssertNonAuthSettings(forkedSubject);
            }

            void AssertNonAuthSettings(ConnectionSettings settings)
            {
                settings.ApplicationName.Should().BeNull();
                settings.Compressors.Should().BeEquivalentTo(__defaults.Compressors);
                settings.MaxIdleTime.Should().Be(__defaults.MaxIdleTime);
                settings.MaxLifeTime.Should().Be(__defaults.MaxLifeTime);
            }
        }

        [Fact]
        public void With_applicationName_should_return_expected_result()
        {
            var oldApplicationName = "app1";
            var newApplicationName = "app2";
            var subject = new ConnectionSettings(applicationName: oldApplicationName);

            var result = subject.With(applicationName: newApplicationName);

            result.ApplicationName.Should().Be(newApplicationName);
            result.Authenticators.Should().Equal(subject.Authenticators);
            subject.Compressors.Should().Equal(__defaults.Compressors);
            result.MaxIdleTime.Should().Be(subject.MaxIdleTime);
            result.MaxLifeTime.Should().Be(subject.MaxLifeTime);
        }

        [Fact]
        public void With_authenticators_should_return_expected_result()
        {
#pragma warning disable 618
            var oldAuthenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username1", "password1")) };
            var newAuthenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username2", "password2")) };
#pragma warning restore 618
            var subject = new ConnectionSettings(authenticators: oldAuthenticators);

            var result = subject.With(authenticators: newAuthenticators);

            result.ApplicationName.Should().Be(subject.ApplicationName);
            result.Authenticators.Should().Equal(newAuthenticators);
            subject.Compressors.Should().Equal(subject.Compressors);
            result.MaxIdleTime.Should().Be(subject.MaxIdleTime);
            result.MaxLifeTime.Should().Be(subject.MaxLifeTime);
        }

        [Fact]
        public void With_authenticatorsConfigurator_should_return_expected_result()
        {
#pragma warning disable 618
            var oldAuthenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username1", "password1")) };
            var newAuthenticators = new[] { new MongoDBCRAuthenticator(new UsernamePasswordCredential("source", "username2", "password2")) };
#pragma warning restore 618

            Func<IEnumerable<IAuthenticator>> oldAuthenticatorsConfigurator = () => oldAuthenticators;
            Func<IEnumerable<IAuthenticator>> newAuthenticatorsConfigurator = () => newAuthenticators;
            var subject = new ConnectionSettings(authenticatorsConfigurator: oldAuthenticatorsConfigurator);

            var result = subject.With(authenticatorsConfigurator: newAuthenticatorsConfigurator);
            result = result.Fork(); // to apply authenticatorsConfigurator

            result.ApplicationName.Should().Be(subject.ApplicationName);
            result.Authenticators.Should().Equal(newAuthenticators);
            subject.Compressors.Should().Equal(subject.Compressors);
            result.MaxIdleTime.Should().Be(subject.MaxIdleTime);
            result.MaxLifeTime.Should().Be(subject.MaxLifeTime);
        }

        [Fact]
        public void With_authenticators_and_authenticatorsConfigurator_should_throw()
        {
            var validWith = __defaults.With(authenticators: new IAuthenticator[0]);
            var exception = Record.Exception(() => validWith.With(authenticatorsConfigurator: null)); // the value doesn't matter
            AssertException<InvalidOperationException>($"authenticatorsConfigurator cannot be specified if authenticators has already been specified.");

            validWith = __defaults.With(authenticatorsConfigurator: null);  // the value doesn't matter
            exception = Record.Exception(() => validWith.With(authenticators: new IAuthenticator[0]));
            AssertException<InvalidOperationException>($"authenticators cannot be specified if authenticatorsConfigurator has already been specified.");

            exception = Record.Exception(() => __defaults.With(authenticators: new IAuthenticator[0], authenticatorsConfigurator: null));
            AssertException<ArgumentException>("authenticators and authenticatorsConfigurator cannot both be configured.");

            void AssertException<TException>(string exceptionMessage) where TException : Exception
            {
                var e = exception.Should().BeOfType<TException>().Subject;
                e.Message.Should().Be(exceptionMessage);
            }
        }

        [Fact]
        public void Authenticators_should_not_be_lost_after_specifying_different_option()
        {
            var authenticators = new IAuthenticator[] { new ScramSha1Authenticator(new UsernamePasswordCredential("s", "u", "p")) };
            var validWith = __defaults.With(authenticators: authenticators);
            validWith._authenticatorsConfigurator().HasValue.Should().BeFalse();
            validWith = validWith.With(compressors: new CompressorConfiguration[0]);

            validWith._authenticatorsConfigurator().HasValue.Should().BeFalse();

            var a = validWith.Authenticators.First().Should().BeOfType<ScramSha1Authenticator>().Subject;
            a.DatabaseName.Should().Be("s");
            a.Name.Should().Be(ScramSha1Authenticator.MechanismName);
        }

        [Fact]
        public void AuthenticatorsConfigurator_should_not_be_lost_after_specifying_different_option()
        {
            var authenticators = new IAuthenticator[] { new ScramSha1Authenticator(new UsernamePasswordCredential("s", "u", "p")) };
            Func<IEnumerable<IAuthenticator>> authenticatorsConfigurator = () => authenticators;
            var validWith = __defaults.With(authenticatorsConfigurator: authenticatorsConfigurator);
            validWith._authenticatorsConfigurator().HasValue.Should().BeTrue();
            validWith = validWith.With(compressors: new CompressorConfiguration[0]);

            validWith._authenticatorsConfigurator().HasValue.Should().BeTrue();

            var forked = validWith.Fork();
            var a = forked.Authenticators.First().Should().BeOfType<ScramSha1Authenticator>().Subject;
            a.DatabaseName.Should().Be("s");
            a.Name.Should().Be(ScramSha1Authenticator.MechanismName);
        }

        [Fact]
        public void With_compressors_should_return_expected_result()
        {
            var oldCompressors = new[] { new CompressorConfiguration(CompressorType.Zlib) };
            var newCompressors = new[] { new CompressorConfiguration(CompressorType.Snappy) };
            var subject = new ConnectionSettings(compressors: oldCompressors);

            var result = subject.With(compressors: newCompressors);

            result.ApplicationName.Should().Be(subject.ApplicationName);
            result.Authenticators.Should().Equal(subject.Authenticators);
            result.Compressors.Should().Equal(newCompressors);
            result.MaxIdleTime.Should().Be(subject.MaxIdleTime);
            result.MaxLifeTime.Should().Be(subject.MaxLifeTime);
        }

        [Fact]
        public void With_maxIdleTime_should_return_expected_result()
        {
            var oldMaxIdleTime = TimeSpan.FromSeconds(1);
            var newMaxIdleTime = TimeSpan.FromSeconds(2);
            var subject = new ConnectionSettings(maxIdleTime: oldMaxIdleTime);

            var result = subject.With(maxIdleTime: newMaxIdleTime);

            result.ApplicationName.Should().Be(subject.ApplicationName);
            result.Authenticators.Should().Equal(subject.Authenticators);
            result.Compressors.Should().Equal(subject.Compressors);
            result.MaxIdleTime.Should().Be(newMaxIdleTime);
            result.MaxLifeTime.Should().Be(subject.MaxLifeTime);
        }

        [Fact]
        public void With_maxLifeTime_should_return_expected_result()
        {
            var oldMaxLifeTime = TimeSpan.FromSeconds(1);
            var newMaxLifeTime = TimeSpan.FromSeconds(2);
            var subject = new ConnectionSettings(maxLifeTime: oldMaxLifeTime);

            var result = subject.With(maxLifeTime: newMaxLifeTime);

            result.ApplicationName.Should().Be(subject.ApplicationName);
            result.Authenticators.Should().Equal(subject.Authenticators);
            result.Compressors.Should().Equal(subject.Compressors);
            result.MaxIdleTime.Should().Be(subject.MaxIdleTime);
            result.MaxLifeTime.Should().Be(newMaxLifeTime);
        }
    }

    internal static class ConnectionSettingsReflector
    {
        public static Optional<Func<IEnumerable<IAuthenticator>>> _authenticatorsConfigurator(this ConnectionSettings connectionSettings) => (Optional<Func<IEnumerable<IAuthenticator>>>)Reflector.GetFieldValue(connectionSettings, nameof(_authenticatorsConfigurator));
    }
}
