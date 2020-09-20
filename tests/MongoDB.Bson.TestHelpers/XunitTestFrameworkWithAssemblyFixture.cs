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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace MongoDB.Bson.TestHelpers.XunitExtensions
{
    public class XunitTestFrameworkWithAssemblyFixture : XunitTestFramework
    {
        public const string TypeName = "MongoDB.Bson.TestHelpers.XunitExtensions.XunitTestFrameworkWithAssemblyFixture";
        public const string AssemblyName = "MongoDB.Bson.TestHelpers";

        public XunitTestFrameworkWithAssemblyFixture(IMessageSink messageSink)
            : base(messageSink)
        {
        }

        // the base implementation just create XunitTestFrameworkExecutor (no new parameters)
        protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName)
            => new XunitTestFrameworkExecutorWithAssemblyFixture(assemblyName, SourceInformationProvider, DiagnosticMessageSink);
    }

    public class XunitTestFrameworkExecutorWithAssemblyFixture : XunitTestFrameworkExecutor
    {
        public XunitTestFrameworkExecutorWithAssemblyFixture(
            AssemblyName assemblyName,
            ISourceInformationProvider sourceInformationProvider,
            IMessageSink diagnosticMessageSink)
            : base(assemblyName, sourceInformationProvider, diagnosticMessageSink)
        { }

        // the base implementation just create XunitTestAssemblyRunner and then call RunAsync (no new parameters)
        protected override async void RunTestCases(IEnumerable<IXunitTestCase> testCases, IMessageSink executionMessageSink, ITestFrameworkExecutionOptions executionOptions)
        {
            using (var assemblyRunner = new XunitTestAssemblyRunnerWithAssemblyFixture(
                TestAssembly,
                testCases,
                DiagnosticMessageSink,
                executionMessageSink,
                executionOptions))
            {
                await assemblyRunner.RunAsync();
            }
        }
    }

    public class XunitTestAssemblyRunnerWithAssemblyFixture : XunitTestAssemblyRunner
    {
        private readonly Dictionary<Type, object> _assemblyFixtureMappings = new Dictionary<Type, object>();

        public XunitTestAssemblyRunnerWithAssemblyFixture(
            ITestAssembly testAssembly,
            IEnumerable<IXunitTestCase> testCases,
            IMessageSink diagnosticMessageSink,
            IMessageSink executionMessageSink,
            ITestFrameworkExecutionOptions executionOptions)
            : base(testAssembly, testCases, diagnosticMessageSink, executionMessageSink, executionOptions)
        { }

        protected override async Task AfterTestAssemblyStartingAsync()
        {
            // Let everything initialize
            await base.AfterTestAssemblyStartingAsync();

            // Aggregator(ExceptionAggregator) is intended to run one or more code blocks, and collect the
            // exceptions thrown by those code blocks.
            Aggregator.Run(() =>
            {
                // Go find all the AssemblyFixtureAttributes adorned on the test assembly
                var testAssembly = ((IReflectionAssemblyInfo)TestAssembly.Assembly).Assembly;
                var assemblyFixtureAttributes = testAssembly
                    .GetCustomAttributes<AssemblyFixtureAttribute>()
                    .ToList();

                // Instantiate all the fixtures
                foreach (var fixtureAttribute in assemblyFixtureAttributes)
                {
                    if (_assemblyFixtureMappings.ContainsKey(fixtureAttribute.FixtureType))
                    {
                        throw new InvalidDataException($"FixtureType {fixtureAttribute.FixtureType} must be unique.");
                    }

                    var assemblyFixture = Activator.CreateInstance(fixtureAttribute.FixtureType);
                    if (assemblyFixture is IConfigureAssemblyFixture configureAssembly)
                    {
                        configureAssembly.Initialize();
                    }

                    _assemblyFixtureMappings[fixtureAttribute.FixtureType] = assemblyFixture;
                }
            });
        }

        protected override Task BeforeTestAssemblyFinishedAsync()
        {
            // Make sure we clean up everybody who is disposable, and use Aggregator.Run to isolate Dispose failures
            foreach (var disposable in _assemblyFixtureMappings.Values.OfType<IDisposable>())
            {
                Aggregator.Run(disposable.Dispose);
            }

            return base.BeforeTestAssemblyFinishedAsync();
        }

        // the base implementation just create XunitTestCollectionRunner and then calls RunAsync (_assemblyFixtureMappings is a new parameter)
        protected override Task<RunSummary> RunTestCollectionAsync(
            IMessageBus messageBus,
            ITestCollection testCollection,
            IEnumerable<IXunitTestCase> testCases,
            CancellationTokenSource cancellationTokenSource)
        {
            var testCollectionRunner = new XunitTestCollectionRunnerWithAssemblyFixture(
                _assemblyFixtureMappings,
                testCollection,
                testCases,
                DiagnosticMessageSink,
                messageBus,
                TestCaseOrderer,
                new ExceptionAggregator(Aggregator),
                cancellationTokenSource);
            return testCollectionRunner.RunAsync();
        }
    }

    public class XunitTestCollectionRunnerWithAssemblyFixture : XunitTestCollectionRunner
    {
        private readonly Dictionary<Type, object> _assemblyFixtureMappings;
        private readonly IMessageSink _diagnosticMessageSink;

        public XunitTestCollectionRunnerWithAssemblyFixture(
            Dictionary<Type, object> assemblyFixtureMappings,
            ITestCollection testCollection,
            IEnumerable<IXunitTestCase> testCases,
            IMessageSink diagnosticMessageSink,
            IMessageBus messageBus,
            ITestCaseOrderer testCaseOrderer,
            ExceptionAggregator aggregator,
            CancellationTokenSource cancellationTokenSource)
            : base(testCollection, testCases, diagnosticMessageSink, messageBus, testCaseOrderer, aggregator, cancellationTokenSource)
        {
            _assemblyFixtureMappings = assemblyFixtureMappings;
            _diagnosticMessageSink = diagnosticMessageSink;
        }

        // the base method just calls XunitTestClassRunner
        // NOTE: This method will be called before each test class
        protected override Task<RunSummary> RunTestClassAsync(ITestClass testClass, IReflectionTypeInfo @class, IEnumerable<IXunitTestCase> testCases)
        {
            // Don't want to use .Concat + .ToDictionary because of the possibility of overriding types,
            // so instead we'll just let collection fixtures override assembly fixtures.
            var combinedFixtures = new Dictionary<Type, object>(_assemblyFixtureMappings);
            foreach (var collectionFixture in CollectionFixtureMappings)
            {
                combinedFixtures[collectionFixture.Key] = collectionFixture.Value;
            }

            // We've done everything we need, so let the built-in types do the rest of the heavy lifting
            var testClassRunner = new XunitTestClassRunner(
                testClass,
                @class,
                testCases,
                _diagnosticMessageSink,
                MessageBus,
                TestCaseOrderer,
                new ExceptionAggregator(Aggregator),
                CancellationTokenSource,
                combinedFixtures);
            return testClassRunner.RunAsync();
        }
    }
}
