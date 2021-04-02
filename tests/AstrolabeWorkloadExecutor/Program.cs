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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using AstrolabeWorkloadExecutor;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Tests.Specifications.unified_test_format;
using MongoDB.Driver.Tests.UnifiedTestOperations;

namespace WorkloadExecutor
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            //args = new[]
            //{
            //    "mongodb://localhost",
            //    "{'description': 'Find', 'schemaVersion': '1.2', 'createEntities': [{'client': {'id': 'client0', 'uriOptions': {'retryReads': true}, 'storeEventsAsEntities': [{'id': 'events', 'events': ['PoolCreatedEvent', 'PoolReadyEvent', 'PoolClearedEvent', 'PoolClosedEvent', 'ConnectionCreatedEvent', 'ConnectionReadyEvent', 'ConnectionClosedEvent', 'ConnectionCheckOutStartedEvent', 'ConnectionCheckOutFailedEvent', 'ConnectionCheckedOutEvent', 'ConnectionCheckedInEvent', 'CommandStartedEvent', 'CommandSucceededEvent', 'CommandFailedEvent']}]}}, {'database': {'id': 'database0', 'client': 'client0', 'databaseName': 'dat'}}, {'collection': {'id': 'collection0', 'database': 'database0', 'collectionName': 'dat'}}], 'initialData': [{'collectionName': 'dat', 'databaseName': 'dat', 'documents': [{'_id': 1, 'x': 11}, {'_id': 2, 'x': 22}, {'_id': 3, 'x': 33}]}], 'tests': [{'description': 'Find one', 'operations': [{'name': 'loop', 'object': 'testRunner', 'arguments': {'storeErrorsAsEntity': 'errors', 'storeIterationsAsEntity': 'iterations', 'storeSuccessesAsEntity': 'successes', 'operations': [{'name': 'find', 'object': 'collection0', 'arguments': {'filter': {'_id': {'$gt': 1}}, 'sort': {'_id': 1}}, 'expectResult': [{'_id': 2, 'x': 22}, {'_id': 3, 'x': 33}]}]}}]}]}"
            //};

            Ensure.IsEqualTo(args.Length, 2, nameof(args.Length));

            var connectionString = args[0];
            var driverWorkload = BsonDocument.Parse(args[1]);

            var cancellationTokenSource = new CancellationTokenSource();
            ConsoleCancelEventHandler cancelHandler = (o, e) => HandleCancel(e, cancellationTokenSource);

            var resultsDir = Environment.GetEnvironmentVariable("RESULTS_DIR") ?? "";
            var eventsPath = Path.Combine(resultsDir, "events.json");
            var resultsPath = Path.Combine(resultsDir, "results.json");
            Console.WriteLine($"dotnet main> Results will be written to {resultsPath}");
            Console.WriteLine($"dotnet main> Events will be written to {eventsPath}");

            Console.CancelKeyPress += cancelHandler;

            Console.WriteLine($"dotnet main> Starting workload executor...");

            var async = bool.Parse(Environment.GetEnvironmentVariable("ASYNC") ?? throw new Exception($"ASYNC environment variable must be configured."));

            var (resultsJson, eventsJson) = ExecuteWorkload(connectionString, driverWorkload, async, cancellationTokenSource.Token);

            Console.CancelKeyPress -= cancelHandler;

            Console.WriteLine($"dotnet main finally> Writing final results and events files");
            File.WriteAllText(resultsPath, resultsJson);
            File.WriteAllText(eventsPath, eventsJson);

            // ensure all messages are propagated to the astrolabe immediately
            Console.Error.Flush();
            Console.Out.Flush();
        }

        private static (string EventsJson, string ResultsJson) CreateWorkloadResult(UnifiedEntityMap entityMap, AstrolabeEventSubscriber eventSubscriber)
        {
            Ensure.IsNotNull(entityMap, nameof(entityMap));

            var iterationsCount = GetValueOrDefault(entityMap.IterationCounts, "iterations", @default: -1);
            var successesCount = GetValueOrDefault(entityMap.SuccessCounts, "successes", @default: -1);

            var errorDocuments = GetValueOrDefault(entityMap.ErrorDocumentsMap, "errors", @default: new BsonArray());
            var errorCount = errorDocuments.Count;
            var failuresDocuments = GetValueOrDefault(entityMap.FailureDocumentsMap, "failures", @default: new BsonArray());
            var failuresCount = failuresDocuments.Count;

            string eventsJson = "[]";
            if (entityMap.EventCapturers.TryGetValue("events", out var eventCapturer))
            {
                //Console.WriteLine($"dotnet events> Number of generated events {eventCapturer.Count}");
                //var stringBuilder = new StringBuilder();
                //stringBuilder.Append("[");
                //for (int i = 0; i < eventCapturer.Events.Count; i++)
                //{
                //    stringBuilder.Append(AstrolabeEventsHandler.CreateEventDocument(eventCapturer.Events[i]));
                //    if (i < eventCapturer.Events.Count - 1)
                //    {
                //        stringBuilder.Append(",");
                //    }
                //}
                //stringBuilder.Append("]");
                //eventsJson = stringBuilder.ToString();
                eventsJson = $"[{string.Join(",", eventSubscriber.Queue.ToList())}]";
            }

            var eventsDocument = @$"{{ ""events"" : {eventsJson}, ""errors"" : {errorDocuments}, ""failures"" : {failuresDocuments} }}";
            var resultsDocument = $@"{{ ""numErrors"" : {errorCount}, ""numFailures"" : {failuresCount}, ""numSuccesses"" : {successesCount},  ""numIterations"" : {iterationsCount} }}";

            return (eventsDocument, resultsDocument);

            T GetValueOrDefault<T>(Dictionary<string, T> dictionary, string key, T @default) => dictionary.TryGetValue(key, out var value) ? value : @default;
        }

        private static (string EventsJson, string ResultsJson) ExecuteWorkload(string connectionString, BsonDocument driverWorkload, bool async, CancellationToken cancellationToken)
        {
            Environment.SetEnvironmentVariable("MONGODB_URI", connectionString); // force using atlas connection string in our internal test connection strings

            var factory = new TestCaseFactory();
            var testCase = factory.CreateTestCase(driverWorkload, async);
            var eventsCapturer = new AstrolabeEventSubscriber();
            using (var testRunner = new UnifiedTestFormatTestRunner(
                allowKillSessions: false,
                eventsCapturer,
                terminationCancellationToken: cancellationToken))
            {
                testRunner.Run(testCase);
                Console.WriteLine($"dotnet ExecuteWorkload> Returning...");
                return CreateWorkloadResult(entityMap: testRunner.EntityMap, eventsCapturer);
            }
        }

        private static void CancelWorkloadTask(CancellationTokenSource cancellationTokenSource)
        {
            Console.Write($"dotnet cancel workload> Canceling the workload task...");
            cancellationTokenSource.Cancel();
            Console.WriteLine($"Done.");
        }

        private static void HandleCancel(
            ConsoleCancelEventArgs args,
            CancellationTokenSource cancellationTokenSource)
        {
            // We set the Cancel property to true to prevent the process from terminating
            args.Cancel = true;
            CancelWorkloadTask(cancellationTokenSource);
        }

        internal class AstrolabeEventSubscriber : IEventSubscriber
        {
            public readonly ConcurrentQueue<string> Queue = new ConcurrentQueue<string>();

            /// <summary>
            /// 
            /// </summary>
            /// <typeparam name="TEvent"></typeparam>
            /// <param name="handler"></param>
            /// <returns></returns>
            public bool TryGetEventHandler<TEvent>(out Action<TEvent> handler)
            {
                handler = (@event) =>
                {
                    var @eventString = AstrolabeEventsHandler.CreateEventDocument(@event);
                    if (@eventString != null)
                    {
                        Queue.Enqueue(@eventString);
                    }
                };
                return true;
            }
        }

        internal class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            public JsonDrivenTestCase CreateTestCase(BsonDocument driverWorkload, bool async)
            {
                var testCase = CreateTestCases(driverWorkload).Single();
                testCase.Test["async"] = async;

                return testCase;
            }

            protected override string GetTestCaseName(BsonDocument shared, BsonDocument test, int index) =>
                $"Astrolabe command line arguments:{base.GetTestName(test, index)}";
        }
    }
}
