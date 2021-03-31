﻿/* Copyright 2020-present MongoDB Inc.
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
using System.Threading;
using AstrolabeWorkloadExecutor;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Tests.UnifiedTestOperations;

namespace WorkloadExecutor
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            Ensure.IsEqualTo(args.Length, 2, nameof(args.Length));

            var connectionString = args[0];
            var driverWorkload = BsonDocument.Parse(args[1]);
            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. Income document: {driverWorkload}");

            var cancellationTokenSource = new CancellationTokenSource();
            ConsoleCancelEventHandler cancelHandler = (o, e) => HandleCancel(e, cancellationTokenSource);

            var resultsDir = Environment.GetEnvironmentVariable("RESULTS_DIR");
            var eventsPath = Path.Combine(resultsDir ?? "", "events.json");
            var resultsPath = Path.Combine(resultsDir ?? "", "results.json");
            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> Results will be written to {resultsPath},\nEvents will be written to {eventsPath}...");

            Console.CancelKeyPress += cancelHandler;

            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> Starting workload executor...");

            if (!bool.TryParse(Environment.GetEnvironmentVariable("ASYNC"), out bool async))
            {
                async = true;
            }

            var entityMap = ExecuteWorkload(connectionString, driverWorkload, async, cancellationTokenSource.Token);
            var resultDetails = HandleWorkloadResult(entityMap: entityMap);

            Console.CancelKeyPress -= cancelHandler;

            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main finally> Writing final results and events files");
            WriteToFile(resultsPath, resultDetails.ResultsJson);
            WriteToFile(eventsPath, resultDetails.EventsJson);

            // ensure all messages are propagated to the astrolabe time immediately
            Console.Error.Flush();
            Console.Out.Flush();
        }

        private static (string EventsJson, string ResultsJson) HandleWorkloadResult(UnifiedEntityMap entityMap)
        {
            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> HandleWorkloadResult_1");
            Ensure.IsNotNull(entityMap, nameof(entityMap));

            var iterationsCount = GetValueOrDefault(entityMap.IterationCounts, "iterations", @default: -1);
            var successesCount = GetValueOrDefault(entityMap.SuccessCounts, "successes", @default: -1);

            var errorDocuments = GetValueOrDefault(entityMap.ErrorDocumentsMap, "errors", @default: new BsonArray());
            var errorCount  = errorDocuments.Count;
            var failuresDocuments = GetValueOrDefault(entityMap.FailureDocumentsMap, "failures", @default: new BsonArray());
            var failuresCount = failuresDocuments.Count;

            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> HandleWorkloadResult_2");
            string eventsJson = "[]";
            if (entityMap.EventCapturers.TryGetValue("events", out var eventCapturer))
            {
                var formattedEvents = eventCapturer.Events.Select(AstrolabeEventsHandler.CreateEventDocument).Take(1);
                eventsJson = $"[{string.Join(",", formattedEvents)}]";
            }

            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> HandleWorkloadResult_3. Events count: {eventCapturer.Count}");
            var eventsDocument = @$"{{ ""events"" : {eventsJson}, ""errors"" : {errorDocuments}, ""failures"" : {failuresDocuments} }}";

            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> HandleWorkloadResult_4");
            var resultsDocument = $@"{{ ""numErrors"" : {errorCount}, ""numFailures"" : {failuresCount}, ""numSuccesses"" : {successesCount},  ""numIterations"" : {iterationsCount} }}";
            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> HandleWorkloadResult_5");

            Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet main> HandleWorkloadResult_7");
            return (eventsDocument, resultsDocument);

            T GetValueOrDefault<T>(Dictionary<string, T> dictionary, string key, T @default) => dictionary.TryGetValue(key, out var value) ? value : @default;
        }

        private static UnifiedEntityMap ExecuteWorkload(string connectionString, BsonDocument driverWorkload, bool async, CancellationToken cancellationToken)
        {
            Environment.SetEnvironmentVariable("MONGODB_URI", connectionString); // force using atlas connection string in our internal test connection strings

            var factory = new TestCaseFactory();
            var testCase = factory.CreateTestCase(driverWorkload, async);
            using (var testsExecutor = new UnifiedTestFormatExecutor(
                allowKillSessions: false,
                terminationCancellationToken: cancellationToken))
            {
                testsExecutor.Run(testCase);
                Console.WriteLine($"Time:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet ExecuteWorkload> Returning...");
                return testsExecutor.EntityMap;
            }
        }

        private static void CancelWorkloadTask(CancellationTokenSource cancellationTokenSource)
        {
            Console.Write($"\nTime:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. dotnet cancel workload> Canceling the workload task...");
            cancellationTokenSource.Cancel();
            Console.WriteLine($"\nTime:{DateTime.UtcNow:MM/dd/yyyy hh:mm:ss.fff tt}. Done.");
        }

        private static void HandleCancel(
            ConsoleCancelEventArgs args,
            CancellationTokenSource cancellationTokenSource)
        {
            // We set the Cancel property to true to prevent the process from terminating
            args.Cancel = true;
            CancelWorkloadTask(cancellationTokenSource);
        }

        private static void WriteToFile(string path, string json)
        {
            File.WriteAllText(path, json);
        }

        internal class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            public JsonDrivenTestCase CreateTestCase(BsonDocument driverWorkload, bool async)
            {
                var testCase = CreateTestCases(driverWorkload).Single();
                testCase.Test["async"] = async;

                return testCase;
            }

            protected override string GetTestCaseName(BsonDocument shared, BsonDocument test, int index) => $"Astrolabe command line arguments:{base.GetTestName(test, index)}";
        }
    }
}
