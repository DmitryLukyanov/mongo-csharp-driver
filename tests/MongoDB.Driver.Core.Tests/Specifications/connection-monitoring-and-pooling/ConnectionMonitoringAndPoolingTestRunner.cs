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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.ConnectionPools;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Helpers;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using Moq;
using Xunit;

namespace MongoDB.Driver.Specifications.connection_monitoring_and_pooling
{
    public class ConnectionMonitoringAndPoolingTestRunner
    {
        #region static
        private static readonly string[] __commonIgnoredEvents =
        {
            //nameof(ConnectionPoolOpenedEvent),
            nameof(ConnectionPoolOpeningEvent),
            nameof(ConnectionPoolAddingConnectionEvent),
            nameof(ConnectionPoolCheckingInConnectionEvent),
            nameof(ConnectionPoolRemovingConnectionEvent),
            //nameof(ConnectionPoolRemovedConnectionEvent),
            nameof(ConnectionPoolClosingEvent),
            nameof(ConnectionPoolClearingEvent)
        };
        #endregion

        [SkippableTheory]
        [ClassData(typeof(TestCaseFactory))]
        public void RunTestDefinition(JsonDrivenTestCase testCase)
        {
            var connectionMap = new ConcurrentDictionary<string, IConnection>();
            var eventCapturer = new Lazy<EventCapturer>(isThreadSafe: true);
            var tasks = new ConcurrentDictionary<string, Task>();

            var test = testCase.Test;
            JsonDrivenHelper.EnsureAllFieldsAreValid(test, "_path", "version", "style", "description", "poolOptions", "operations", "error", "events", "ignore");
            EnsureAvailableStyle(test);
            ResetConnectionId();

            var connectionPool = new Lazy<IConnectionPool>(
                () =>
                {
                    var pool = SetupConnectionPool(test, eventCapturer.Value);
                    pool.Initialize();
                    return pool;
                },
                true);

            var operations = testCase.Test.GetValue("operations").AsBsonArray;
            foreach (var operation in operations.Cast<BsonDocument>())
            {
                ExecuteOperation(
                    operation,
                    eventCapturer.Value,
                    connectionMap,
                    tasks,
                    connectionPool,
                    out var connectionPoolException);

                if (connectionPoolException != null)
                {
                    AssertError(test, connectionPoolException);
                    break;
                }
            }

            AssertEvents(test, eventCapturer.Value);
        }

        // private methods
        private void AssertError(BsonDocument test, Exception ex)
        {
            var containsErrorNode = test.Contains("error");
            if (!containsErrorNode && ex != null)
            {
                throw new Exception("Unexpected exception has been thrown.", ex);
            }
            else if (containsErrorNode && ex == null)
            {
                throw new Exception($"The test was expected to throw an exception {test["error"]}, but no exception was thrown.");
            }
            else if (containsErrorNode)
            {
                var error = test["error"].AsBsonDocument;
                JsonDrivenHelper.EnsureAllFieldsAreValid(error, "type", "message");
                var exType = MapErrorTypeToExpected(ex);
                var exMessage = MapErrorMessageToExpected(ex.Message);

                var expectedExceptionType = error["type"].ToString();
                var expectedErrorMessage = error["message"].ToString();

                exType.Should().Be(expectedExceptionType);
                exMessage.Should().Be(expectedErrorMessage);
            }
        }

        private void AssertEvent(object actualEvent, BsonDocument expectedEvent)
        {
            var actualType = actualEvent.GetType().Name;
            var expectedType = expectedEvent.GetValue("type").ToString();
            actualType.Should().Be(expectedType);
            if (expectedEvent.Contains("connectionId"))
            {
                var expectedConnectionId = expectedEvent["connectionId"].ToInt32();
                if (expectedConnectionId == 42) // 42 - placeholder
                {
                    actualEvent.ConnectionId().Should().NotBeNull();
                }
                else
                {
                    actualEvent.ConnectionId().LocalValue.Should().Be(expectedConnectionId);
                }
            }

            if (expectedEvent.Contains("options"))
            {
                var connectionPoolSettings = actualEvent.ConnectionPoolSettings();
                var expectedOption = expectedEvent["options"];
                if (expectedOption.IsInt32 && expectedOption == 42) // 42 - placeholder
                {
                    connectionPoolSettings.Should().NotBeNull();
                }
                else
                {
                    var expectedMaxPoolSize = expectedOption["maxPoolSize"].ToInt32();
                    connectionPoolSettings.MaxConnections.Should().Be(expectedMaxPoolSize);
                    var expectedMinPoolSize = expectedOption["minPoolSize"].ToInt32();
                    connectionPoolSettings.MinConnections.Should().Be(expectedMinPoolSize);
                    // maxIdleTimeMS : todo:
                }
            }

            if (expectedEvent.Contains("address"))
            {
                var expectedAddress = expectedEvent["address"].ToInt32();
                if (expectedAddress == 42)
                {
                    actualEvent.ServerId().EndPoint.Should().NotBeNull();
                }
                else
                {
                    throw new NotImplementedException();
                }
            }

            //if (expectedEvent.Contains("reason"))
            //{
            //    //throw new NotImplementedException();
            //}
        }

        private void AssertEvents(BsonDocument test, EventCapturer eventCapturer)
        {
            var actualEvents = GetFilteredEvents(eventCapturer, test);
            var expectedEvents = GetExpectedEvents(test);
            var minCount = Math.Min(actualEvents.Count, expectedEvents.Count);
            for (var i = 0; i < minCount; i++)
            {
                var expectedEvent = expectedEvents[i];
                JsonDrivenHelper.EnsureAllFieldsAreValid(expectedEvent, "type", "address", "connectionId", "options", "reason");
                AssertEvent(actualEvents[i], expectedEvent);
            }

            if (actualEvents.Count < expectedEvents.Count)
            {
                throw new Exception($"Missing event: {expectedEvents[actualEvents.Count]}.");
            }

            if (actualEvents.Count > expectedEvents.Count)
            {
                throw new Exception($"Unexpected event of type: {actualEvents[expectedEvents.Count].GetType().Name}.");
            }
        }

        private void EnsureAvailableStyle(BsonDocument test)
        {
            var style = test.GetValue("style").ToString();
            switch (style)
            {
                case "unit":
                    return;
                default:
                    throw new ArgumentException($"Unknown style {style}.");
            }
        }

        private void ExecuteCheckIn(BsonDocument operation, ConcurrentDictionary<string, IConnection> map, out Exception exception)
        {
            exception = null;
            var connectionName = operation.GetValue("connection").ToString();
            if (map.TryGetValue(connectionName, out var connection))
            {
                exception = Record.Exception(() => connection.Dispose());
            }
            else
            {
                throw new Exception("Connection must have a label.");
            }
        }

        private void ExecuteCheckOut(
            IConnectionPool connectionPool,
            BsonDocument operation,
            ConcurrentDictionary<string, IConnection> map,
            ConcurrentDictionary<string, Task> tasks,
            out Exception exception)
        {
            exception = null;

            if (operation.TryGetValue("thread", out var thread))
            {
                var target = thread.ToString();
                if (!tasks.ContainsKey(target))
                {
                    throw new ArgumentException($"Task {target} must be started before usage.");
                }
                else
                {
                    if (tasks[target] != null)
                    {
                        throw new Exception($"Task {target} must not be processed.");
                    }
                    else
                    {
                        tasks[target] = CreateTask(() => CheckOut(operation, connectionPool, map));
                    }
                }
            }
            else
            {
                exception = CheckOut(operation, connectionPool, map);
            }

            Exception CheckOut(BsonDocument op, IConnectionPool cp, ConcurrentDictionary<string, IConnection> cm)
            {
                IConnection conn = null;
                var ex = Record.Exception(() => conn = cp.AcquireConnection(CancellationToken.None));
                if (op.TryGetValue("label", out var label))
                {
                    cm.GetOrAdd(label.ToString(), conn);
                }
                else
                {
                    // do nothing
                }
                return ex;
            }
        }

        private void ExecuteOperation(
            BsonDocument operation,
            EventCapturer eventCapturer,
            ConcurrentDictionary<string, IConnection> connectionMap,
            ConcurrentDictionary<string, Task> tasks,
            Lazy<IConnectionPool> connectionPool,
            out Exception connectionPoolException)
        {
            connectionPoolException = null;
            var name = operation.GetValue("name").ToString();

            switch (name)
            {
                case "checkIn":
                    ExecuteCheckIn(operation, connectionMap, out connectionPoolException);
                    break;
                case "checkOut":
                    ExecuteCheckOut(connectionPool.Value, operation, connectionMap, tasks, out connectionPoolException);
                    break;
                case "clear":
                    connectionPool.Value.Clear();
                    break;
                case "close":
                    connectionPool.Value.Dispose();
                    break;
                case "start":
                    JsonDrivenHelper.EnsureAllFieldsAreValid(operation, "name", "target");
                    Start(operation, tasks);
                    break;
                case "wait":
                    var ms = operation.GetValue("ms").ToInt32();
                    Thread.Sleep(TimeSpan.FromMilliseconds(ms));
                    break;
                case "waitForEvent":
                    JsonDrivenHelper.EnsureAllFieldsAreValid(operation, "name", "event", "count");
                    WaitForEvent(connectionPool, eventCapturer, operation);
                    break;
                case "waitForThread":
                    JsonDrivenHelper.EnsureAllFieldsAreValid(operation, "name", "target");
                    WaitForThread(operation, tasks);
                    break;
                default:
                    throw new ArgumentException($"Unknown operation {name}.");
            }
        }

        private List<BsonDocument> GetExpectedEvents(BsonDocument test)
        {
            var expectedEvents = test
                .GetValue("events")
                .AsBsonArray
                .Select(e =>
                {
                    var expectedType = e["type"].ToString();
                    var mappedType = MapExpectedEventName(expectedType);
                    if (mappedType != null)
                    {
                        e["type"] = mappedType;
                    }
                    return e;
                })
                .Cast<BsonDocument>()
                .ToList();

            return expectedEvents;
        }

        private List<object> GetFilteredEvents(EventCapturer eventCapturer, BsonDocument test)
        {
            var commonIgnoredEvents = new List<string>();
            commonIgnoredEvents.AddRange(__commonIgnoredEvents);
            if (test.TryGetValue("ignore", out var ignore))
            {
                var ignoredEvents = ignore
                    .AsBsonArray
                    .Select(c => MapExpectedEventName(c.ToString()))
                    .Where(c => c != null)
                    .ToList();
                commonIgnoredEvents.AddRange(ignoredEvents);
            }

            return eventCapturer.Events.Where(c => commonIgnoredEvents.Contains(c.GetType().Name.ToString()) != true).ToList();
        }

        private Task CreateTask(Func<Exception> action)
        {
            return Task.Factory.StartNew(
                () =>
                {
                    // todo: ex
                    var ex = action;
                }, 
                CancellationToken.None, 
                TaskCreationOptions.LongRunning, 
                new ThreadPerTaskScheduler());
        }

        private string MapErrorTypeToExpected(Exception exception)
        {
            switch (exception.GetType().Name)
            {
                case nameof(ObjectDisposedException):
                    return "PoolClosedError";
                default:
                    return exception.ToString();
            }
        }

        private string MapErrorMessageToExpected(string errorMessage)
        {
            switch (errorMessage)
            {
                case "Cannot access a disposed object.\r\nObject name: 'ExclusiveConnectionPool'.":
                    return "Attempted to check out a connection from closed connection pool";
                default:
                    return errorMessage;
            }
        }

        private string MapExpectedEventName(string expectedEventName)
        {
            switch (expectedEventName)
            {
                // connection pool
                case "ConnectionPoolCreated":
                    //return nameof(ConnectionPoolOpeningEvent);
                    return nameof(ConnectionPoolOpenedEvent);
                case "ConnectionPoolClosed":
                    return nameof(ConnectionPoolClosedEvent);
                case "ConnectionPoolCleared":
                    return nameof(ConnectionPoolClearedEvent);
                case "ConnectionReady":
                    // ignore for all tests
                    return null;

                // check in
                case "ConnectionCheckedIn":
                    return nameof(ConnectionPoolCheckedInConnectionEvent);

                // check out
                case "ConnectionCheckedOut":
                    return nameof(ConnectionPoolCheckedOutConnectionEvent);
                case "ConnectionCheckOutFailed":
                    return nameof(ConnectionPoolCheckingOutConnectionFailedEvent);
                case "ConnectionCheckOutStarted":
                    return nameof(ConnectionPoolCheckingOutConnectionEvent);
                case "ConnectionClosed":
                    //return nameof(ConnectionClosedEvent);
                    return nameof(ConnectionPoolRemovedConnectionEvent);
                case "ConnectionCreated":
                    return nameof(ConnectionPoolAddedConnectionEvent);

                default:
                    throw new ArgumentException($"Unexpected event name {expectedEventName}.");
            }
        }

        private void ParseSettings(
            BsonDocument test,
            out ConnectionPoolSettings connectionPoolSettings,
            out ConnectionSettings connectionSettings)
        {
            connectionPoolSettings = new ConnectionPoolSettings();
            connectionSettings = new ConnectionSettings();

            if (test.Contains("poolOptions"))
            {
                var poolOptionsDocument = test["poolOptions"].AsBsonDocument;
                foreach (var poolOption in poolOptionsDocument.Elements)
                {
                    switch (poolOption.Name)
                    {
                        case "maxPoolSize":
                            connectionPoolSettings = connectionPoolSettings.With(maxConnections: poolOption.Value.ToInt32());
                            break;
                        case "minPoolSize":
                            connectionPoolSettings = connectionPoolSettings.With(minConnections: poolOption.Value.ToInt32());
                            break;
                        case "waitQueueTimeoutMS":
                            connectionPoolSettings = connectionPoolSettings.With(waitQueueTimeout: TimeSpan.FromMilliseconds(poolOption.Value.ToInt32()));
                            break;
                        case "maxIdleTimeMS":
                            connectionSettings = connectionSettings.With(maxIdleTime: TimeSpan.FromMilliseconds(poolOption.Value.ToInt32()));
                            break;
                        default:
                            throw new ArgumentException($"Unknown pool option {poolOption.Name}.");
                    }
                }
            }
        }

        public void ResetConnectionId()
        {
            IdGeneratorReflector.__lastId(0);
        }

        private IConnectionPool SetupConnectionPool(BsonDocument test, IEventSubscriber eventSubscriber)
        {
            var endPoint = new DnsEndPoint("localhost", 27017);
            var serverId = new ServerId(new ClusterId(), endPoint);
            ParseSettings(test, out var connectionPoolSettings, out var connectionSettings);

            var connectionFactory = new Mock<IConnectionFactory>();
            connectionFactory
                .Setup(c => c.CreateConnection(serverId, endPoint))
                .Returns(() =>
                {
                    var connection = new MockConnection(serverId, connectionSettings);
                    connection.Open(CancellationToken.None);
                    return connection;
                });
            var connectionPool = new ExclusiveConnectionPool(
                serverId,
                endPoint,
                connectionPoolSettings,
                connectionFactory.Object,
                eventSubscriber);

            return connectionPool;
        }

        private void Start(BsonDocument operation, ConcurrentDictionary<string, Task> tasks)
        {
            var startTarget = operation.GetValue("target").ToString();
            tasks.GetOrAdd(startTarget, (Task)null);
        }

        // todo: refactor this method
        private void WaitForEvent(Lazy<IConnectionPool> lazyConnectionPool, EventCapturer eventCapturer, BsonDocument operation)
        {
            var eventType = MapExpectedEventName(operation.GetValue("event").ToString());
            if (eventType == null)
            {
                return;
            }

            var expectedCount = operation.GetValue("count").ToInt32();
            eventCapturer.SetNotifyWhenCondition(queue =>
            {
                return queue.Count(c => c.GetType().Name == eventType) >= expectedCount;
            });
            Task.WaitAny(eventCapturer.NotifyWhenTaskCompletionSource.Task);
        }

        private void WaitForThread(BsonDocument operation, ConcurrentDictionary<string, Task> tasks)
        {
            var waitThread = operation.GetValue("target").ToString();
            if (tasks.TryGetValue(waitThread, out var task) && task != null)
            {
                task.Wait();
            }
            else
            {
                throw new Exception($"The task {waitThread} must be configured before waiting.");
            }
        }

        // nested types
        private class TestCaseFactory : JsonDrivenTestCaseFactory
        {
            #region static
            private static readonly string[] __ignoredTestNames =
            {
                //"pool-checkin.json", //done
                //"connection-must-have-id.json", //done 
                //"connection-must-order-ids.json", //done 
                //"pool-close-destroy-conns.json", // done: ConnectionClosed => ConnectionPoolRemovedConnectionEvent
                //"pool-checkin-destroy-closed.json", // done: but `public void Return(PooledConnection connection)`
                //"pool-checkin-destroy-stale.json", // wrong events order in the end: 
                ////[2]: {{ "type" : "ConnectionPoolCheckedInConnectionEvent", "connectionId" : 1 }}
                ////[3]: {{ "type" : "ConnectionPoolRemovedConnectionEvent", "connectionId" : 1, "reason" : "stale" }}
                //"pool-checkin-make-available.json",
                //"pool-checkout-connection.json", //done
                //"pool-checkout-error-closed.json", //done but should be reviewed. Changes in events
                //"pool-checkout-no-stale.json", //done
                //"pool-close.json", //done
                //"pool-checkout-no-idle.json", //done
                //"pool-create.json", // done, require refactoring
                //"pool-create-min-size.json", // done but with wrong order
                //"pool-create-with-options.json", //done
                //"pool-checkout-multiple.json", // done, but should be checked

                //"wait-queue-fairness.json", //thread
                //"wait-queue-timeout.json", //thread
                //"pool-create-max-size.json" //thread
            };
            #endregion

            protected override string PathPrefix => "MongoDB.Driver.Core.Tests.Specifications.connection_monitoring_and_pooling.tests.";

            protected override IEnumerable<JsonDrivenTestCase> CreateTestCases(BsonDocument document)
            {
                var version = document.GetValue("version", null)?.ToInt32() ?? 1;
                var name = GetTestCaseName(document, document, version);
                if (__ignoredTestNames.Any(c => name.Contains(c)))
                {
                    yield break;
                }
                yield return new JsonDrivenTestCase(name, document, document);
            }
        }

        private class ThreadPerTaskScheduler : TaskScheduler
        {
            /// <summary>Gets the tasks currently scheduled to this scheduler.</summary> 
            /// <remarks>This will always return an empty enumerable, as tasks are launched as soon as they're queued.</remarks> 
            protected override IEnumerable<Task> GetScheduledTasks() { return Enumerable.Empty<Task>(); }

            /// <summary>Starts a new thread to process the provided task.</summary> 
            /// <param name="task">The task to be executed.</param> 
            protected override void QueueTask(Task task)
            {
                new Thread(() => TryExecuteTask(task)) { IsBackground = true }.Start();
            }

            /// <summary>Runs the provided task on the current thread.</summary> 
            /// <param name="task">The task to be executed.</param> 
            /// <param name="taskWasPreviouslyQueued">Ignored.</param> 
            /// <returns>Whether the task could be executed on the current thread.</returns> 
            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
            {
                return TryExecuteTask(task);
            }
        }
    }

    // todo: rename
    internal static class CmapEventsReflector
    {
        public static ConnectionId ConnectionId(this object @event)
        {
            return (ConnectionId)Reflector.GetPropertyValue(@event, nameof(ConnectionId), BindingFlags.Public | BindingFlags.Instance);
        }

        public static ConnectionPoolSettings ConnectionPoolSettings(this object @event)
        {
            return (ConnectionPoolSettings)Reflector.GetPropertyValue(@event, nameof(ConnectionPoolSettings), BindingFlags.Public | BindingFlags.Instance);
        }

        public static ServerId ServerId(this object @event)
        {
            return (ServerId)Reflector.GetPropertyValue(@event, nameof(ServerId), BindingFlags.Public | BindingFlags.Instance);
        }
    }

    internal static class IdGeneratorReflector
    {
        public static void __lastId(object value) => Reflector.SetStaticField(typeof(IdGenerator<ConnectionId>), nameof(__lastId), value);
    }
}
