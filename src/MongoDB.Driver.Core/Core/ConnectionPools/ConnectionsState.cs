/*Copyright 2021 - present MongoDB Inc.
 *
* Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
*Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using MongoDB.Bson;

namespace MongoDB.Driver.Core.ConnectionPools
{
    internal class ConnectionsState
    {
        private readonly Dictionary<ObjectId, ServiceState> _serviceStates = new();
        private object _lock = new object();

        public bool TryGetGeneration(ObjectId objectId, out int generation)
        {
            lock (_lock)
            {
                if (_serviceStates.TryGetValue(objectId, out var serviceState))
                {
                    generation = serviceState.Generation.Value;
                    return true;
                }
                else
                {
                    generation = -1; // should not be reached
                    return false;
                }
            }
        }

        public void AddConnectionState(ObjectId serviceId)
        {
            lock (_lock)
            {
                if (!_serviceStates.TryGetValue(serviceId, out var serviceState))
                {
                    _serviceStates.Add(serviceId, new ServiceState(generation: 0, connectionCount: 0));
                }
                else
                {
                    serviceState.ConnectionsCount.Increment();
                }
            }
        }

        public void RemoveConnectionState(ObjectId serviceId)
        {
            lock (_lock)
            {
                if (!_serviceStates.TryGetValue(serviceId, out var serviceState))
                {
                    // should not be reached
                    throw new InvalidOperationException("RemoveConnection has no target.");
                }
                if (serviceState.ConnectionsCount.DecrementAndReturn() == 0)
                {
                    _serviceStates.Remove(serviceId);
                }
            }
        }

        public void IncreamentGenerationAndCleanConnections(ObjectId serviceId)
        {
            lock (_lock)
            {
                if (!_serviceStates.TryGetValue(serviceId, out var serviceState))
                {
                    throw new InvalidOperationException("Generation increment failure.");
                }
                serviceState.Generation.Increment();
            }
        }

        // nested types
        //private class ServiceState
        //{
        //    private int _connectionCount;
        //    private int _generation;

        //    public ServiceState(int generation, int connectionCount)
        //    {
        //        _connectionCount = connectionCount;
        //        _generation = generation;
        //    }

        //    public int ConnectionsCount => _connectionCount;

        //    public int Generation => _generation;

        //    public void IncrementGeneration()
        //    {
        //        Interlocked.Increment(ref _generation);
        //    }

        //    public void IncrementConnectionsCount()
        //    {
        //        Interlocked.Increment(ref _connectionCount);
        //    }

        //    public int DecrementConnectionsCount()
        //    {
        //        return Interlocked.Decrement(ref _connectionCount);
        //    }

        //    public void ResetConnections()
        //    {
        //        Interlocked.CompareExchange(ref _connectionCount, value: 0, comparand: _connectionCount);
        //    }
        //}
        private class ServiceState
        {
            private Counter _generationCounter;
            private Counter _connectionCountCounter;

            public ServiceState(int generation, int connectionCount)
            {
                _connectionCountCounter = new Counter(connectionCount);
                _generationCounter = new Counter(generation);
            }

            public Counter ConnectionsCount => _connectionCountCounter;

            public Counter Generation => _generationCounter;
        }

        private class Counter
        {
            private int _value;

            public Counter(int value) => _value = value;

            public int Value => _value;
            public void Increment() => _value++;
            public int DecrementAndReturn() => _value--;
        }
    }
}
