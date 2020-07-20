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
using FluentAssertions;
using MongoDB.Driver.Core.Misc;
using Xunit;

namespace MongoDB.Driver.Core.Tests.Core.Misc
{
    public class ExceptionHelperTests
    {
        [Theory]
        [MemberData(nameof(Exception_equals_test_cases))]
        public void Equals_should_return_expected_result(Exception a, Exception b, bool expectedResult)
        {
            var result = ExceptionHelper.Equals(a, b);
            result.Should().Be(expectedResult);
        }

        public static IEnumerable<object[]> Exception_equals_test_cases()
        {
            yield return new object[] { null, null, true };

            var exception = new Exception();
            yield return new object[] { exception, exception, true };

            yield return new object[] { null, exception, false};
            yield return new object[] { exception, null, false };

            yield return new object[] { exception, new ArgumentException(), false };
            yield return new object[] { new ArgumentException(), exception, false };

            var exceptionWithInnerException = new Exception("WithInnerException", exception);
            yield return new object[] { exceptionWithInnerException, exception, false };
            yield return new object[] { exception, exceptionWithInnerException, false };

            var exceptionWithDifferentInnerException = new Exception("WithInnerException", new ArgumentException());
            yield return new object[] { exceptionWithInnerException, exceptionWithDifferentInnerException, false };
            yield return new object[] { exceptionWithDifferentInnerException, exceptionWithInnerException, false };

            var exceptionWithALotInnerException = new Exception("main", new Exception("inner1", new Exception("inner3", new Exception())));
            var exceptionWithALotInnerExceptionAndDifferentLastMessage = new Exception("main", new Exception("inner1", new Exception("differentInner3", new Exception())));
            yield return new object[] { exceptionWithALotInnerException, exceptionWithALotInnerExceptionAndDifferentLastMessage, false };
            yield return new object[] { exceptionWithALotInnerExceptionAndDifferentLastMessage, exceptionWithALotInnerException, false };

            var exceptionWithStackTrace = new TestException("ex", "stack");
            var exceptionWithDifferentStackTrace = new TestException("ex", "stackDiff");
            yield return new object[] { exceptionWithStackTrace, exceptionWithDifferentStackTrace, false };
            yield return new object[] { exceptionWithDifferentStackTrace, exceptionWithStackTrace, false };
        }

#pragma warning disable CA1064 // Exceptions should be public
        private class TestException : Exception
#pragma warning restore CA1064 // Exceptions should be public
        {
            private string _emulatedStackTrace;

            public TestException(string message, string stackTrace) : base(message)
            {
                _emulatedStackTrace = stackTrace;
            }


            public override string StackTrace
            {
                get
                {
                    return _emulatedStackTrace;
                }
            }
        }
    }
}
