using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Hangfire.Elasticsearch.Tests.Helpers
{
    public static class TaskHelper
    {
        public static Task<TimedTaskResult<T>> RunTimedTask<T>(Func<T> action)
        {
            return Task.Run(() =>
            {
                var sw = Stopwatch.StartNew();
                var result = action();
                sw.Stop();

                return new TimedTaskResult<T>(sw.Elapsed, result);
            });
        }

        public struct TimedTaskResult<T>
        {
            public TimedTaskResult(TimeSpan executionDuration, T result)
            {
                ExecutionDuration = executionDuration;
                Result = result;
            }

            public TimeSpan ExecutionDuration { get; }
            public T Result { get; }
        }
    }
}