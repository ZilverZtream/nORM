using System;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Internal
{
    /// <summary>
    /// Provides helpers for executing asynchronous operations synchronously
    /// without risking deadlocks by using a dedicated task factory with the
    /// default task scheduler.
    /// </summary>
    internal static class AsyncExecutionContext
    {
        private static readonly TaskFactory UnboundTaskFactory = new(
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskContinuationOptions.None,
            TaskScheduler.Default);

        public static void RunSync(Func<Task> task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            UnboundTaskFactory
                .StartNew(task)
                .Unwrap()
                .GetAwaiter()
                .GetResult();
        }

        public static T RunSync<T>(Func<Task<T>> task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            return UnboundTaskFactory
                .StartNew(task)
                .Unwrap()
                .GetAwaiter()
                .GetResult();
        }
    }
}
