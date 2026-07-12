using System;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;

#nullable enable

namespace nORM.Execution
{
    internal interface IExecutionStrategy
    {
        Task<T> ExecuteAsync<T>(Func<DbContext, CancellationToken, Task<T>> operation, CancellationToken ct);

        /// <summary>
        /// Executes <paramref name="operation"/> with retry, but never retries once
        /// <paramref name="isCommitAttempted"/> reports <c>true</c>. A commit whose outcome is
        /// unknown must not be replayed - retrying it could duplicate an already-committed write.
        /// The operation is responsible for resetting the underlying flag at the start of each
        /// attempt and setting it immediately before it commits.
        /// </summary>
        Task<T> ExecuteAsync<T>(Func<DbContext, CancellationToken, Task<T>> operation, Func<bool>? isCommitAttempted, CancellationToken ct);
    }
}