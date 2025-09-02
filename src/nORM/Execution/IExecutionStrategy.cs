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
    }
}