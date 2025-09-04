using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Enterprise;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Execution
{
    internal sealed class RetryingExecutionStrategy : IExecutionStrategy
    {
        private readonly DbContext _ctx;
        private readonly RetryPolicy _policy;

        public RetryingExecutionStrategy(DbContext ctx, RetryPolicy policy)
        {
            _ctx = ctx;
            _policy = policy;
        }

        public async Task<T> ExecuteAsync<T>(Func<DbContext, CancellationToken, Task<T>> operation, CancellationToken ct)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    return await operation(_ctx, ct).ConfigureAwait(false);
                }
                catch (DbException ex)
                {
                    var normEx = new NormException(ex.Message, null, null, ex);
                    _ctx.Options.Logger?.LogError(normEx, retryCount);

                    if (retryCount >= _policy.MaxRetries || !_policy.ShouldRetry(ex))
                    {
                        throw normEx;
                    }
                    var delay = TimeSpan.FromMilliseconds(_policy.BaseDelay.TotalMilliseconds * Math.Pow(2, retryCount));
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    retryCount++;
                }
            }
        }
    }
}