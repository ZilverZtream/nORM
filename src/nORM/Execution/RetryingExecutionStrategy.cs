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

        public Task<T> ExecuteAsync<T>(Func<DbContext, CancellationToken, Task<T>> operation, CancellationToken ct)
            => ExecuteAsync(operation, null, ct);

        public async Task<T> ExecuteAsync<T>(Func<DbContext, CancellationToken, Task<T>> operation, Func<bool>? isCommitAttempted, CancellationToken ct)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    return await operation(_ctx, ct).ConfigureAwait(false);
                }
                // A1/X1 fix: TimeoutException is excluded. Retrying a timed-out write can duplicate
                // data (the write may have succeeded before the timeout was raised), so timeout
                // exceptions are not retryable on either the query or the save path.
                // This aligns with DbContext.IsRetryableException which already excludes timeouts.
                // The read pipeline wraps provider exceptions in NormException before they reach
                // this strategy, so the filter and the policy predicate both classify by the
                // UNWRAPPED provider exception - otherwise a transient DbException surfacing as
                // NormException escaped without any retry and the policy was dead code on reads.
                catch (Exception ex) when (UnwrapForRetry(ex) is DbException or System.IO.IOException or System.Net.Sockets.SocketException)
                {
                    var providerEx = UnwrapForRetry(ex);
                    var normEx = ex is NormException ? ex as NormException : new NormException(ex.Message, null, null, ex);
                    _ctx.Options.Logger?.LogError(normEx!, retryCount);

                    // Once the operation has attempted its commit the outcome is unknown, so replaying
                    // it could duplicate an already-committed write. Surface the error without retrying,
                    // mirroring the commit-attempted guard on the SaveChanges retry path.
                    if (isCommitAttempted?.Invoke() ?? false)
                        throw normEx!;

                    bool shouldRetry;
                    try
                    {
                        shouldRetry = _policy.ShouldRetry(providerEx);
                    }
                    catch
                    {
                        // If the retry predicate itself throws, treat the error as non-retryable
                        // to avoid masking the original exception.
                        throw normEx!;
                    }

                    if (retryCount >= _policy.MaxRetries || !shouldRetry)
                    {
                        throw normEx!;
                    }
                    // Cap exponent at 30 to prevent unreasonably long delays (2^30 * 1s = ~12 days).
                    // With MaxRetries defaulting to 3 this cap is rarely reached, but it protects
                    // against misconfigured policies with very high MaxRetries values.
                    var cappedRetry = Math.Min(retryCount, 30);
                    var delayMs = Math.Min(
                        _policy.BaseDelay.TotalMilliseconds * Math.Pow(2, cappedRetry),
                        TimeSpan.FromMinutes(5).TotalMilliseconds);
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false);
                    retryCount++;
                }
            }
        }

        /// <summary>
        /// The provider exception a wrapped failure should be classified by: NormException
        /// wrappers are peeled to their innermost non-NormException cause.
        /// </summary>
        private static Exception UnwrapForRetry(Exception ex)
        {
            var current = ex;
            while (current is NormException && current.InnerException is { } inner)
                current = inner;
            return current;
        }
    }
}