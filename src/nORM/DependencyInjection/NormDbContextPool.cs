using System;
using System.Collections.Concurrent;
using System.Threading;
using nORM.Core;

#nullable enable

namespace nORM.DependencyInjection
{
    /// <summary>
    /// A bounded pool of reusable <typeparamref name="TContext"/> instances backing
    /// <c>IServiceCollection.AddNormPool&lt;TContext&gt;</c>. <see cref="Rent"/> returns a warm context (one
    /// previously reset and pooled, or a freshly created one) with a return hook wired so that disposing it —
    /// e.g. when the DI scope ends — resets it and returns it here instead of tearing it down. When a returned
    /// context is not poolable (it holds a live transaction, has been disposed) or the pool is already full,
    /// it is disposed normally. Registered as a singleton; disposing the pool disposes every pooled context.
    /// </summary>
    internal sealed class NormDbContextPool<TContext> : IDisposable
        where TContext : DbContext
    {
        private readonly Func<TContext> _factory;
        private readonly int _maxSize;
        private readonly ConcurrentQueue<TContext> _pool = new();
        private int _count;
        private volatile bool _disposed;

        public NormDbContextPool(Func<TContext> factory, int maxSize)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            if (maxSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxSize), "Pool size must be greater than zero.");
            _maxSize = maxSize;
        }

        /// <summary>Leases a warm context and wires its pool-return hook.</summary>
        public TContext Rent()
        {
            TContext ctx;
            if (_pool.TryDequeue(out var pooled))
            {
                Interlocked.Decrement(ref _count);
                ctx = pooled;
            }
            else
            {
                ctx = _factory()
                    ?? throw new InvalidOperationException("The nORM pool context factory returned null.");
            }

            // Route this context's next Dispose back into the pool instead of tearing it down.
            ctx.SetPoolReturnHook(() => Return(ctx));
            return ctx;
        }

        // Invoked from the context's Dispose via the hook. Returns true when the context was reset and
        // re-pooled (the caller skips teardown); false when it must be fully disposed — the pool is disposed,
        // the context is not poolable (live transaction / already disposed), or the pool is full.
        private bool Return(TContext ctx)
        {
            if (_disposed || !ctx.TryResetForPooling())
                return false;

            if (Interlocked.Increment(ref _count) > _maxSize)
            {
                Interlocked.Decrement(ref _count);
                return false;
            }

            _pool.Enqueue(ctx);
            return true;
        }

        public void Dispose()
        {
            _disposed = true;
            while (_pool.TryDequeue(out var ctx))
            {
                // Clear the hook so disposal tears the context down instead of re-entering the pool.
                ctx.SetPoolReturnHook(null);
                ctx.Dispose();
            }
        }
    }
}
