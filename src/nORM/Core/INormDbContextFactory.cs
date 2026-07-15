using System;

#nullable enable
namespace nORM.Core
{
    /// <summary>
    /// Creates <typeparamref name="TContext"/> instances on demand, independent of any
    /// dependency-injection scope. Register it with
    /// <c>IServiceCollection.AddNormFactory&lt;TContext&gt;(...)</c> and inject it into
    /// singletons, background services, or components that need to control a context's
    /// lifetime explicitly - for example one short-lived context per unit of work.
    /// </summary>
    /// <remarks>
    /// Unlike a scope-resolved <see cref="DbContext"/>, contexts returned by
    /// <see cref="CreateDbContext"/> are owned by the caller and must be disposed by the
    /// caller (ideally with <c>await using</c>).
    /// </remarks>
    /// <typeparam name="TContext">The context type produced by the factory.</typeparam>
    public interface INormDbContextFactory<out TContext>
        where TContext : DbContext
    {
        /// <summary>
        /// Creates a new <typeparamref name="TContext"/>. The caller owns the returned
        /// instance and is responsible for disposing it.
        /// </summary>
        TContext CreateDbContext();
    }

    /// <summary>
    /// Default <see cref="INormDbContextFactory{TContext}"/> that delegates to a
    /// user-supplied factory, resolving dependencies from the root service provider so
    /// created contexts do not capture a per-request scope.
    /// </summary>
    internal sealed class NormDbContextFactory<TContext> : INormDbContextFactory<TContext>
        where TContext : DbContext
    {
        private readonly IServiceProvider _services;
        private readonly Func<IServiceProvider, TContext> _factory;

        public NormDbContextFactory(IServiceProvider services, Func<IServiceProvider, TContext> factory)
        {
            _services = services;
            _factory = factory;
        }

        public TContext CreateDbContext() => _factory(_services);
    }
}
