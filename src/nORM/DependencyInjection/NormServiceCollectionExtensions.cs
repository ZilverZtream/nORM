using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;

#nullable enable
namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    /// Registers nORM with an <see cref="IServiceCollection"/> so a
    /// <see cref="DbContext"/> participates in dependency injection with a
    /// container-managed lifetime, mirroring the ASP.NET Core hosting model
    /// (<c>builder.Services.AddNorm(...)</c>). The extension methods live in the
    /// <c>Microsoft.Extensions.DependencyInjection</c> namespace so they surface on
    /// <see cref="IServiceCollection"/> without an extra <c>using</c>, following the
    /// standard convention for <c>Microsoft.Extensions.*</c> integrations.
    /// </summary>
    public static class NormServiceCollectionExtensions
    {
        private const string RequiresUnreferencedCodeMessage =
            "nORM builds entity mappings and materializers by reflecting over entity types; " +
            "trimming may remove required members. See docs/aot-trimming.md.";

        private const string RequiresDynamicCodeMessage =
            "nORM uses Expression-based query translation and reflection-emit materializers and " +
            "is not NativeAOT-compatible. See docs/aot-trimming.md.";

        /// <summary>
        /// Registers a <see cref="DbContext"/> built from a connection string and a provider
        /// factory. A fresh <see cref="DbContextOptions"/> is created and configured for each
        /// context, so no configuration state is shared between instances. The default
        /// <see cref="ServiceLifetime.Scoped"/> lifetime yields one context - and one
        /// connection - per DI scope, which the container disposes when the scope ends.
        /// </summary>
        /// <param name="services">The service collection to add the registration to.</param>
        /// <param name="connectionString">The provider connection string.</param>
        /// <param name="providerFactory">
        /// Factory that returns the <see cref="DatabaseProvider"/> for each context. A new
        /// provider is requested per created context.
        /// </param>
        /// <param name="configureOptions">Optional callback to configure per-context options.</param>
        /// <param name="lifetime">
        /// The <see cref="DbContext"/> service lifetime. Defaults to
        /// <see cref="ServiceLifetime.Scoped"/>.
        /// </param>
        /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
        [RequiresUnreferencedCode(RequiresUnreferencedCodeMessage)]
        [RequiresDynamicCode(RequiresDynamicCodeMessage)]
        public static IServiceCollection AddNorm(
            this IServiceCollection services,
            string connectionString,
            Func<DatabaseProvider> providerFactory,
            Action<DbContextOptions>? configureOptions = null,
            ServiceLifetime lifetime = ServiceLifetime.Scoped)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(connectionString);
            ArgumentNullException.ThrowIfNull(providerFactory);

            services.Add(new ServiceDescriptor(
                typeof(DbContext),
                _ => CreateContext(connectionString, providerFactory, configureOptions),
                lifetime));
            return services;
        }

        /// <summary>
        /// Registers a custom <typeparamref name="TContext"/> produced by an explicit factory,
        /// which may resolve additional services from the <see cref="IServiceProvider"/>. The
        /// default <see cref="ServiceLifetime.Scoped"/> lifetime yields one context per DI
        /// scope, disposed by the container at scope end. Inject <typeparamref name="TContext"/>.
        /// </summary>
        /// <param name="services">The service collection to add the registration to.</param>
        /// <param name="contextFactory">Factory that builds the context from the scope's services.</param>
        /// <param name="lifetime">
        /// The context service lifetime. Defaults to <see cref="ServiceLifetime.Scoped"/>.
        /// </param>
        /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
        public static IServiceCollection AddNorm<TContext>(
            this IServiceCollection services,
            Func<IServiceProvider, TContext> contextFactory,
            ServiceLifetime lifetime = ServiceLifetime.Scoped)
            where TContext : DbContext
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(contextFactory);

            services.Add(new ServiceDescriptor(typeof(TContext), sp => contextFactory(sp), lifetime));
            return services;
        }

        /// <summary>
        /// Registers an <see cref="INormDbContextFactory{TContext}"/> singleton that produces
        /// caller-owned <typeparamref name="TContext"/> instances on demand. Use it for
        /// singletons, background services, or parallel work that must control context
        /// lifetime explicitly instead of relying on a DI scope. Contexts returned by the
        /// factory are owned - and must be disposed - by the caller.
        /// </summary>
        /// <param name="services">The service collection to add the registration to.</param>
        /// <param name="contextFactory">
        /// Factory that builds a context from the root <see cref="IServiceProvider"/>.
        /// </param>
        /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
        public static IServiceCollection AddNormFactory<TContext>(
            this IServiceCollection services,
            Func<IServiceProvider, TContext> contextFactory)
            where TContext : DbContext
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(contextFactory);

            services.TryAddSingleton<INormDbContextFactory<TContext>>(
                sp => new NormDbContextFactory<TContext>(sp, contextFactory));
            return services;
        }

        [RequiresUnreferencedCode(RequiresUnreferencedCodeMessage)]
        [RequiresDynamicCode(RequiresDynamicCodeMessage)]
        private static DbContext CreateContext(
            string connectionString,
            Func<DatabaseProvider> providerFactory,
            Action<DbContextOptions>? configureOptions)
        {
            var provider = providerFactory()
                ?? throw new InvalidOperationException("The nORM provider factory returned null.");

            DbContextOptions? options = null;
            if (configureOptions is not null)
            {
                options = new DbContextOptions();
                configureOptions(options);
            }

            return new DbContext(connectionString, provider, options);
        }
    }
}
