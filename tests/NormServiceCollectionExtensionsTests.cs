using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies the dependency-injection registration surface: scoped context lifetime,
/// container-driven disposal, per-scope isolation, option configuration, and the
/// caller-owned context factory.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public sealed class NormServiceCollectionExtensionsTests
{
    private sealed class TrackingContext : DbContext
    {
        public TrackingContext(DbConnection connection, DatabaseProvider provider)
            : base(connection, provider)
        {
        }
    }

    private static SqliteConnection NewOpenConnection()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    [Fact]
    public void AddNorm_generic_resolves_one_scoped_context_per_scope_and_disposes_it_with_the_scope()
    {
        // A single connection owned by one context lets us observe container-driven disposal:
        // when the scope ends, the container disposes the context, which closes its connection.
        var connection = NewOpenConnection();
        var services = new ServiceCollection();
        services.AddNorm<TrackingContext>(_ => new TrackingContext(connection, new SqliteProvider()));

        using var provider = services.BuildServiceProvider();

        using (var scope = provider.CreateScope())
        {
            var first = scope.ServiceProvider.GetRequiredService<TrackingContext>();
            var second = scope.ServiceProvider.GetRequiredService<TrackingContext>();

            Assert.Same(first, second);
            Assert.Equal(ConnectionState.Open, connection.State);
        }

        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void AddNorm_generic_yields_distinct_contexts_across_scopes()
    {
        var services = new ServiceCollection();
        services.AddNorm<TrackingContext>(_ => new TrackingContext(NewOpenConnection(), new SqliteProvider()));

        using var provider = services.BuildServiceProvider();

        TrackingContext fromFirstScope;
        TrackingContext fromSecondScope;
        using (var scope = provider.CreateScope())
            fromFirstScope = scope.ServiceProvider.GetRequiredService<TrackingContext>();
        using (var scope = provider.CreateScope())
            fromSecondScope = scope.ServiceProvider.GetRequiredService<TrackingContext>();

        Assert.NotSame(fromFirstScope, fromSecondScope);
    }

    [Fact]
    public void AddNorm_connection_string_overload_builds_a_context_and_applies_configure_options()
    {
        var configured = false;
        var services = new ServiceCollection();
        services.AddNorm(
            "Data Source=:memory:",
            () => new SqliteProvider(),
            _ => configured = true);

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var context = scope.ServiceProvider.GetRequiredService<DbContext>();

        Assert.NotNull(context);
        Assert.True(configured, "configureOptions must run when the context is created.");
    }

    [Fact]
    public void AddNormFactory_registers_a_singleton_factory_that_creates_caller_owned_contexts()
    {
        var services = new ServiceCollection();
        services.AddNormFactory(_ => new TrackingContext(NewOpenConnection(), new SqliteProvider()));

        using var provider = services.BuildServiceProvider();

        var factory = provider.GetRequiredService<INormDbContextFactory<TrackingContext>>();
        var sameFactory = provider.GetRequiredService<INormDbContextFactory<TrackingContext>>();
        Assert.Same(factory, sameFactory);

        using var first = factory.CreateDbContext();
        using var second = factory.CreateDbContext();

        Assert.NotNull(first);
        Assert.NotSame(first, second);
    }
}
