using System;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Sample.Store;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
[Collection(LiveTemporalProviderCollection.Name)]
public class TenantTemporalProviderSwapTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Store_scenario_combines_tenant_boundary_and_temporal_history_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            if (Skip.If(IsProtectedTemporalDatabase(kind, connection.Database),
                    $"Store tenant/temporal live scenario requires an application database/schema; current {kind} database '{connection.Database}' is provider-owned and rejects temporal DDL."))
                return;

            var result = await StoreScenario.RunAsync(connection, provider, ToStoreProvider(kind));
            Assert.True(result.Success, result.Summary);
        }
    }

    private static bool IsProtectedTemporalDatabase(ProviderKind kind, string? databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            return false;

        var name = databaseName.Trim();
        return kind switch
        {
            ProviderKind.SqlServer =>
                name.Equals("master", StringComparison.OrdinalIgnoreCase)
                || name.Equals("model", StringComparison.OrdinalIgnoreCase)
                || name.Equals("msdb", StringComparison.OrdinalIgnoreCase)
                || name.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
            ProviderKind.Postgres =>
                name.Equals("postgres", StringComparison.OrdinalIgnoreCase)
                || name.Equals("template0", StringComparison.OrdinalIgnoreCase)
                || name.Equals("template1", StringComparison.OrdinalIgnoreCase),
            ProviderKind.MySql =>
                name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                || name.Equals("sys", StringComparison.OrdinalIgnoreCase)
                || name.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
                || name.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    private static StoreProvider ToStoreProvider(ProviderKind kind) => kind switch
    {
        ProviderKind.Sqlite => new StoreProvider(StoreProviderKind.Sqlite, "sqlite"),
        ProviderKind.SqlServer => new StoreProvider(StoreProviderKind.SqlServer, "sqlserver"),
        ProviderKind.Postgres => new StoreProvider(StoreProviderKind.Postgres, "postgres"),
        ProviderKind.MySql => new StoreProvider(StoreProviderKind.MySql, "mysql"),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
}
