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
            var result = await StoreScenario.RunAsync(connection, provider, ToStoreProvider(kind));
            Assert.True(result.Success, result.Summary);
        }
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
