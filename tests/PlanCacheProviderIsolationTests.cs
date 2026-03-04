using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Q2/S1/R1: Verifies that the global query plan cache uses the provider's runtime type as part
/// of the cache key so that plans built for one provider are never returned for a different provider.
/// </summary>
public class PlanCacheProviderIsolationTests
{
    [Table("Widget")]
    private class Widget
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static DbContext CreateContext(DatabaseProvider provider)
    {
        // Use an in-memory SQLite connection for both contexts; provider differences are
        // reflected in the SQL quoting style, not in the actual connection type.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, provider);
    }

    [Fact]
    public void SameExpression_DifferentProviders_ProduceDifferentSql()
    {
        using var sqliteCtx = CreateContext(new SqliteProvider());
        using var sqlServerCtx = CreateContext(new SqlServerProvider());

        var sqliteQuery  = sqliteCtx.Query<Widget>().Where(w => w.Name == "test");
        var sqlServerQuery = sqlServerCtx.Query<Widget>().Where(w => w.Name == "test");

        var sqliteProvider  = (NormQueryProvider)sqliteQuery.Provider;
        var sqlServerProvider = (NormQueryProvider)sqlServerQuery.Provider;

        var sqlitePlan    = sqliteProvider.GetPlan(sqliteQuery.Expression, out _, out _);
        var sqlServerPlan = sqlServerProvider.GetPlan(sqlServerQuery.Expression, out _, out _);

        // The SQL should differ in quoting style: SQLite uses " while SQL Server uses []
        Assert.NotEqual(sqlitePlan.Sql, sqlServerPlan.Sql);
        Assert.Contains("\"Widget\"", sqlitePlan.Sql);    // SQLite double-quotes
        Assert.Contains("[Widget]",   sqlServerPlan.Sql); // SQL Server brackets
    }

    [Fact]
    public void WarmingCacheWithOneProvider_DoesNotPollutePlanForOtherProvider()
    {
        // Warm the cache with the SQLite provider plan
        using var sqliteCtx = CreateContext(new SqliteProvider());
        var sqliteQuery = sqliteCtx.Query<Widget>().Where(w => w.Id > 0);
        var sqliteProvider = (NormQueryProvider)sqliteQuery.Provider;
        var sqlitePlan = sqliteProvider.GetPlan(sqliteQuery.Expression, out _, out _);

        // Now use the SQL Server provider with the same expression shape
        using var sqlServerCtx = CreateContext(new SqlServerProvider());
        var sqlServerQuery = sqlServerCtx.Query<Widget>().Where(w => w.Id > 0);
        var sqlServerProvider = (NormQueryProvider)sqlServerQuery.Provider;
        var sqlServerPlan = sqlServerProvider.GetPlan(sqlServerQuery.Expression, out _, out _);

        // The SQL Server plan must NOT be the cached SQLite plan
        Assert.NotEqual(sqlitePlan.Sql, sqlServerPlan.Sql);
        // Specifically: SQL Server should bracket-quote, not double-quote
        Assert.DoesNotContain("\"Widget\"", sqlServerPlan.Sql);
        Assert.Contains("[Widget]", sqlServerPlan.Sql);
    }
}
