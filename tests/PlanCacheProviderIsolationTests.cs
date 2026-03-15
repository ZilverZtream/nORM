using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that the global query plan cache uses the provider's runtime type as part
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

 // Two contexts with the same provider and entity type but different ToTable mappings
 // must produce separate cache entries.

    [Table("OriginalTable")]
    private class RemappableWidget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void TwoContexts_SameProvider_DifferentToTable_ProduceSeparateCacheEntries()
    {
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();

 // First context uses default table name from [Table] attribute
        using var ctx1 = new DbContext(cn1, new SqliteProvider());

 // Second context remaps the same entity to a different physical table
        var options2 = new DbContextOptions();
        options2.OnModelCreating = mb => mb.Entity<RemappableWidget>().ToTable("AlternativeTable");
        using var ctx2 = new DbContext(cn2, new SqliteProvider(), options2);

 // Force mapping registration in both contexts
        var query1 = ctx1.Query<RemappableWidget>().Where(w => w.Id > 0);
        var query2 = ctx2.Query<RemappableWidget>().Where(w => w.Id > 0);

        var provider1 = (NormQueryProvider)query1.Provider;
        var provider2 = (NormQueryProvider)query2.Provider;

        var plan1 = provider1.GetPlan(query1.Expression, out _, out _);
        var plan2 = provider2.GetPlan(query2.Expression, out _, out _);

 // The plans should have distinct SQL since they target different tables
        Assert.NotEqual(plan1.Sql, plan2.Sql);
        Assert.Contains("\"OriginalTable\"", plan1.Sql);
        Assert.Contains("\"AlternativeTable\"", plan2.Sql);

        cn1.Dispose();
        cn2.Dispose();
    }
}
