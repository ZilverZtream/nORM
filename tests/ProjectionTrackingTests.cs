using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// M-1: Verifies that only query-root entity types are tracked by the ChangeTracker.
/// Types that are NOT registered via Query&lt;T&gt; must not be tracked, even if they
/// satisfy the structural trackability conditions (class, parameterless ctor, etc.).
/// </summary>
public class ProjectionTrackingTests
{
    // ── Mapped entity ─────────────────────────────────────────────────────────

    [Table("PtWidget")]
    private class PtWidget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── Another mapped entity (different table) ───────────────────────────────

    [Table("PtOrder")]
    private class PtOrder
    {
        [Key] public int Id { get; set; }
        public decimal Total { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE PtWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE PtOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Total REAL NOT NULL);";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
        return (cn, ctx);
    }

    [Fact]
    public async Task QueryRootEntity_IsTracked()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        // Seed
        ctx.Add(new PtWidget { Name = "Widget1" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        // Query — entity should be tracked after this
        var widgets = await ctx.Query<PtWidget>().ToListAsync();
        Assert.Single(widgets);
        Assert.Contains(ctx.ChangeTracker.Entries, e => e.Entity is PtWidget);
    }

    [Fact]
    public async Task UnqueriedEntity_NotTracked_WhenPopulatedFromOtherPath()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        // Seed
        ctx.Add(new PtWidget { Name = "Widget2" });
        await ctx.SaveChangesAsync();

        // PtOrder was never used as a query root — IsMapped should reflect that
        // We test by checking that IsMapped(PtOrder) returns false (indirectly via the logic)
        // and IsMapped(PtWidget) returns true after Query<PtWidget>

        // Register PtWidget as query root
        var __ = ctx.Query<PtWidget>();  // registers PtWidget

        // PtOrder is NOT a query root
        var isMappedWidget = InvokeIsMapped(ctx, typeof(PtWidget));
        var isMappedOrder = InvokeIsMapped(ctx, typeof(PtOrder));

        Assert.True(isMappedWidget, "PtWidget should be mapped after Query<PtWidget>()");
        Assert.False(isMappedOrder, "PtOrder should NOT be mapped — never used as query root");
    }

    [Fact]
    public async Task TwoDistinctEntityTypes_EachTrackedSeparately()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        ctx.Add(new PtWidget { Name = "W" });
        ctx.Add(new PtOrder { Total = 99.9m });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        await ctx.Query<PtWidget>().ToListAsync();
        await ctx.Query<PtOrder>().ToListAsync();

        var widgetEntries = ctx.ChangeTracker.Entries.Where(e => e.Entity is PtWidget).ToList();
        var orderEntries = ctx.ChangeTracker.Entries.Where(e => e.Entity is PtOrder).ToList();

        Assert.Single(widgetEntries);
        Assert.Single(orderEntries);
    }

    [Fact]
    public async Task EntityWithNoTracking_NotAddedToChangeTracker()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PtWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions
        {
            EagerChangeTracking = true,
            DefaultTrackingBehavior = QueryTrackingBehavior.NoTracking
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await using var _ = ctx;

        // Seed one row directly via SQL
        using var insert = cn.CreateCommand();
        insert.CommandText = "INSERT INTO PtWidget (Name) VALUES ('Untracked')";
        insert.ExecuteNonQuery();

        await ctx.Query<PtWidget>().ToListAsync();

        Assert.Empty(ctx.ChangeTracker.Entries);
    }

    // Helper to call the internal IsMapped method via reflection
    private static bool InvokeIsMapped(DbContext ctx, Type t)
    {
        var method = typeof(DbContext).GetMethod("IsMapped",
            BindingFlags.Instance | BindingFlags.NonPublic);
        return (bool)method!.Invoke(ctx, new object[] { t })!;
    }
}
