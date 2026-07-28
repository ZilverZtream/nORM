using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A window function (WithRowNumber/WithRank/…) projects into an unmapped result type, so the global-filter
/// injection did not descend into it: the ranked source table was queried with NO soft-delete / tenant
/// predicate. That both leaked filtered rows into the result AND computed every rank over rows the caller
/// should never see. The filter must land on the window's source, before the window is applied.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class WindowFunctionGlobalFilterTests
{
    [Table("WfgOrder_Test")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int Amt { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE WfgOrder_Test (Id INTEGER PRIMARY KEY, Amt INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
                // Amt order desc: 999(deleted), 100, 50. The deleted row would otherwise rank #1.
                "INSERT INTO WfgOrder_Test VALUES (1, 100, 0), (2, 999, 1), (3, 50, 0);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Order>(o => !o.IsDeleted);
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void Window_function_ranks_over_the_globally_filtered_source()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var rows = ctx.Query<Order>()
            .OrderByDescending(o => o.Amt)
            .WithRowNumber((o, rn) => new { o.Id, o.Amt, Rn = rn })
            .ToList()
            .Select(r => (r.Id, r.Rn))
            .ToList();

        // The soft-deleted row (Id=2) must be absent, and the ranks must be computed over only the visible
        // rows: Id=1 (Amt 100) → rank 1, Id=3 (Amt 50) → rank 2.
        Assert.Equal(new[] { (1, 1), (3, 2) }, rows.ToArray());
    }

    [Fact]
    public void IgnoreQueryFilters_lets_the_window_rank_over_all_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var rows = ctx.Query<Order>().IgnoreQueryFilters()
            .OrderByDescending(o => o.Amt)
            .WithRowNumber((o, rn) => new { o.Id, o.Amt, Rn = rn })
            .ToList()
            .Select(r => (r.Id, r.Rn))
            .ToList();

        // With filters ignored, all three rank together: Id=2 (999) → 1, Id=1 (100) → 2, Id=3 (50) → 3.
        Assert.Equal(new[] { (2, 1), (1, 2), (3, 3) }, rows.ToArray());
    }
}
