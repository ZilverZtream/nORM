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
/// Pins the fast-path x global-filter interaction: the query shapes the fast-path executor
/// serves (property-equality Where, plain Take, count) must return FILTERED results when a
/// global filter is configured — the executor bails to the full pipeline rather than emitting
/// its filter-less SQL. A fast path that ignored the bail would silently resurrect
/// soft-deleted rows on exactly the most common query shapes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FastPathGlobalFilterInteractionContractTests
{
    [Table("FpgfRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static DbContext NewCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FpgfRow_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
                "INSERT INTO FpgfRow_Test VALUES (1,5,0),(2,5,1),(3,7,0),(4,7,1),(5,7,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.AddGlobalFilter<Row>(r => !r.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Property_equality_where_respects_the_filter()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var rows = ctx.Query<Row>().Where(r => r.Val == 5).ToList();
        Assert.Equal(new[] { 1 }, rows.Select(r => r.Id).ToArray());

        var sevens = ctx.Query<Row>().Where(r => r.Val == 7).ToList();
        Assert.Equal(new[] { 3 }, sevens.Select(r => r.Id).ToArray());
    }

    [Fact]
    public void Take_and_count_respect_the_filter()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var taken = ctx.Query<Row>().OrderBy(r => r.Id).Take(10).ToList();
        Assert.Equal(new[] { 1, 3 }, taken.Select(r => r.Id).ToArray());

        Assert.Equal(2, ctx.Query<Row>().Count());
        Assert.Equal(1, ctx.Query<Row>().Count(r => r.Val == 7));
    }
}
