using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Probe: plain <see cref="DateTime"/> stored as TEXT on SQLite ("yyyy-MM-dd HH:mm:ss[.fffffff]"). Rows 1
/// and 2 are the same instant written with a different fractional scale ("12:00:00" vs "12:00:00.0000000").
/// Equality must match both; GROUP BY / DISTINCT must collapse them to one. Analogous to the TimeOnly
/// fix — checking whether plain DateTime has the same non-canonical-fraction gap (DateTime is NOT in the
/// ExactKeySql dispatcher).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeFractionalScaleTests
{
    [Table("DtFrac_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public DateTime Dt { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE DtFrac_Test (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL);" +
                "INSERT INTO DtFrac_Test VALUES (1,'2026-05-25 12:00:00'),(2,'2026-05-25 12:00:00.0000000'),(3,'2026-05-25 13:00:00');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task DateTime_equality_matches_by_instant()
    {
        using var ctx = NewCtx();
        var noon = new DateTime(2026, 5, 25, 12, 0, 0);
        var ids = await ctx.Query<Row>().Where(r => r.Dt == noon).Select(r => new { r.Id }).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task DateTime_groupby_keys_by_instant()
    {
        using var ctx = NewCtx();
        var groups = await ctx.Query<Row>().GroupBy(r => r.Dt).Select(g => new { N = g.Count() }).ToListAsync();
        Assert.Equal(2, groups.Count); // 12:00 (2 rows), 13:00 (1)
    }

    [Fact]
    public async Task DateTime_distinct_collapses_equal_instants()
    {
        using var ctx = NewCtx();
        var distinct = await ctx.Query<Row>().Select(r => new { r.Dt }).Distinct().ToListAsync();
        Assert.Equal(2, distinct.Count);
    }

    [Fact]
    public async Task DateTime_equality_simple_where_fast_path_matches_by_instant()
    {
        using var ctx = NewCtx();
        var noon = new DateTime(2026, 5, 25, 12, 0, 0);
        var rows = await ctx.Query<Row>().Where(r => r.Dt == noon).ToListAsync(); // simple-Where fast path
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task DateTime_equality_filtered_ordered_fast_path_matches_by_instant()
    {
        using var ctx = NewCtx();
        var noon = new DateTime(2026, 5, 25, 12, 0, 0);
        var rows = await ctx.Query<Row>().Where(r => r.Dt == noon).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task DateTime_equality_count_matches_by_instant()
    {
        using var ctx = NewCtx();
        var noon = new DateTime(2026, 5, 25, 12, 0, 0);
        Assert.Equal(2, await ctx.Query<Row>().CountAsync(r => r.Dt == noon));
    }

    [Fact]
    public async Task DateTime_equality_projected_is_by_instant()
    {
        using var ctx = NewCtx();
        var noon = new DateTime(2026, 5, 25, 12, 0, 0);
        var rows = await ctx.Query<Row>().Select(r => new { r.Id, M = r.Dt == noon }).OrderBy(r => r.Id).ToListAsync();
        Assert.True(rows.Single(r => r.Id == 1).M);
        Assert.True(rows.Single(r => r.Id == 2).M);
        Assert.False(rows.Single(r => r.Id == 3).M);
    }
}
