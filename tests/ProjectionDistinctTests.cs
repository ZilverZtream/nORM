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
/// Distinct after a scalar projection must deduplicate the PROJECTED values. When the projection sits over a
/// set operation, the Select wraps the compound as `SELECT ... FROM (compound) AS alias`, which pre-fills the
/// SQL buffer - so the trailing Distinct's flag (rendered only for an empty buffer) never reached the SQL and
/// duplicates survived. Found by the coverage-guided query-IR differential fuzzer.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionDistinctTests
{
    [Table("PdRow")]
    public class Row { [Key] public int Id { get; set; } public int A { get; set; } public int B { get; set; } }

    private static DbContext Ctx(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PdRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
            "INSERT INTO PdRow VALUES (1,4,1),(2,4,1),(3,2,2),(4,3,2),(5,4,3);";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
    }

    [Fact]
    public void Distinct_after_scalar_projection_deduplicates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var vals = ctx.Query<Row>().Select(r => r.B).Distinct().ToList().OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 2, 3 }, vals);
    }

    [Fact]
    public void Distinct_after_computed_projection_deduplicates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var vals = ctx.Query<Row>().Select(r => r.B + 60).Distinct().ToList().OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 61, 62, 63 }, vals);
    }

    [Fact]
    public void Distinct_after_setop_projection_deduplicates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var vals = ctx.Query<Row>().Concat(ctx.Query<Row>()).Select(r => r.B + 60).Distinct().ToList().OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 61, 62, 63 }, vals);
    }

    [Fact]
    public void Distinct_after_union_projection_deduplicates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var vals = ctx.Query<Row>().Where(r => r.A >= 3).Union(ctx.Query<Row>().Where(r => r.B <= 2))
            .Select(r => r.A).Distinct().ToList().OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 2, 3, 4 }, vals);
    }
}
