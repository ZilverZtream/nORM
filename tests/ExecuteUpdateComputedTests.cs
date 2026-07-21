using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for set-based ExecuteUpdate/ExecuteDelete with COMPUTED SetProperty
/// expressions and filtered scopes — a competitive feature (EF Core ExecuteUpdate). The SetProperty
/// value expression and the Where scope go through the write-path expression translation; the result
/// is compared against applying the same transform in memory.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ExecuteUpdateComputedTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("EucRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static (DbContext, SqliteConnection) NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EucRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            for (int i = 1; i <= 20; i++) cmd.CommandText += $"INSERT INTO EucRow VALUES ({i},{i * 2},{i % 5});";
            cmd.ExecuteNonQuery();
        }
        return (new DbContext(cn, new SqliteProvider()), cn);
    }

    private static List<(int Id, int A, int B)> Seed()
        => Enumerable.Range(1, 20).Select(i => (Id: i, A: i * 2, B: i % 5)).ToList();

    private static List<(int, int, int)> Read(DbContext ctx)
        => ctx.Query<Row>().OrderBy(r => r.Id).ToList().Select(r => (r.Id, r.A, r.B)).ToList();

    [Fact]
    public async Task ExecuteUpdate_computed_expression_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        // A = A * 2 + B, filtered to A > 10.
        await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>().Where(r => r.A > 10),
            s => s.SetProperty(p => p.A, p => p.A * 2 + p.B));

        var expected = Seed().Select(r => r.A > 10 ? (r.Id, A: r.A * 2 + r.B, r.B) : r).OrderBy(r => r.Id).ToList();
        Assert.Equal(expected, Read(ctx));
    }

    [Fact]
    public async Task ExecuteUpdate_multiple_setproperty_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>().Where(r => r.B == 0),
            s => s.SetProperty(p => p.A, p => p.A + 1000).SetProperty(p => p.B, p => p.B + 7));

        var expected = Seed().Select(r => r.B == 0 ? (r.Id, A: r.A + 1000, B: r.B + 7) : r).OrderBy(r => r.Id).ToList();
        Assert.Equal(expected, Read(ctx));
    }

    [Fact]
    public async Task ExecuteUpdate_constant_value_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>().Where(r => r.Id <= 5),
            s => s.SetProperty(p => p.A, 999));

        var expected = Seed().Select(r => r.Id <= 5 ? (r.Id, A: 999, r.B) : r).OrderBy(r => r.Id).ToList();
        Assert.Equal(expected, Read(ctx));
    }

    [Fact]
    public async Task ExecuteDelete_filtered_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        var deleted = await NormAsyncExtensions.ExecuteDeleteAsync(ctx.Query<Row>().Where(r => r.A % 3 == 0));

        var expected = Seed().Where(r => r.A % 3 != 0).OrderBy(r => r.Id).ToList();
        var expectedDeleted = Seed().Count(r => r.A % 3 == 0);
        Assert.Equal(expectedDeleted, deleted);
        Assert.Equal(expected, Read(ctx));
    }
}
