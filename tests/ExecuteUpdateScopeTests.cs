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
/// Oracle-compared coverage for set-based ExecuteUpdate with less-common scopes and value shapes: a
/// Contains-list filter, a conditional (ternary) SetProperty value, a SetProperty that swaps/references
/// another column, and a no-match scope (zero rows affected). Verified against applying the same
/// transform in memory.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ExecuteUpdateScopeTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("EusRow")]
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
            cmd.CommandText = "CREATE TABLE EusRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            for (int i = 1; i <= 20; i++) cmd.CommandText += $"INSERT INTO EusRow VALUES ({i},{i * 2},{i % 6});";
            cmd.ExecuteNonQuery();
        }
        return (new DbContext(cn, new SqliteProvider()), cn);
    }

    private static List<(int Id, int A, int B)> Seed()
        => Enumerable.Range(1, 20).Select(i => (Id: i, A: i * 2, B: i % 6)).ToList();

    private static List<(int, int, int)> Read(DbContext ctx)
        => ctx.Query<Row>().OrderBy(r => r.Id).ToList().Select(r => (r.Id, r.A, r.B)).ToList();

    [Fact]
    public async Task ExecuteUpdate_contains_scope_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        var ids = new[] { 2, 5, 8, 11 };
        await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>().Where(r => ids.Contains(r.Id)),
            s => s.SetProperty(p => p.A, p => p.A + 500));

        var idSet = ids.ToHashSet();
        var expected = Seed().Select(r => idSet.Contains(r.Id) ? (r.Id, A: r.A + 500, r.B) : r).OrderBy(r => r.Id).ToList();
        Assert.Equal(expected, Read(ctx));
    }

    [Fact]
    public async Task ExecuteUpdate_conditional_value_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        // B = A > 20 ? 1 : 0 for all rows.
        await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>(),
            s => s.SetProperty(p => p.B, p => p.A > 20 ? 1 : 0));

        var expected = Seed().Select(r => (r.Id, r.A, B: r.A > 20 ? 1 : 0)).OrderBy(r => r.Id).ToList();
        Assert.Equal(expected, Read(ctx));
    }

    [Fact]
    public async Task ExecuteUpdate_cross_column_reference_matches_manual()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        // A = B * 100 (A becomes a function of another column).
        await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>(),
            s => s.SetProperty(p => p.A, p => p.B * 100));

        var expected = Seed().Select(r => (r.Id, A: r.B * 100, r.B)).OrderBy(r => r.Id).ToList();
        Assert.Equal(expected, Read(ctx));
    }

    [Fact]
    public async Task ExecuteUpdate_no_match_affects_zero_and_leaves_data()
    {
        var (ctx, cn) = NewCtx(); using var _ = cn; using var __ = ctx;
        var affected = await NormAsyncExtensions.ExecuteUpdateAsync(
            ctx.Query<Row>().Where(r => r.Id > 1000),
            s => s.SetProperty(p => p.A, 0));

        Assert.Equal(0, affected);
        Assert.Equal(Seed().OrderBy(r => r.Id).ToList(), Read(ctx));
    }
}
