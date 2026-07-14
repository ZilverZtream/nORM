using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// An entity-shaped `.Distinct()` outer is a no-op for Join/GroupJoin — table
/// rows are key-unique and a dedup-safe chain (no projection or fan-out) cannot
/// duplicate them — so the translator unwraps it instead of failing closed.
/// Computed-scalar Distinct outers keep the guidance throw on Join.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntityDistinctJoinOuterTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("EdjLeft_Test")]
    public class Left
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Code { get; set; }
        public bool Active { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("EdjRight_Test")]
    public class Right
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Code { get; set; }
        public string Label { get; set; } = "";
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Left[] Lefts, Right[] Rights) CreateDb()
    {
        var cs = $"Data Source=file:edj_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EdjLeft_Test (Id INTEGER PRIMARY KEY, Code INTEGER NOT NULL, Active INTEGER NOT NULL);" +
                "CREATE TABLE EdjRight_Test (Id INTEGER PRIMARY KEY, Code INTEGER NOT NULL, Label TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var lefts = new[]
        {
            new Left { Id = 1, Code = 10, Active = true },
            new Left { Id = 2, Code = 20, Active = false },
            new Left { Id = 3, Code = 10, Active = true },
            new Left { Id = 4, Code = 30, Active = true },
        };
        var rights = new[]
        {
            new Right { Id = 1, Code = 10, Label = "a" },
            new Right { Id = 2, Code = 10, Label = "b" },
            new Right { Id = 3, Code = 20, Label = "c" },
            new Right { Id = 4, Code = 40, Label = "d" },
        };
        return (keeper, ctx, lefts, rights);
    }

    private static async Task SeedAsync(DbContext ctx, Left[] lefts, Right[] rights)
    {
        foreach (var l in lefts) ctx.Add(l);
        foreach (var r in rights) ctx.Add(r);
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Entity_distinct_outer_join_matches_linq()
    {
        var (keeper, ctx, lefts, rights) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, lefts, rights);

        var expected = lefts.Where(l => l.Active).Distinct()
            .Join(rights, l => l.Code, r => r.Code, (l, r) => new { l.Id, r.Label })
            .Select(x => (x.Id, x.Label)).OrderBy(t => t).ToList();
        var actual = (await ctx.Query<Left>().Where(l => l.Active).Distinct()
            .Join(ctx.Query<Right>(), l => l.Code, r => r.Code, (l, r) => new { l.Id, r.Label })
            .ToListAsync()).Select(x => (x.Id, x.Label)).OrderBy(t => t).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Entity_distinct_outer_group_join_matches_linq()
    {
        var (keeper, ctx, lefts, rights) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, lefts, rights);

        var expected = lefts.Distinct()
            .GroupJoin(rights, l => l.Code, r => r.Code, (l, rs) => new { l.Id, Count = rs.Count() })
            .Select(x => (x.Id, x.Count)).OrderBy(t => t).ToList();
        var actual = (await ctx.Query<Left>().Distinct()
            .GroupJoin(ctx.Query<Right>(), l => l.Code, r => r.Code, (l, rs) => new { l.Id, Count = rs.Count() })
            .ToListAsync()).Select(x => (x.Id, x.Count)).OrderBy(t => t).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Entity_distinct_over_a_window_joins_only_the_window()
    {
        var (keeper, ctx, lefts, rights) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, lefts, rights);

        var expected = lefts.OrderBy(l => l.Id).Take(2).Distinct()
            .Join(rights, l => l.Code, r => r.Code, (l, r) => new { l.Id, r.Label })
            .Select(x => (x.Id, x.Label)).OrderBy(t => t).ToList();
        var actual = (await ctx.Query<Left>().OrderBy(l => l.Id).Take(2).Distinct()
            .Join(ctx.Query<Right>(), l => l.Code, r => r.Code, (l, r) => new { l.Id, r.Label })
            .ToListAsync()).Select(x => (x.Id, x.Label)).OrderBy(t => t).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Computed_scalar_distinct_outer_join_keeps_the_guidance_throw()
    {
        var (keeper, ctx, lefts, rights) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, lefts, rights);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Left>().Select(l => l.Code % 7).Distinct()
                .Join(ctx.Query<Right>(), c => c, r => r.Code, (c, r) => new { c, r.Label })
                .ToListAsync());
        Assert.Contains("Distinct", ex.Message, StringComparison.Ordinal);
    }
}
