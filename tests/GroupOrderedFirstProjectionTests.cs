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
/// Greatest-per-group projection members — <c>g.OrderBy(...).First().X</c> and
/// friends — lower to a correlated single-row subquery. The subquery must see
/// exactly the group's rows: the query's own Where filters re-apply inside it,
/// ThenBy chains and group-local Where compose, Last flips every key direction,
/// and ElementAt offsets into the ordered group.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupOrderedFirstProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GrpFirst_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Grp { get; set; }
        public int Val { get; set; }
        public int Amount { get; set; }
        public bool Flag { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Row[] Rows) CreateDb()
    {
        var cs = $"Data Source=file:grpfirst_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GrpFirst_Test (Id INTEGER PRIMARY KEY, Grp INTEGER NOT NULL, " +
                "Val INTEGER NOT NULL, Amount INTEGER NOT NULL, Flag INTEGER NOT NULL, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        // Three groups; Val deliberately collides across groups, ties broken by Amount.
        var rows = new[]
        {
            new Row { Id = 1,  Grp = 1, Val = 50, Amount = 10, Flag = true,  Name = "alpha" },
            new Row { Id = 2,  Grp = 1, Val = 80, Amount = 20, Flag = false, Name = "bravo" },
            new Row { Id = 3,  Grp = 1, Val = 80, Amount = 5,  Flag = true,  Name = "charlie" },
            new Row { Id = 4,  Grp = 2, Val = 30, Amount = 40, Flag = true,  Name = "delta" },
            new Row { Id = 5,  Grp = 2, Val = 90, Amount = 15, Flag = false, Name = "echo" },
            new Row { Id = 6,  Grp = 2, Val = 30, Amount = 25, Flag = true,  Name = "fox" },
            new Row { Id = 7,  Grp = 3, Val = 70, Amount = 30, Flag = true,  Name = "golf" },
            new Row { Id = 8,  Grp = 3, Val = 20, Amount = 45, Flag = true,  Name = "hotel" },
            new Row { Id = 9,  Grp = 3, Val = 70, Amount = 35, Flag = false, Name = "india" },
        };
        return (keeper, ctx, rows);
    }

    private static async Task SeedAsync(DbContext ctx, Row[] rows)
    {
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Source_Where_filters_the_correlated_first_row()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // The highest-Val row in groups 1 and 2 has Flag=false; a subquery that
        // ignores the source Where would pick it and return the WRONG Amount.
        var expected = rows.Where(x => x.Flag).GroupBy(x => x.Grp)
            .Select(g => new { Key = g.Key, Top = g.OrderByDescending(x => x.Val).First().Amount })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Top)).ToList();
        var actual = (await ctx.Query<Row>().Where(x => x.Flag).GroupBy(x => x.Grp)
            .Select(g => new { Key = g.Key, Top = g.OrderByDescending(x => x.Val).First().Amount })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Top)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task ThenBy_chain_breaks_ties_in_the_correlated_first_row()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Pick = g.OrderByDescending(x => x.Val).ThenBy(x => x.Amount).First().Id })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Pick)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Pick = g.OrderByDescending(x => x.Val).ThenBy(x => x.Amount).First().Id })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Pick)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Group_local_Where_restricts_the_correlated_first_row()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Best = g.Where(x => x.Flag).OrderByDescending(x => x.Val).First().Name })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Best)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Best = g.Where(x => x.Flag).OrderByDescending(x => x.Val).First().Name })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Best)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Terminal_predicate_filters_like_a_Where()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Pick = g.OrderBy(x => x.Amount).First(x => x.Val >= 50).Id })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Pick)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Pick = g.OrderBy(x => x.Amount).First(x => x.Val >= 50).Id })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Pick)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Last_flips_every_key_in_the_ThenBy_chain()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Pick = g.OrderBy(x => x.Val).ThenByDescending(x => x.Amount).Last().Id })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Pick)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Pick = g.OrderBy(x => x.Val).ThenByDescending(x => x.Amount).Last().Id })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Pick)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task ElementAt_offsets_into_the_ordered_group()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Second = g.OrderBy(x => x.Amount).ThenBy(x => x.Id).ElementAt(1).Id })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Second)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Second = g.OrderBy(x => x.Amount).ThenBy(x => x.Id).ElementAt(1).Id })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Second)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Select_projection_with_trailing_member_composes()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Len = g.OrderByDescending(x => x.Val).ThenBy(x => x.Id).Select(x => x.Name).First().Length })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Len)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Len = g.OrderByDescending(x => x.Val).ThenBy(x => x.Id).Select(x => x.Name).First().Length })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Len)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Composite_key_groups_correlate_on_every_key_member()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => new { x.Grp, x.Flag })
            .Select(g => new { g.Key.Grp, g.Key.Flag, Top = g.OrderByDescending(x => x.Val).ThenBy(x => x.Id).First().Id })
            .OrderBy(r => r.Grp).ThenBy(r => r.Flag).Select(r => (r.Grp, r.Flag, r.Top)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => new { x.Grp, x.Flag })
            .Select(g => new { g.Key.Grp, g.Key.Flag, Top = g.OrderByDescending(x => x.Val).ThenBy(x => x.Id).First().Id })
            .ToListAsync())
            .OrderBy(r => r.Grp).ThenBy(r => r.Flag).Select(r => (r.Grp, r.Flag, r.Top)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    [Fact]
    public async Task Computed_closure_group_key_correlates_consistently()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // The computed group key translates TWICE — once for GROUP BY (compiled
        // parameter) and once for the subquery correlation (folded literal); both
        // must see the same closure value, including across cached-plan reruns.
        foreach (var mod in new[] { 2, 3 })
        {
            var expected = rows.GroupBy(x => x.Grp % mod)
                .Select(g => (g.Key, Top: g.OrderByDescending(x => x.Val).ThenBy(x => x.Id).First().Id))
                .OrderBy(t => t.Key).ToList();
            var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp % mod)
                .Select(g => new { g.Key, Top = g.OrderByDescending(x => x.Val).ThenBy(x => x.Id).First().Id })
                .ToListAsync()).Select(x => (x.Key, x.Top)).OrderBy(t => t.Key).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"mod={mod}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task Existing_single_order_shape_still_translates()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var expected = rows.GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Latest = g.OrderByDescending(x => x.Id).First().Name })
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Latest)).ToList();
        var actual = (await ctx.Query<Row>().GroupBy(x => x.Grp)
            .Select(g => new { g.Key, Latest = g.OrderByDescending(x => x.Id).First().Name })
            .ToListAsync())
            .OrderBy(r => r.Key).Select(r => (r.Key, r.Latest)).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }
}
