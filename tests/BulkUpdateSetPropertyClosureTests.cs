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
/// ExecuteUpdate assignment values captured from locals must re-bind on every
/// execution: plans are cached by expression fingerprint, so a baked SetProperty
/// value would silently write the FIRST execution's value in every later bulk
/// update sharing the shape. Same contract for computed assignments with
/// captured deltas and for the WHERE closure alongside them.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkUpdateSetPropertyClosureTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BupRow_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int V { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:bupclo_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BupRow_Test (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Tag TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()));
    }

    private static async Task SeedAsync(DbContext ctx)
    {
        for (var i = 1; i <= 5; i++)
            ctx.Add(new Row { Id = i, V = i * 10, Tag = "t" + i });
        await ctx.SaveChangesAsync();
    }

    private static int[] ReadRaw(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT V FROM BupRow_Test ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var list = new System.Collections.Generic.List<int>();
        while (reader.Read()) list.Add(reader.GetInt32(0));
        return list.ToArray();
    }

    [Fact]
    public async Task Captured_set_value_rebinds_across_cached_plans()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        foreach (var (cut, newValue) in new[] { (25, 111), (35, 222) })
        {
            await ctx.Query<Row>().Where(r => r.V >= cut && r.V < 1000)
                .ExecuteUpdateAsync(s => s.SetProperty(r => r.V, newValue));
            // Restore so the second round targets fresh values.
            var raw = ReadRaw(keeper);
            var expectedHits = new[] { 10, 20, 30, 40, 50 }.Count(v => v >= cut);
            Assert.True(raw.Count(v => v == newValue) == expectedHits,
                $"cut={cut},newValue={newValue}: raw [{string.Join(",", raw)}] — expected {expectedHits} rows set to {newValue}");
            using var reset = keeper.CreateCommand();
            reset.CommandText = "UPDATE BupRow_Test SET V = Id * 10";
            reset.ExecuteNonQuery();
        }
    }

    [Fact]
    public async Task Captured_computed_delta_rebinds_across_cached_plans()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        var current = new[] { 10, 20, 30, 40, 50 };
        foreach (var (cut, delta) in new[] { (15, 7), (25, 1000) })
        {
            await ctx.Query<Row>().Where(r => r.V >= cut)
                .ExecuteUpdateAsync(s => s.SetProperty(r => r.V, r => r.V + delta));
            current = current.Select(v => v >= cut ? v + delta : v).ToArray();
            var raw = ReadRaw(keeper);
            Assert.True(current.SequenceEqual(raw),
                $"cut={cut},delta={delta}: expected [{string.Join(",", current)}] got [{string.Join(",", raw)}]");
        }
    }

    [Fact]
    public async Task Correlated_subquery_set_value_translates_or_fails_closed()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        // A correlated aggregate as the assignment value: acceptable outcomes
        // are a correct server-side translation or a deterministic throw —
        // silently writing wrong values is not.
        try
        {
            await ctx.Query<Row>()
                .ExecuteUpdateAsync(s => s.SetProperty(r => r.V, r => ctx.Query<Row>().Count(x => x.Id > r.Id)));
        }
        catch (NormUnsupportedFeatureException)
        {
            return; // deterministic fail-closed — acceptable contract
        }

        var raw = ReadRaw(keeper);
        Assert.True(raw.SequenceEqual(new[] { 4, 3, 2, 1, 0 }),
            $"correlated SET translated but wrote WRONG values: [{string.Join(",", raw)}] (expected [4,3,2,1,0])");
    }

    [Fact]
    public async Task Where_and_set_closures_bind_independently()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx);

        // Two closures whose values are deliberately swappable — a cross-bind
        // changes both the row set and the written value.
        var cut = 40;
        var newValue = 3;
        await ctx.Query<Row>().Where(r => r.V >= cut)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.V, newValue));
        var raw = ReadRaw(keeper);
        Assert.True(raw.SequenceEqual(new[] { 10, 20, 30, 3, 3 }),
            $"expected [10,20,30,3,3] got [{string.Join(",", raw)}]");
    }
}
