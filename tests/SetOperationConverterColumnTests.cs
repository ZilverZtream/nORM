using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Set operations (Union / Except / Intersect / Concat) that project a value-converter column apply
/// ConvertFromProvider to the result. The outer set-op materializer runs with the scalar ELEMENT mapping
/// (e.g. int), not the entity, so its direct-member converter resolver couldn't see the column and returned
/// the raw stored value (a +1000 converter yielded 1010 instead of 10). The converter is now resolved from
/// the lifted projection's parameter entity type and applied. Set operations on non-converter columns are a
/// baseline; dedup/difference happen on the stored representation (equal for an order-preserving converter).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SetOperationConverterColumnTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("SocWidget")]
    public sealed class Widget
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Score { get; set; } // converter (+1000)
        public string Category { get; set; } = "";
    }

    private static readonly (int id, int score, string cat)[] Rows =
        { (1, 10, "A"), (2, 20, "B"), (3, 5, "A"), (4, 20, "C") };

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SocWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Category TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property(w => w.Score).HasConversion(new OffsetConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in Rows) await ctx.InsertAsync(new Widget { Id = r.id, Score = r.score, Category = r.cat });
        return ctx;
    }

    // q1 = Score>8 -> ids{1,2,4} scores{10,20,20}; q2 = Category=="A" -> ids{1,3} scores{10,5}
    private static IQueryable<Widget> Q1(DbContext c) => c.Query<Widget>().Where(w => w.Score > 8);
    private static IQueryable<Widget> Q2(DbContext c) => c.Query<Widget>().Where(w => w.Category == "A");

    [Fact]
    public async Task Union_of_converter_column_converts_result()
    {
        using var ctx = await CtxAsync();
        // {10,20} ∪ {10,5} = {5,10,20} model values (not stored 1010/1020/1005).
        var got = Q1(ctx).Select(w => w.Score).Union(Q2(ctx).Select(w => w.Score)).OrderBy(x => x).ToList();
        Assert.Equal(new[] { 5, 10, 20 }, got);
    }

    [Fact]
    public async Task Except_of_converter_column_converts_result()
    {
        using var ctx = await CtxAsync();
        // {10,20} \ {10,5} = {20}.
        var got = Q1(ctx).Select(w => w.Score).Except(Q2(ctx).Select(w => w.Score)).OrderBy(x => x).ToList();
        Assert.Equal(new[] { 20 }, got);
    }

    [Fact]
    public async Task Intersect_of_converter_column_converts_result()
    {
        using var ctx = await CtxAsync();
        // {10,20} ∩ {10,5} = {10}.
        var got = Q1(ctx).Select(w => w.Score).Intersect(Q2(ctx).Select(w => w.Score)).OrderBy(x => x).ToList();
        Assert.Equal(new[] { 10 }, got);
    }

    [Fact]
    public async Task Concat_of_converter_column_converts_each_row()
    {
        using var ctx = await CtxAsync();
        // {10,20,20} ++ {10,5} = 5 rows, converted.
        var got = Q1(ctx).Select(w => w.Score).Concat(Q2(ctx).Select(w => w.Score)).ToList().OrderBy(x => x).ToList();
        Assert.Equal(new[] { 5, 10, 10, 20, 20 }, got);
    }

    [Fact]
    public async Task Set_operations_on_nonconverter_column_baseline()
    {
        using var ctx = await CtxAsync();
        Assert.Equal(new[] { 1, 2, 3, 4 }, Q1(ctx).Select(w => w.Id).Union(Q2(ctx).Select(w => w.Id)).OrderBy(x => x).ToList());
        Assert.Equal(new[] { 2, 4 }, Q1(ctx).Select(w => w.Id).Except(Q2(ctx).Select(w => w.Id)).OrderBy(x => x).ToList());
        Assert.Equal(new[] { 1 }, Q1(ctx).Select(w => w.Id).Intersect(Q2(ctx).Select(w => w.Id)).OrderBy(x => x).ToList());
    }
}
