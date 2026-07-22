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
/// Distinct-values and paging (Skip/Take, OrderBy+Take) over a value-converter scalar projection apply
/// ConvertFromProvider correctly. Unlike a set operation (whose outer materializer runs with the element
/// mapping, not the entity), these single-source operators keep the entity mapping so the projection's
/// converter resolves directly. Regression guard adjacent to the ComputeProjectionSubqueryConverters /
/// ExtractColumnsFromProjection changes made for the set-op converter fixes -- a +1000 converter makes an
/// unconverted result observable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DistinctAndPagingConverterProjectionTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("DapWidget")]
    public sealed class Widget
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Score { get; set; } // converter (+1000)
    }

    private static readonly (int id, int score)[] Rows = { (1, 10), (2, 20), (3, 5), (4, 20) };

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DapWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);";
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
        foreach (var r in Rows) await ctx.InsertAsync(new Widget { Id = r.id, Score = r.score });
        return ctx;
    }

    [Fact]
    public async Task Distinct_over_converter_scalar_converts()
    {
        using var ctx = await CtxAsync();
        // scores 10,20,5,20 -> distinct {5,10,20} model values (not stored 1005/1010/1020).
        Assert.Equal(new[] { 5, 10, 20 }, ctx.Query<Widget>().Select(w => w.Score).Distinct().OrderBy(x => x).ToList());
    }

    [Fact]
    public async Task Distinct_over_anon_converter_member_converts()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Widget>().Select(w => new { w.Score }).Distinct().ToList().Select(x => x.Score).OrderBy(x => x).ToList();
        Assert.Equal(new[] { 5, 10, 20 }, got);
    }

    [Fact]
    public async Task Skip_take_over_converter_scalar_converts()
    {
        using var ctx = await CtxAsync();
        // by Id: scores 10,20,5,20 ; Skip(1).Take(2) -> {20,5}.
        Assert.Equal(new[] { 20, 5 }, ctx.Query<Widget>().OrderBy(w => w.Id).Select(w => w.Score).Skip(1).Take(2).ToList());
    }

    [Fact]
    public async Task OrderBy_take_over_converter_scalar_converts()
    {
        using var ctx = await CtxAsync();
        // ordered by Score (stored, order-preserving) ascending, Take(2) -> {5,10}.
        Assert.Equal(new[] { 5, 10 }, ctx.Query<Widget>().OrderBy(w => w.Score).Select(w => w.Score).Take(2).ToList());
    }

    [Fact]
    public async Task Distinct_then_Contains_binds_provider_value()
    {
        using var ctx = await CtxAsync();
        Assert.True(ctx.Query<Widget>().Select(w => w.Score).Distinct().Contains(20));
        Assert.False(ctx.Query<Widget>().Select(w => w.Score).Distinct().Contains(999));
    }
}
