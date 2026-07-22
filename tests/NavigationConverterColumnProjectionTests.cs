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
/// Projecting a First/Last/Min/Max over a NAVIGATION collection whose selected column has a value converter
/// applies ConvertFromProvider to the scalar result — the same contract as the ctx.Query correlated path.
/// The SQL selects/aggregates the STORED column and orders on the stored representation (EF-consistent), and
/// the materializer maps the single result value back. A NegatingConverter (int stored as -int) makes an
/// unconverted result visible as the wrong sign, and makes Min/Max pick the order-reversed element so the
/// stored-then-convert semantics are observable. Sum/Average stay fail-loud: SUM/AVG combine stored values
/// and ConvertFromProvider (a per-value map) does not distribute over that combination.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationConverterColumnProjectionTests
{
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -(Convert.ToInt32(v));
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NccpParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NccpChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Score { get; set; } // stored negated via converter
        public Parent Parent { get; set; } = default!;
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NccpParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NccpChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Score INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Child>().Property(c => c.Score).HasConversion(new NegatingConverter());
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await ctx.InsertAsync(new Parent { Id = 1 });
        await ctx.InsertAsync(new Child { Id = 1, ParentId = 1, Score = 3 });
        await ctx.InsertAsync(new Child { Id = 2, ParentId = 1, Score = 7 });
        return ctx;
    }

    [Fact]
    public async Task Converter_column_round_trips_on_plain_read()
    {
        using var ctx = await CtxAsync();
        var scores = ctx.Query<Child>().OrderBy(c => c.Id).ToList().Select(c => c.Score).ToList();
        Assert.Equal(new[] { 3, 7 }, scores); // ConvertFromProvider applied — not the stored -3,-7
    }

    [Fact]
    public async Task Count_over_converter_child_still_works()
    {
        using var ctx = await CtxAsync();
        var counts = ctx.Query<Parent>().OrderBy(p => p.Id).Select(p => p.Children.Count()).ToList();
        Assert.Equal(new[] { 2 }, counts);
    }

    [Fact]
    public async Task First_selecting_converter_column_applies_ConvertFromProvider()
    {
        using var ctx = await CtxAsync();
        // First child by Id -> child1, Score model value 3 (stored -3).
        var got = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => p.Children.OrderBy(c => c.Id).Select(c => c.Score).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 3 }, got);
    }

    [Fact]
    public async Task First_ordered_by_converter_column_orders_on_stored_form()
    {
        using var ctx = await CtxAsync();
        // OrderByDescending(Score) runs on the STORED value: stored {-3,-7} desc -> -3 (child1) -> Id 1.
        // (EF-consistent: ordering/aggregation on the provider representation.)
        var got = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => p.Children.OrderByDescending(c => c.Score).Select(c => c.Id).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 1 }, got);
    }

    [Fact]
    public async Task Max_over_converter_column_converts_result()
    {
        using var ctx = await CtxAsync();
        // MAX(stored {-3,-7}) = -3 -> ConvertFromProvider -> 3 (equals MIN of the model values under negation).
        var got = ctx.Query<Parent>().OrderBy(p => p.Id).Select(p => p.Children.Max(c => c.Score)).ToList();
        Assert.Equal(new[] { 3 }, got);
    }

    [Fact]
    public async Task Min_over_converter_column_converts_result()
    {
        using var ctx = await CtxAsync();
        // MIN(stored {-3,-7}) = -7 -> ConvertFromProvider -> 7.
        var got = ctx.Query<Parent>().OrderBy(p => p.Id).Select(p => p.Children.Min(c => c.Score)).ToList();
        Assert.Equal(new[] { 7 }, got);
    }

    [Fact]
    public async Task Sum_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>().Select(p => p.Children.Sum(c => c.Score)).ToList());
        Assert.Contains("converter", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Average_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>().Select(p => p.Children.Average(c => c.Score)).ToList());
    }

    [Fact]
    public async Task Wrapped_projection_First_over_converter_column_converts()
    {
        using var ctx = await CtxAsync();
        // The anonymous-type (member-named) registration path, not just the bare reserved-key path.
        var got = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Top = p.Children.OrderBy(c => c.Id).Select(c => c.Score).FirstOrDefault() })
            .ToList();
        Assert.Equal(3, got.Single().Top);
    }
}
