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
/// A value-converter column cannot be aggregated or selected-as-first over a navigation collection in SQL:
/// the correlated subquery runs on the STORED representation (a negating/scaling converter yields a wrong
/// Sum, Min/Max can pick the wrong element under an order-reversing converter) and the result isn't run
/// through ConvertFromProvider. These shapes now FAIL LOUD with a clear message instead of returning a
/// silently-wrong value. A converter column NOT involved in the aggregate/first still works (Count, and a
/// converter column that round-trips on a normal materialised read).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationConverterColumnGuardTests
{
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NccgParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NccgChild")]
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
                "CREATE TABLE NccgParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NccgChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Score INTEGER NOT NULL);";
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
    public async Task Sum_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>().Select(p => p.Children.Sum(c => c.Score)).ToList());
        Assert.Contains("converter", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Max_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>().Select(p => p.Children.Max(c => c.Score)).ToList());
    }

    [Fact]
    public async Task First_selecting_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>().Select(p => p.Children.OrderBy(c => c.Id).Select(c => c.Score).FirstOrDefault()).ToList());
    }

    [Fact]
    public async Task First_ordered_by_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Parent>().Select(p => p.Children.OrderByDescending(c => c.Score).Select(c => c.Id).FirstOrDefault()).ToList());
    }
}
