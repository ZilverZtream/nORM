using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for two-hop navigation aggregates in a projection —
/// <c>p.Children.SelectMany(c =&gt; c.Grands).Sum(g =&gt; g.Amount)</c> (sum of grandchildren per parent) and
/// Min/Max/Average over the flattened two-hop leaves. Count/Any/Count(predicate) already worked; the
/// aggregate-with-selector shapes threw an internal error (the selector resolved in the wrong context).
/// The two-hop emit now renders the aggregate over the leaf table, with Sum COALESCEd to 0 for an empty
/// flattened set. Includes parents with no children and no grandchildren. Compared against an in-memory
/// graph oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class TwoHopNavigationAggregateTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("ThnParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("ThnChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public Parent Parent { get; set; } = default!;
        public List<Grand> Grands { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("ThnGrand")]
    public sealed class Grand
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ChildId { get; set; }
        public int Amount { get; set; }
        public Child Child { get; set; } = default!;
    }

    private static readonly Parent[] Parents = Enumerable.Range(1, 3).Select(i => new Parent { Id = i }).ToArray();
    private static readonly Child[] Children = new[]
    {
        new Child { Id = 1, ParentId = 1 }, new Child { Id = 2, ParentId = 1 }, new Child { Id = 3, ParentId = 2 },
    };
    // parent1 grands: 10,20,5 ; parent2: none ; parent3: no children
    private static readonly Grand[] Grands = new[]
    {
        new Grand { Id = 1, ChildId = 1, Amount = 10 }, new Grand { Id = 2, ChildId = 1, Amount = 20 }, new Grand { Id = 3, ChildId = 2, Amount = 5 },
    };

    private static IEnumerable<Grand> GrandsOf(int pid) =>
        Children.Where(c => c.ParentId == pid).SelectMany(c => Grands.Where(g => g.ChildId == c.Id));

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE ThnParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE ThnChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);" +
                "CREATE TABLE ThnGrand (Id INTEGER PRIMARY KEY, ChildId INTEGER NOT NULL, Amount INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Grand>().HasKey(g => g.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId, p => p.Id);
                mb.Entity<Child>().HasMany(c => c.Grands).WithOne(g => g.Child).HasForeignKey(g => g.ChildId, c => c.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var p in Parents) await ctx.InsertAsync(new Parent { Id = p.Id });
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, ParentId = c.ParentId });
        foreach (var g in Grands) await ctx.InsertAsync(new Grand { Id = g.Id, ChildId = g.ChildId, Amount = g.Amount });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Parent>, IEnumerable<TR>> q, Func<IEnumerable<Parent>, IEnumerable<TR>> oracle)
    {
        var expected = oracle(Parents.AsEnumerable()).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Parent>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public Task TwoHop_Sum() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.SelectMany(c => c.Grands).Sum(g => g.Amount)), o => o.OrderBy(p => p.Id).Select(p => GrandsOf(p.Id).Sum(g => g.Amount)));
    [Fact] public Task TwoHop_Max_guarded() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.SelectMany(c => c.Grands).Any() ? p.Children.SelectMany(c => c.Grands).Max(g => g.Amount) : -1), o => o.OrderBy(p => p.Id).Select(p => GrandsOf(p.Id).Any() ? GrandsOf(p.Id).Max(g => g.Amount) : -1));
    [Fact] public Task TwoHop_Sum_empty_is_zero_in_concat() => Assert_(q => q.OrderBy(p => p.Id).Select(p => "t=" + p.Children.SelectMany(c => c.Grands).Sum(g => g.Amount)), o => o.OrderBy(p => p.Id).Select(p => "t=" + GrandsOf(p.Id).Sum(g => g.Amount)));
    [Fact] public Task TwoHop_Sum_computed_selector() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.SelectMany(c => c.Grands).Sum(g => g.Amount * 2)), o => o.OrderBy(p => p.Id).Select(p => GrandsOf(p.Id).Sum(g => g.Amount * 2)));
    [Fact] public Task TwoHop_Count_regression() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.SelectMany(c => c.Grands).Count()), o => o.OrderBy(p => p.Id).Select(p => GrandsOf(p.Id).Count()));
    [Fact] public Task TwoHop_Any_regression() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.SelectMany(c => c.Grands).Any()), o => o.OrderBy(p => p.Id).Select(p => GrandsOf(p.Id).Any()));
}
