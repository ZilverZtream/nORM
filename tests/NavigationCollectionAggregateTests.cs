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
/// Oracle-compared coverage for navigation-collection aggregates from the principal side
/// (<c>p.Children.Count()/Sum/Any/All/Max/Average</c>) across projection, predicate, ordering and
/// embedded (concat / arithmetic) positions, including a parent with NO children. A key case: an empty
/// navigation collection's <c>Sum</c> is 0 (C# <c>Enumerable.Sum</c> semantics) — previously the SQL SUM's
/// NULL leaked through when the aggregate was embedded (e.g. concatenated), so an empty parent rendered
/// empty instead of "0". The nav-aggregate Sum now COALESCEs to 0 in every context. Compared against an
/// in-memory graph oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationCollectionAggregateTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NcaParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string PName { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NcaChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public Parent Parent { get; set; } = default!;
    }

    private static readonly Parent[] Parents = Enumerable.Range(1, 4).Select(i => new Parent { Id = i, PName = "P" + i }).ToArray();
    private static readonly Child[] Children = new[]
    {
        new Child { Id = 1, ParentId = 1, Val = 1 }, new Child { Id = 2, ParentId = 1, Val = 2 }, new Child { Id = 3, ParentId = 1, Val = 3 },
        new Child { Id = 4, ParentId = 2, Val = 10 }, new Child { Id = 5, ParentId = 2, Val = 20 },
        new Child { Id = 6, ParentId = 4, Val = 7 }, // Parent 3 has no children
    };

    private static IEnumerable<Child> ChildrenOf(int pid) => Children.Where(c => c.ParentId == pid);

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NcaParent (Id INTEGER PRIMARY KEY, PName TEXT NOT NULL);" +
                "CREATE TABLE NcaChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var p in Parents) await ctx.InsertAsync(new Parent { Id = p.Id, PName = p.PName });
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, ParentId = c.ParentId, Val = c.Val });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Parent>, IEnumerable<TR>> q, Func<IEnumerable<Parent>, IEnumerable<TR>> oracle)
    {
        var expected = oracle(Parents.AsEnumerable()).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Parent>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public Task Count_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Count()), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Count()));
    [Fact] public Task Count_predicate_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Count(c => c.Val > 5)), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Count(c => c.Val > 5)));
    [Fact] public Task Sum_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Sum(c => c.Val)), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Sum(c => c.Val)));
    [Fact] public Task Any_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Any()), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Any()));
    [Fact] public Task Any_predicate_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Any(c => c.Val > 8)), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Any(c => c.Val > 8)));
    [Fact] public Task All_predicate_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.All(c => c.Val > 5)), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).All(c => c.Val > 5)));
    [Fact] public Task Count_predicate() => Assert_(q => q.Where(p => p.Children.Count() > 2).OrderBy(p => p.Id).Select(p => p.Id), o => o.Where(p => ChildrenOf(p.Id).Count() > 2).OrderBy(p => p.Id).Select(p => p.Id));
    [Fact] public Task Any_predicate() => Assert_(q => q.Where(p => p.Children.Any(c => c.Val > 15)).OrderBy(p => p.Id).Select(p => p.Id), o => o.Where(p => ChildrenOf(p.Id).Any(c => c.Val > 15)).OrderBy(p => p.Id).Select(p => p.Id));
    [Fact] public Task Count_ordering() => Assert_(q => q.OrderBy(p => p.Children.Count()).ThenBy(p => p.Id).Select(p => p.Id), o => o.OrderBy(p => ChildrenOf(p.Id).Count()).ThenBy(p => p.Id).Select(p => p.Id));
    [Fact] public Task Where_then_count_projection() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Where(c => c.Val > 5).Count()), o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Where(c => c.Val > 5).Count()));
    [Fact] public Task Average_projection_non_empty() => Assert_(q => q.Where(p => p.Children.Any()).OrderBy(p => p.Id).Select(p => p.Children.Average(c => c.Val)), o => o.Where(p => ChildrenOf(p.Id).Any()).OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Average(c => c.Val)));

    // Empty-collection Sum = 0 in embedded contexts (the fix).
    [Fact] public Task Sum_in_concat_empty_is_zero() => Assert_(
        q => q.OrderBy(p => p.Id).Select(p => p.PName + ":" + p.Children.Count() + ":" + p.Children.Sum(c => c.Val)),
        o => o.OrderBy(p => p.Id).Select(p => p.PName + ":" + ChildrenOf(p.Id).Count() + ":" + ChildrenOf(p.Id).Sum(c => c.Val)));
    [Fact] public Task Sum_in_arithmetic_empty_is_zero() => Assert_(
        q => q.OrderBy(p => p.Id).Select(p => p.Children.Sum(c => c.Val) + 1000),
        o => o.OrderBy(p => p.Id).Select(p => ChildrenOf(p.Id).Sum(c => c.Val) + 1000));
    [Fact] public Task Sum_in_predicate() => Assert_(
        q => q.Where(p => p.Children.Sum(c => c.Val) > 0).OrderBy(p => p.Id).Select(p => p.Id),
        o => o.Where(p => ChildrenOf(p.Id).Sum(c => c.Val) > 0).OrderBy(p => p.Id).Select(p => p.Id));
}
