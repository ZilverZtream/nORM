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
/// Oracle-compared coverage for navigation-collection aggregates with COMPUTED selectors —
/// <c>p.Children.Sum(c =&gt; c.Qty * c.Price)</c> (line-item totals), arithmetic, ternary and filtered
/// selectors, in projection and predicate positions. These worked in a predicate but the projection path
/// only handled member selectors and threw for computed ones; the projection emit now translates the
/// selector through a full sub-visitor (the same mechanism the predicate side uses), so both positions
/// support the identical selector shapes. Includes a childless parent (empty collection sums to 0).
/// Compared against an in-memory graph oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationAggregateComputedSelectorTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NacsParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NacsChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Qty { get; set; }
        public int Price { get; set; }
        public Parent Parent { get; set; } = default!;
    }

    private static readonly Parent[] Parents = Enumerable.Range(1, 4).Select(i => new Parent { Id = i }).ToArray();
    private static readonly Child[] Children = new[]
    {
        new Child { Id = 1, ParentId = 1, Qty = 2, Price = 5 }, new Child { Id = 2, ParentId = 1, Qty = 3, Price = 10 }, new Child { Id = 3, ParentId = 1, Qty = 2, Price = 5 },
        new Child { Id = 4, ParentId = 2, Qty = 1, Price = 20 }, new Child { Id = 5, ParentId = 2, Qty = 4, Price = 2 },
        new Child { Id = 6, ParentId = 4, Qty = 5, Price = 7 }, // Parent 3 empty
    };

    private static IEnumerable<Child> Ch(int pid) => Children.Where(c => c.ParentId == pid);

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NacsParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NacsChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Qty INTEGER NOT NULL, Price INTEGER NOT NULL);";
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
        foreach (var p in Parents) await ctx.InsertAsync(new Parent { Id = p.Id });
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, ParentId = c.ParentId, Qty = c.Qty, Price = c.Price });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Parent>, IEnumerable<TR>> q, Func<IEnumerable<Parent>, IEnumerable<TR>> oracle)
    {
        var expected = oracle(Parents.AsEnumerable()).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Parent>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public Task Sum_product_selector() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Sum(c => c.Qty * c.Price)), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Sum(c => c.Qty * c.Price)));
    [Fact] public Task Sum_addition_selector() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Sum(c => c.Qty + c.Price)), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Sum(c => c.Qty + c.Price)));
    [Fact] public Task Sum_ternary_selector() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Sum(c => c.Qty > 2 ? c.Price : 0)), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Sum(c => c.Qty > 2 ? c.Price : 0)));
    [Fact] public Task Filtered_then_computed_sum() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Where(c => c.Price > 4).Sum(c => c.Qty * c.Price)), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Where(c => c.Price > 4).Sum(c => c.Qty * c.Price)));
    [Fact] public Task Max_computed_selector_guarded() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Any() ? p.Children.Max(c => c.Qty * c.Price) : -1), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Any() ? Ch(p.Id).Max(c => c.Qty * c.Price) : -1));
    [Fact] public Task Computed_sum_in_predicate() => Assert_(q => q.Where(p => p.Children.Sum(c => c.Qty * c.Price) > 30).OrderBy(p => p.Id).Select(p => p.Id), o => o.Where(p => Ch(p.Id).Sum(c => c.Qty * c.Price) > 30).OrderBy(p => p.Id).Select(p => p.Id));
    [Fact] public Task Computed_sum_in_concat() => Assert_(q => q.OrderBy(p => p.Id).Select(p => "T=" + p.Children.Sum(c => c.Qty * c.Price)), o => o.OrderBy(p => p.Id).Select(p => "T=" + Ch(p.Id).Sum(c => c.Qty * c.Price)));
}
