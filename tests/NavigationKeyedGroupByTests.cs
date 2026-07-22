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
/// Coverage for GroupBy keyed by a reference-navigation member. The composition
/// <c>GroupBy(c =&gt; c.Parent.Id).OrderBy(g =&gt; g.Key).Select(g =&gt; computed)</c> previously threw an
/// unhandled NullReferenceException: the intermediate OrderBy forces client-side streaming grouping, whose
/// compiled key selector dereferenced the (unloaded) Parent navigation. The key selector's
/// <c>nav.PrincipalKey</c> access is now rewritten to the local foreign-key column (a materialized value),
/// so the common FK-equivalent case works; a non-key nav member (whose value isn't in the row) fails loud
/// with a clear message instead of crashing. Oracle-compared against LINQ over the FK.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationKeyedGroupByTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NkgParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Region { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NkgChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public Parent Parent { get; set; } = default!;
    }

    private static readonly Parent[] Parents = Enumerable.Range(1, 3).Select(i => new Parent { Id = i, Region = i % 2 }).ToArray();
    private static readonly Child[] Children = Enumerable.Range(1, 12).Select(i => new Child { Id = i, ParentId = (i % 3) + 1, Val = i }).ToArray();

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NkgParent (Id INTEGER PRIMARY KEY, Region INTEGER NOT NULL);" +
                "CREATE TABLE NkgChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);";
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
        foreach (var p in Parents) await ctx.InsertAsync(new Parent { Id = p.Id, Region = p.Region });
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, ParentId = c.ParentId, Val = c.Val });
        return ctx;
    }

    [Fact]
    public async Task NavKey_with_intermediate_orderby_and_computed_select()
    {
        var expected = Children.GroupBy(c => c.ParentId).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()).ToList();
        using var ctx = await CtxAsync();
        var actual = ctx.Query<Child>().GroupBy(c => c.Parent.Id).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task NavKey_direct_projection_still_works()
    {
        var expected = Children.GroupBy(c => c.ParentId).OrderBy(g => g.Key).Select(g => g.Count()).ToList();
        using var ctx = await CtxAsync();
        var actual = ctx.Query<Child>().GroupBy(c => c.Parent.Id).Select(g => g.Count()).OrderBy(c => c).ToList();
        Assert.Equal(expected.OrderBy(c => c).ToList(), actual);
    }

    [Fact]
    public async Task NavKey_anonymous_projection()
    {
        var expected = Children.GroupBy(c => c.ParentId).OrderBy(g => g.Key).Select(g => (g.Key, C: g.Count())).ToList();
        using var ctx = await CtxAsync();
        var actual = ctx.Query<Child>().GroupBy(c => c.Parent.Id).Select(g => new { g.Key, C = g.Count() })
            .ToList().Select(x => (x.Key, x.C)).OrderBy(x => x.Key).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task NonKey_nav_member_fails_loud_not_nre()
    {
        using var ctx = await CtxAsync();
        // Grouping by a NON-key nav member can't run client-side (value not in the row);
        // it must fail loud with an actionable message, never a NullReferenceException.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Child>().GroupBy(c => c.Parent.Region).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()).ToList());
        Assert.Contains("navigation", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
