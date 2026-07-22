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
/// Oracle-compared coverage for distinct-count over a navigation collection in a projection —
/// <c>p.Children.Select(c =&gt; c.Cat).Distinct().Count()</c> (→ <c>COUNT(DISTINCT col)</c>), the
/// whole-entity <c>p.Children.Distinct().Count()</c> (distinct is a no-op given the unique key → COUNT(*)),
/// and a filtered variant. Previously the analyzer flagged the chain as client-evaluation because Distinct
/// was not in its translatable set. Gated to non-nullable value-type selectors (SQL COUNT(DISTINCT) ignores
/// NULLs while C# Distinct counts null as one value). Includes a childless parent (0). Compared against an
/// in-memory graph oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationDistinctCountTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NdcParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NdcChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Cat { get; set; }
        public Parent Parent { get; set; } = default!;
    }

    // Parent 1 cats {1,1,2}; Parent 2 {5,5}; Parent 3 empty; Parent 4 {7}
    private static readonly Parent[] Parents = Enumerable.Range(1, 4).Select(i => new Parent { Id = i }).ToArray();
    private static readonly Child[] Children = new[]
    {
        new Child { Id = 1, ParentId = 1, Cat = 1 }, new Child { Id = 2, ParentId = 1, Cat = 1 }, new Child { Id = 3, ParentId = 1, Cat = 2 },
        new Child { Id = 4, ParentId = 2, Cat = 5 }, new Child { Id = 5, ParentId = 2, Cat = 5 },
        new Child { Id = 6, ParentId = 4, Cat = 7 },
    };

    private static IEnumerable<Child> Ch(int pid) => Children.Where(c => c.ParentId == pid);

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NdcParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NdcChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Cat INTEGER NOT NULL);";
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
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, ParentId = c.ParentId, Cat = c.Cat });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Parent>, IEnumerable<TR>> q, Func<IEnumerable<Parent>, IEnumerable<TR>> oracle)
    {
        var expected = oracle(Parents.AsEnumerable()).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Parent>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public Task Distinct_count_scalar_selector() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Select(c => c.Cat).Distinct().Count()), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Select(c => c.Cat).Distinct().Count()));
    [Fact] public Task Distinct_count_in_concat() => Assert_(q => q.OrderBy(p => p.Id).Select(p => "d=" + p.Children.Select(c => c.Cat).Distinct().Count()), o => o.OrderBy(p => p.Id).Select(p => "d=" + Ch(p.Id).Select(c => c.Cat).Distinct().Count()));
    [Fact] public Task Distinct_count_whole_entity() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Distinct().Count()), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Distinct().Count()));
    [Fact] public Task Filtered_distinct_count() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Where(c => c.Cat > 1).Select(c => c.Cat).Distinct().Count()), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Where(c => c.Cat > 1).Select(c => c.Cat).Distinct().Count()));
    [Fact] public Task Distinct_count_arithmetic() => Assert_(q => q.OrderBy(p => p.Id).Select(p => p.Children.Select(c => c.Cat).Distinct().Count() * 10), o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).Select(c => c.Cat).Distinct().Count() * 10));
}
