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
/// Oracle-compared coverage for the "top related value" projection —
/// <c>p.Children.OrderByDescending(c =&gt; c.Val).Select(c =&gt; c.Val).FirstOrDefault()</c> and friends —
/// emitted as a LIMIT-1 correlated subquery. Covers FirstOrDefault (empty → default), a filtered variant,
/// ThenBy tie-breaks, and Last (reversed ordering). Only deterministic orderings on SQL-order-safe key
/// types are admitted (string/decimal/TimeSpan order keys keep the client-eval fallback to avoid a
/// silent-wrong first row). Compared against an in-memory graph oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationOrderedFirstTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NofParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NofChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public int Seq { get; set; }
        public Parent Parent { get; set; } = default!;
    }

    // Parent 1 vals {3,1,2}; Parent 2 {10,20}; Parent 3 empty; Parent 4 {7}
    private static readonly Parent[] Parents = Enumerable.Range(1, 4).Select(i => new Parent { Id = i }).ToArray();
    private static readonly Child[] Children = new[]
    {
        new Child { Id = 1, ParentId = 1, Val = 3, Seq = 1 }, new Child { Id = 2, ParentId = 1, Val = 1, Seq = 2 }, new Child { Id = 3, ParentId = 1, Val = 2, Seq = 3 },
        new Child { Id = 4, ParentId = 2, Val = 10, Seq = 1 }, new Child { Id = 5, ParentId = 2, Val = 20, Seq = 2 },
        new Child { Id = 6, ParentId = 4, Val = 7, Seq = 1 },
    };

    private static IEnumerable<Child> Ch(int pid) => Children.Where(c => c.ParentId == pid);

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NofParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NofChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL, Seq INTEGER NOT NULL);";
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
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, ParentId = c.ParentId, Val = c.Val, Seq = c.Seq });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Parent>, IEnumerable<TR>> q, Func<IEnumerable<Parent>, IEnumerable<TR>> oracle)
    {
        var expected = oracle(Parents.AsEnumerable()).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Parent>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public Task Top_value_first_or_default() => Assert_(
        q => q.Where(p => p.Children.Any()).OrderBy(p => p.Id).Select(p => p.Children.OrderByDescending(c => c.Val).Select(c => c.Val).FirstOrDefault()),
        o => o.Where(p => Ch(p.Id).Any()).OrderBy(p => p.Id).Select(p => Ch(p.Id).OrderByDescending(c => c.Val).Select(c => c.Val).FirstOrDefault()));

    [Fact]
    public Task Empty_first_or_default_is_default() => Assert_(
        q => q.OrderBy(p => p.Id).Select(p => p.Children.OrderBy(c => c.Val).Select(c => c.Val).FirstOrDefault()),
        o => o.OrderBy(p => p.Id).Select(p => Ch(p.Id).OrderBy(c => c.Val).Select(c => c.Val).FirstOrDefault()));

    [Fact]
    public Task Filtered_ordered_first() => Assert_(
        q => q.Where(p => p.Children.Any(c => c.Val > 5)).OrderBy(p => p.Id).Select(p => p.Children.Where(c => c.Val > 5).OrderBy(c => c.Val).Select(c => c.Val).FirstOrDefault()),
        o => o.Where(p => Ch(p.Id).Any(c => c.Val > 5)).OrderBy(p => p.Id).Select(p => Ch(p.Id).Where(c => c.Val > 5).OrderBy(c => c.Val).Select(c => c.Val).FirstOrDefault()));

    [Fact]
    public Task ThenBy_tiebreak_first() => Assert_(
        q => q.Where(p => p.Children.Any()).OrderBy(p => p.Id).Select(p => p.Children.OrderBy(c => c.Val).ThenBy(c => c.Seq).Select(c => c.Id).FirstOrDefault()),
        o => o.Where(p => Ch(p.Id).Any()).OrderBy(p => p.Id).Select(p => Ch(p.Id).OrderBy(c => c.Val).ThenBy(c => c.Seq).Select(c => c.Id).FirstOrDefault()));

    [Fact]
    public Task Last_reverses_ordering() => Assert_(
        q => q.Where(p => p.Children.Any()).OrderBy(p => p.Id).Select(p => p.Children.OrderBy(c => c.Val).Select(c => c.Val).LastOrDefault()),
        o => o.Where(p => Ch(p.Id).Any()).OrderBy(p => p.Id).Select(p => Ch(p.Id).OrderBy(c => c.Val).Select(c => c.Val).LastOrDefault()));

    [Fact]
    public Task Ordered_first_in_arithmetic() => Assert_(
        q => q.Where(p => p.Children.Any()).OrderBy(p => p.Id).Select(p => p.Children.OrderByDescending(c => c.Val).Select(c => c.Val).FirstOrDefault() * 10),
        o => o.Where(p => Ch(p.Id).Any()).OrderBy(p => p.Id).Select(p => Ch(p.Id).OrderByDescending(c => c.Val).Select(c => c.Val).FirstOrDefault() * 10));
}
