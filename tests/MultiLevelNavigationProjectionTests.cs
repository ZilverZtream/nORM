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
/// Oracle-compared coverage for reference-navigation projections across a THREE-level graph
/// (Grand → Parent → Child): single- and two-level nav member access in projection, predicate, ordering
/// and 3-level concat positions, plus an orphan (FK to a missing principal → null nav). Wrong navigation
/// resolution here is silent-wrong, so this directly serves the zero-data-loss verification. Rows are
/// seeded through nORM's own insert path; each case runs the identical LINQ expression against nORM
/// (SQLite) and an in-memory graph oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class MultiLevelNavigationProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("MlnGrand")]
    public sealed class Grand
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string GName { get; set; } = "";
        public List<Parent> Parents { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MlnParent")]
    public sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string PName { get; set; } = "";
        public int GrandId { get; set; }
        public Grand Grand { get; set; } = default!;
        public List<Child> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MlnChild")]
    public sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string CName { get; set; } = "";
        public int ParentId { get; set; } // 0 = orphan (no matching parent)
        public Parent Parent { get; set; } = default!;
    }

    private static readonly Grand[] Grands = { new() { Id = 1, GName = "G1" }, new() { Id = 2, GName = "G2" } };
    private static readonly Parent[] Parents = Enumerable.Range(1, 4).Select(i => new Parent { Id = i, PName = "P" + i, GrandId = (i % 2) + 1 }).ToArray();
    private static readonly Child[] Children = Enumerable.Range(1, 8).Select(i => new Child { Id = i, CName = "C" + i, ParentId = (i == 8) ? 0 : ((i % 4) + 1) }).ToArray();

    private static (Child c, Parent? p, Grand? g) Resolve(Child c)
    {
        var p = Parents.FirstOrDefault(x => x.Id == c.ParentId);
        var g = p != null ? Grands.FirstOrDefault(x => x.Id == p.GrandId) : null;
        return (c, p, g);
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MlnGrand (Id INTEGER PRIMARY KEY, GName TEXT NOT NULL);" +
                "CREATE TABLE MlnParent (Id INTEGER PRIMARY KEY, PName TEXT NOT NULL, GrandId INTEGER NOT NULL);" +
                "CREATE TABLE MlnChild (Id INTEGER PRIMARY KEY, CName TEXT NOT NULL, ParentId INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Grand>().HasKey(g => g.Id);
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Grand>().HasMany(g => g.Parents).WithOne(p => p.Grand).HasForeignKey(p => p.GrandId, g => g.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var g in Grands) await ctx.InsertAsync(new Grand { Id = g.Id, GName = g.GName });
        foreach (var p in Parents) await ctx.InsertAsync(new Parent { Id = p.Id, PName = p.PName, GrandId = p.GrandId });
        foreach (var c in Children) await ctx.InsertAsync(new Child { Id = c.Id, CName = c.CName, ParentId = c.ParentId });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Child>, IEnumerable<TR>> q, Func<IEnumerable<(Child c, Parent? p, Grand? g)>, IEnumerable<TR>> oracle)
    {
        var expected = oracle(Children.Select(Resolve)).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Child>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public Task Parent_name_projection() => Assert_(
        q => q.Where(c => c.ParentId != 0).OrderBy(c => c.Id).Select(c => c.Parent.PName),
        o => o.Where(t => t.c.ParentId != 0).OrderBy(t => t.c.Id).Select(t => t.p!.PName));

    [Fact]
    public Task Two_level_nav_projection() => Assert_(
        q => q.Where(c => c.ParentId != 0).OrderBy(c => c.Id).Select(c => c.Parent.Grand.GName),
        o => o.Where(t => t.c.ParentId != 0).OrderBy(t => t.c.Id).Select(t => t.g!.GName));

    [Fact]
    public Task Nav_member_predicate() => Assert_(
        q => q.Where(c => c.Parent.PName == "P2").OrderBy(c => c.Id).Select(c => c.Id),
        o => o.Where(t => t.p != null && t.p.PName == "P2").OrderBy(t => t.c.Id).Select(t => t.c.Id));

    [Fact]
    public Task Two_level_nav_predicate() => Assert_(
        q => q.Where(c => c.Parent.Grand.GName == "G1").OrderBy(c => c.Id).Select(c => c.Id),
        o => o.Where(t => t.g != null && t.g.GName == "G1").OrderBy(t => t.c.Id).Select(t => t.c.Id));

    [Fact]
    public Task Nav_member_ordering() => Assert_(
        q => q.Where(c => c.ParentId != 0).OrderBy(c => c.Parent.PName).ThenBy(c => c.Id).Select(c => c.CName),
        o => o.Where(t => t.c.ParentId != 0).OrderBy(t => t.p!.PName, StringComparer.Ordinal).ThenBy(t => t.c.Id).Select(t => t.c.CName));

    [Fact]
    public Task Three_level_concat_projection() => Assert_(
        q => q.Where(c => c.ParentId != 0).OrderBy(c => c.Id).Select(c => c.CName + "/" + c.Parent.PName + "/" + c.Parent.Grand.GName),
        o => o.Where(t => t.c.ParentId != 0).OrderBy(t => t.c.Id).Select(t => t.c.CName + "/" + t.p!.PName + "/" + t.g!.GName));

    [Fact]
    public Task Nav_fk_member_predicate() => Assert_(
        q => q.Where(c => c.Parent.GrandId == 1).OrderBy(c => c.Id).Select(c => c.Id),
        o => o.Where(t => t.p != null && t.p.GrandId == 1).OrderBy(t => t.c.Id).Select(t => t.c.Id));
}
