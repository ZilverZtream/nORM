using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class SoftDeleteFilterSubqueryTests
{
    [Table("SdfParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("SdfChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public bool IsDeleted { get; set; }
    }

    // Parent 1: 2 live + 1 deleted. Parent 2: 0 live + 2 deleted. Parent 3: 1 live.
    private static readonly (int Id, int Pid, bool Del)[] ChildRows =
    {
        (1, 1, false), (2, 1, false), (3, 1, true),
        (4, 2, true), (5, 2, true),
        (6, 3, false),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SdfParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE SdfChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO SdfParent VALUES (1,'a'),(2,'b'),(3,'c');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, del) in ChildRows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO SdfChild VALUES ({id},{pid},{(del ? 1 : 0)})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        opts.AddGlobalFilter<Child>(c => !c.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // Live (non-deleted) children only.
    private static IEnumerable<Child> LiveChildren() =>
        ChildRows.Where(r => !r.Del).Select(r => new Child { Id = r.Id, ParentId = r.Pid });

    [Fact]
    public async Task Correlated_count_excludes_soft_deleted_children()
    {
        await using var ctx = Make();
        // Live counts: p1=2, p2=0, p3=1.
        var got = (await ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Cnt = ctx.Query<Child>().Count(c => c.ParentId == p.Id) })
            .ToListAsync());
        var byId = got.ToDictionary(x => x.Id, x => x.Cnt);
        Assert.Equal(2, byId[1]);
        Assert.Equal(0, byId[2]); // both children soft-deleted → 0, not 2
        Assert.Equal(1, byId[3]);
    }

    [Fact]
    public async Task Navigation_aggregate_excludes_soft_deleted_children()
    {
        await using var ctx = Make();
        var got = (await ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Cnt = p.Children.Count() })
            .ToListAsync());
        var byId = got.ToDictionary(x => x.Id, x => x.Cnt);
        Assert.Equal(2, byId[1]);
        Assert.Equal(0, byId[2]);
        Assert.Equal(1, byId[3]);
    }

    [Fact]
    public async Task Predicate_has_any_live_child_excludes_soft_deleted()
    {
        await using var ctx = Make();
        // Parents with at least one LIVE child: p1, p3 (p2 has only deleted).
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Any(c => c.ParentId == p.Id))
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 3 }, got);
    }
}
