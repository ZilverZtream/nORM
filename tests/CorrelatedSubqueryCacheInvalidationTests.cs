using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A Cacheable query whose result depends on a correlated-subquery table must be tagged with
/// that table too, so a write to it invalidates the cached entry. Previously only the root
/// table tagged the entry, so a child write left a stale correlated aggregate cached.
/// </summary>
[Trait("Category", "Fast")]
public class CorrelatedSubqueryCacheInvalidationTests
{
    [Table("CsciParent")]
    public class Parent { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("CsciChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(NormMemoryCacheProvider cache)
    {
        var keeper = new SqliteConnection($"Data Source=file:csci_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CsciParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CsciChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
                INSERT INTO CsciParent VALUES (1,'a'),(2,'b');
                INSERT INTO CsciChild VALUES (1,1),(2,1);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                CacheProvider = cache,
                OnModelCreating = mb => { mb.Entity<Parent>().HasKey(p => p.Id); mb.Entity<Child>().HasKey(c => c.Id); }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    [Fact]
    public async Task Writing_child_invalidates_cached_parent_query_with_correlated_count()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        // Warm the cache: parent 1 has 2 children.
        var before = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking()
            .Where(p => p.Id == 1)
            .Select(p => new { p.Id, Cnt = ctx.Query<Child>().Count(c => c.ParentId == p.Id) })
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(2, before[0].Cnt);

        // Add a third child to parent 1. This writes the Child table; the cached parent query's
        // result DEPENDS on Child, so it must be invalidated — otherwise it serves a stale count.
        ctx.Add(new Child { Id = 3, ParentId = 1 });
        await ctx.SaveChangesAsync();

        var after = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking()
            .Where(p => p.Id == 1)
            .Select(p => new { p.Id, Cnt = ctx.Query<Child>().Count(c => c.ParentId == p.Id) })
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(3, after[0].Cnt); // fresh count, not the stale cached 2
    }

    [Fact]
    public async Task Deleting_child_invalidates_cached_parent_query_with_correlated_count()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        var before = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Where(p => p.Id == 1)
            .Select(p => new { p.Id, Cnt = ctx.Query<Child>().Count(c => c.ParentId == p.Id) })
            .Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Equal(2, before[0].Cnt);

        // Bulk-delete a child; the correlated count must refresh, not serve the stale 2.
        await ctx.Query<Child>().Where(c => c.Id == 1).ExecuteDeleteAsync();

        var after = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Where(p => p.Id == 1)
            .Select(p => new { p.Id, Cnt = ctx.Query<Child>().Count(c => c.ParentId == p.Id) })
            .Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Equal(1, after[0].Cnt);
    }
}
