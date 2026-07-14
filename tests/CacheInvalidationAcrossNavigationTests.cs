using System;
using System.Collections.Generic;
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
/// A Cacheable query that reads a child table through a navigation — an Include eager-load or
/// a predicate navigation aggregate — must be invalidated when that child table is written, so
/// the re-query reflects the change rather than serving a stale cached graph/result set.
/// </summary>
[Trait("Category", "Fast")]
public class CacheInvalidationAcrossNavigationTests
{
    [Table("IciParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("IciChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public int Val { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(NormMemoryCacheProvider cache)
    {
        var keeper = new SqliteConnection($"Data Source=file:ici_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IciParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE IciChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO IciParent VALUES (1,'a');
                INSERT INTO IciChild VALUES (1,1,10),(2,1,20);
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
                OnModelCreating = mb =>
                {
                    mb.Entity<Parent>().HasKey(p => p.Id);
                    mb.Entity<Child>().HasKey(c => c.Id);
                    mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    [Fact]
    public async Task Include_query_reflects_child_write_across_cache()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        var before = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Include(p => p.Children).Where(p => p.Id == 1)
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(2, before[0].Children.Count);

        // Add a third child; a cached Include graph must reflect it, not serve the stale 2.
        ctx.Add(new Child { Id = 3, ParentId = 1, Val = 30 });
        await ctx.SaveChangesAsync();

        var after = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Include(p => p.Children).Where(p => p.Id == 1)
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(3, after[0].Children.Count);
    }

    [Fact]
    public async Task Predicate_navigation_aggregate_query_reflects_child_write_across_cache()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        // Cache parents having >= 3 children: initially none.
        var before = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Where(p => p.Children.Count() >= 3)
            .Select(p => new { p.Id })
            .Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Empty(before);

        ctx.Add(new Child { Id = 3, ParentId = 1, Val = 30 });
        await ctx.SaveChangesAsync();

        // Parent 1 now has 3 children → must appear, not the stale empty cached set.
        var after = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Where(p => p.Children.Count() >= 3)
            .Select(p => new { p.Id })
            .Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Single(after);
        Assert.Equal(1, after[0].Id);
    }
}
