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

[Trait("Category", "Fast")]
public class MultiTableCacheInvalidationTests
{
    [Table("MtcParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("MtcChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public int Val { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(NormMemoryCacheProvider cache)
    {
        var keeper = new SqliteConnection($"Data Source=file:mtc_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MtcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE MtcChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO MtcParent VALUES (1,'a'),(2,'b');
                INSERT INTO MtcChild VALUES (1,1,10),(2,1,20);
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
    public async Task Join_query_invalidated_by_write_to_joined_table()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        // Cache a JOIN: parent name + child val for parent 1 (2 rows).
        var before = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking()
            .Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.Name, c.Val })
            .Where(x => x.Name == "a")
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(2, before.Count);

        // Add a third child to parent 1 → the cached JOIN result must refresh.
        ctx.Add(new Child { Id = 3, ParentId = 1, Val = 30 });
        await ctx.SaveChangesAsync();

        var after = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking()
            .Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.Name, c.Val })
            .Where(x => x.Name == "a")
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(3, after.Count);
    }

    [Fact]
    public async Task Navigation_aggregate_query_invalidated_by_write_to_child()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        // Cache a navigation-aggregate projection: p.Children.Count().
        var before = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Where(p => p.Id == 1)
            .Select(p => new { p.Id, Cnt = p.Children.Count() })
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(2, before[0].Cnt);

        ctx.Add(new Child { Id = 3, ParentId = 1, Val = 30 });
        await ctx.SaveChangesAsync();

        var after = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking().Where(p => p.Id == 1)
            .Select(p => new { p.Id, Cnt = p.Children.Count() })
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToList();
        Assert.Equal(3, after[0].Cnt);
    }
}
