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

/// <summary>
/// A cacheable shaped collection projection must tag the CHILD table it reads, not just the parent — a child
/// write has to evict the cached result, otherwise the projected collection serves stale rows (the same
/// multi-table-cache-tag class that a correlated subquery once tripped by tagging only the root table).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CacheableShapedCollectionTests
{
    [Table("CspOrder")]
    public class Order
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("CspLine")]
    public class Line
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
    }

    [Fact]
    public async Task cacheable_shaped_collection_is_invalidated_by_a_child_write()
    {
        using var cache = new NormMemoryCacheProvider();
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CspOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT);
                CREATE TABLE CspLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO CspOrder (Id) VALUES (1);
                INSERT INTO CspLine (OrderId, Sku) VALUES (1, 'a');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var expiry = TimeSpan.FromMinutes(5);

        int SkuCount() => ctx.Query<Order>()
            .Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToList() })
            .Cacheable(expiry).ToList().Single().Skus.Count;

        Assert.Equal(1, SkuCount());   // caches the projected result (order 1 → 1 line)

        // A nORM write to the CHILD table must invalidate the cached projection.
        ctx.Add(new Line { OrderId = 1, Sku = "b" });
        await ctx.SaveChangesAsync();

        Assert.Equal(2, SkuCount());   // re-read reflects the new line — the child table was tagged
    }
}
