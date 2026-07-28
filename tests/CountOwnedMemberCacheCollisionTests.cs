using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The Count fast-path SQL cache must key predicates by the full owned access path, not the leaf member name.
/// A root member (`o.Status`) and an owned member (`o.Ship.Status`) that share a leaf name build different SQL
/// (`Status` vs `Ship_Status`) but hashed to the same cache key, so whichever `Count(predicate)` ran first
/// pinned its column for both — the second silently counted the wrong column. The simple-query cache already
/// keys by the full path; Count must match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CountOwnedMemberCacheCollisionTests
{
    public class Address { public string Status { get; set; } = ""; }

    [Table("CoMcOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Status { get; set; } = "";
        public Address Ship { get; set; } = new();
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CoMcOrder (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL, Ship_Status TEXT NOT NULL);" +
                // root Status: shipped x3, delivered x2 ; owned Ship_Status: pending x3, done x2
                "INSERT INTO CoMcOrder VALUES (1,'shipped','pending'),(2,'shipped','pending'),(3,'shipped','done')," +
                "(4,'delivered','pending'),(5,'delivered','done');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsOne(o => o.Ship);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void Owned_member_count_not_hijacked_by_prior_root_member_count()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Equal(3, ctx.Query<Order>().Count(o => o.Status == "shipped"));   // warms cache
        Assert.Equal(2, ctx.Query<Order>().Count(o => o.Ship.Status == "done")); // must count Ship_Status
    }

    [Fact]
    public void Root_member_count_not_hijacked_by_prior_owned_member_count()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Equal(2, ctx.Query<Order>().Count(o => o.Ship.Status == "done"));  // warms cache
        Assert.Equal(3, ctx.Query<Order>().Count(o => o.Status == "shipped"));    // must count Status
    }
}
