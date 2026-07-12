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
/// A global filter (soft-delete !IsDeleted) on a child entity must also apply inside correlated
/// navigation-predicate subqueries — c.Orders.Any(), .Count(), .All(). Otherwise a customer whose
/// only orders are soft-deleted is wrongly matched by Where(c => c.Orders.Any()) (over-inclusion),
/// and .Count() counts soft-deleted rows.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GlobalFilterNavPredicateLeakTests
{
    [Table("GnpCustomer")]
    private class GnpCustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<GnpOrder> Orders { get; set; } = new();
    }

    [Table("GnpOrder")]
    private class GnpOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static DbContext CreateContext(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GnpCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE GnpOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
                // Customer 1 has a live order; Customer 2's only order is soft-deleted; Customer 3 has none.
                "INSERT INTO GnpCustomer VALUES (1,'live'),(2,'onlydeleted'),(3,'none');" +
                "INSERT INTO GnpOrder VALUES (10,1,0),(11,2,1),(12,1,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GnpCustomer>()
                .HasKey(c => c.Id)
                .HasMany(c => c.Orders).WithOne().HasForeignKey(o => o.CustomerId, c => c.Id)
        };
        opts.AddGlobalFilter<GnpOrder>(o => !o.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Any_over_navigation_honors_child_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        var withLiveOrders = (await ctx.Query<GnpCustomer>()
                .Where(c => c.Orders.Any())
                .ToListAsync())
            .Select(c => c.Id).OrderBy(i => i).ToList();

        // Only customer 1 has a non-deleted order. Customer 2 (only a deleted order) must be excluded.
        Assert.Equal(new[] { 1 }, withLiveOrders);
    }

    [Fact]
    public async Task Count_over_navigation_honors_child_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        var counts = (await ctx.Query<GnpCustomer>()
                .Select(c => new { c.Id, N = c.Orders.Count() })
                .ToListAsync())
            .ToDictionary(x => x.Id, x => x.N);

        Assert.Equal(1, counts[1]); // one live order (the deleted one, Id 12, is excluded)
        Assert.Equal(0, counts[2]); // its only order is soft-deleted
        Assert.Equal(0, counts[3]);
    }
}
