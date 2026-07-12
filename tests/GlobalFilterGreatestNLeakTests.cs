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
/// The greatest-N-per-group correlated subquery (g.OrderByDescending(x => x.Date).First().Amount)
/// re-scans the grouped table correlated only on the group key. A global filter (soft-delete) on
/// that entity must also apply inside the subquery, or a soft-deleted row that happens to be the
/// latest one leaks its scalar into the projection.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GlobalFilterGreatestNLeakTests
{
    [Table("GgnOrder")]
    private class GgnOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int OrderDate { get; set; }
        public int Amount { get; set; }
        public bool IsDeleted { get; set; }
    }

    [Fact]
    public async Task Greatest_n_per_group_subquery_honors_soft_delete_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GgnOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, OrderDate INTEGER NOT NULL, Amount INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
                // Customer 1: order A (date 1, amt 10, live), order B (date 2, amt 20, DELETED).
                // The latest LIVE order is A (amount 10); B is newer but soft-deleted.
                "INSERT INTO GgnOrder VALUES (1,1,1,10,0),(2,1,2,20,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions().AddGlobalFilter<GgnOrder>(o => !o.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var byCustomer = (await ctx.Query<GgnOrder>()
                .GroupBy(o => o.CustomerId)
                .Select(g => new { g.Key, Latest = g.OrderByDescending(o => o.OrderDate).First().Amount })
                .ToListAsync())
            .ToDictionary(x => x.Key, x => x.Latest);

        // Must be 10 (order A, the latest non-deleted) — NOT 20 (order B, soft-deleted).
        Assert.Equal(10, byCustomer[1]);
    }
}
