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
/// Adding an already-tracked child to a principal's collection navigation (ctx.Add(child); then
/// principal.Children.Add(child), without setting the child's FK or reference navigation) must propagate
/// the principal's key to the child's FK — the collection membership establishes the relationship. Fixup
/// skipped already-tracked children entirely, so the child was inserted with a default (0) FK: an orphan
/// on SQLite (silent) and an FK violation elsewhere. Only unset FKs are filled (a deliberately-set FK is
/// left alone), and a DB-generated principal key still defers to post-insert propagation.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FkPropagationToTrackedChildTests
{
    [Table("FkpOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<OrderLine> Lines { get; set; } = new();
    }

    [Table("FkpLine")]
    public class OrderLine
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
    }

    private static DbContext CreateDb(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FkpOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE FkpLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<OrderLine>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static int OrderIdOf(SqliteConnection cn, int lineId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT OrderId FROM FkpLine WHERE Id = {lineId}";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Tracked_child_added_to_persisted_principals_collection_gets_the_fk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateDb(cn);

        var seed = new Order { Id = 5 };
        ctx.Add(seed);
        await ctx.SaveChangesAsync();   // order 5 persisted, now Unchanged

        var order = await ctx.Query<Order>().FirstAsync(o => o.Id == 5);
        var line = new OrderLine { Id = 1 };   // OrderId unset (0)
        ctx.Add(line);                          // tracked Added
        order.Lines.Add(line);                  // collection membership only
        await ctx.SaveChangesAsync();

        Assert.Equal(5, OrderIdOf(cn, 1));      // BUG: 0 — FK never propagated
    }

    [Fact]
    public async Task Tracked_child_added_to_client_keyed_principal_in_same_save_gets_the_fk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateDb(cn);

        var order = new Order { Id = 7 };       // client-assigned key
        var line = new OrderLine { Id = 2 };
        ctx.Add(order);
        ctx.Add(line);
        order.Lines.Add(line);
        await ctx.SaveChangesAsync();

        Assert.Equal(7, OrderIdOf(cn, 2));
    }
}
