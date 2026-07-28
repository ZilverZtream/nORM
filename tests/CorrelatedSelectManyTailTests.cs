using System;
using System.Collections.Generic;
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
/// A correlated SelectMany whose collection body carries an ordering/paging/Distinct tail
/// (`c => c.Orders.OrderByDescending(o => o.Amount).Take(1)` — a top-N-per-group shape) is not translatable
/// to a single SQLite query (it needs a lateral join). It previously fell through to a CROSS JOIN that
/// dropped both the correlation and the tail, silently returning a cartesian product. It must fail loud
/// instead. Genuine (uncorrelated) cross joins and the supported correlated shapes are unaffected.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedSelectManyTailTests
{
    [Table("CsmCustomer")]
    public class Customer
    {
        [Key] public int Id { get; set; }
        public List<Order> Orders { get; set; } = new();
    }

    [Table("CsmOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int Amount { get; set; }
        public Customer? Customer { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CsmCustomer (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE CsmOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL);" +
                "INSERT INTO CsmCustomer VALUES (1), (2);" +
                "INSERT INTO CsmOrder VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200), (4, 2, 10);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Customer>().HasMany(c => c.Orders).WithOne(o => o.Customer).HasForeignKey(o => o.CustomerId, c => c.Id)
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void Correlated_selectmany_with_take_tail_fails_loud_not_cartesian()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Oracle would be top-1 order per customer (2 rows). The old translation emitted a CROSS JOIN
        // (2 customers x 4 orders = 8 rows) — silently wrong. It must throw a clear, actionable error instead.
        var ex = Record.Exception(() =>
            ctx.Query<Customer>().SelectMany(c => c.Orders.OrderByDescending(o => o.Amount).Take(1)).ToList());

        Assert.IsType<NormUnsupportedFeatureException>(ex);
    }

    [Fact]
    public void Bare_correlated_navigation_selectmany_still_works()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // The supported shape (bare navigation collection) must keep working — an INNER JOIN, all 4 orders.
        var ids = ctx.Query<Customer>().SelectMany(c => c.Orders).Select(o => o.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2, 3, 4 }, ids.ToArray());
    }
}
