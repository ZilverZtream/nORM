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
/// A Join whose INNER source is windowed with OrderBy + Take (a global top-N subquery) must join only the
/// windowed rows. The outer source is already wrapped as a derived table when windowed; the inner was not, so
/// the Take/OrderBy on the inner was silently dropped and the join ran against the whole inner table —
/// returning extra rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class JoinWindowedInnerSourceTests
{
    [Table("JwiCustomer")]
    public class Customer
    {
        [Key] public int Id { get; set; }
    }

    [Table("JwiOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int Amount { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE JwiCustomer (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE JwiOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL);" +
                "INSERT INTO JwiCustomer VALUES (1), (2);" +
                "INSERT INTO JwiOrder VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200), (4, 2, 10);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Join_over_a_windowed_inner_source_joins_only_the_windowed_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Inner = the two highest orders (Amount 200 -> order 3, Amount 100 -> order 1). Joined to their
        // customers: {2, 200} and {1, 100}. LINQ-to-Objects returns exactly those two rows.
        var rows = ctx.Query<Customer>()
            .Join(ctx.Query<Order>().OrderByDescending(o => o.Amount).Take(2),
                  c => c.Id, o => o.CustomerId, (c, o) => new { CustomerId = c.Id, o.Amount })
            .OrderBy(x => x.CustomerId)
            .ToList()
            .Select(x => (x.CustomerId, x.Amount))
            .ToList();

        Assert.Equal(new[] { (1, 100), (2, 200) }, rows.ToArray());
    }

    [Fact]
    public void GroupJoin_over_a_windowed_inner_source_groups_only_the_windowed_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Inner = top-2 orders (order 3 for cust 2, order 1 for cust 1). Each customer groups exactly one.
        var rows = ctx.Query<Customer>()
            .GroupJoin(ctx.Query<Order>().OrderByDescending(o => o.Amount).Take(2),
                       c => c.Id, o => o.CustomerId, (c, orders) => new { CustomerId = c.Id, Count = orders.Count() })
            .OrderBy(x => x.CustomerId)
            .ToList()
            .Select(x => (x.CustomerId, x.Count))
            .ToList();

        Assert.Equal(new[] { (1, 1), (2, 1) }, rows.ToArray());
    }

    [Fact]
    public void LeftJoin_over_a_windowed_inner_source_matches_only_the_windowed_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var rows = (from c in ctx.Query<Customer>()
                    join o in ctx.Query<Order>().OrderByDescending(o => o.Amount).Take(2)
                        on c.Id equals o.CustomerId into g
                    from o in g.DefaultIfEmpty()
                    select new { CustomerId = c.Id, Amount = (int?)o.Amount })
            .OrderBy(x => x.CustomerId)
            .ToList()
            .Select(x => (x.CustomerId, x.Amount))
            .ToList();

        // Each customer matches its single windowed order (top-2 = order 1 for cust 1, order 3 for cust 2).
        Assert.Equal(new[] { (1, (int?)100), (2, (int?)200) }, rows.ToArray());
    }
}
