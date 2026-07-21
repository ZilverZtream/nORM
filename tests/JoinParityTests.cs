using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Core join shapes checked against a LINQ-to-Objects oracle over the same rows: an inner join (a dangling
/// foreign key drops out), a group-join / left join (a parent with no children is kept with count 0), a
/// join followed by where + order, and a self-join. nORM must produce the same results.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class JoinParityTests
{
    [Table("JpCustomer")]
    public class Customer { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("JpOrder")]
    public class Order { [Key] public int Id { get; set; } public int CustomerId { get; set; } public int Amount { get; set; } }

    private static readonly (int Id, string Name)[] Customers = { (1, "alice"), (2, "bob"), (3, "carol") };
    private static readonly (int Id, int CustomerId, int Amount)[] Orders =
    {
        (10, 1, 100), (11, 1, 50), (12, 2, 30), (13, 99, 999),   // 99 has no customer (dangling)
    };

    private static (SqliteConnection, DbContext) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE JpCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "CREATE TABLE JpOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL);";
            foreach (var (id, name) in Customers) cmd.CommandText += $"INSERT INTO JpCustomer VALUES ({id},'{name}');";
            foreach (var (id, cid, amt) in Orders) cmd.CommandText += $"INSERT INTO JpOrder VALUES ({id},{cid},{amt});";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static List<Customer> OracleCustomers() => Customers.Select(c => new Customer { Id = c.Id, Name = c.Name }).ToList();
    private static List<Order> OracleOrders() => Orders.Select(o => new Order { Id = o.Id, CustomerId = o.CustomerId, Amount = o.Amount }).ToList();

    [Fact]
    public async Task Inner_join_matches_the_oracle_and_drops_the_dangling_foreign_key()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Order>()
            .Join(ctx.Query<Customer>(), o => o.CustomerId, c => c.Id, (o, c) => new { c.Name, o.Amount })
            .ToListAsync()).OrderBy(x => x.Name).ThenBy(x => x.Amount).Select(x => $"{x.Name}:{x.Amount}").ToList();
        var oracle = OracleOrders()
            .Join(OracleCustomers(), o => o.CustomerId, c => c.Id, (o, c) => new { c.Name, o.Amount })
            .OrderBy(x => x.Name).ThenBy(x => x.Amount).Select(x => $"{x.Name}:{x.Amount}").ToList();

        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Group_join_keeps_a_parent_with_no_children_matching_the_oracle()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Customer>()
            .GroupJoin(ctx.Query<Order>(), c => c.Id, o => o.CustomerId, (c, os) => new { c.Name, Count = os.Count() })
            .ToListAsync()).OrderBy(x => x.Name).Select(x => $"{x.Name}:{x.Count}").ToList();
        var oracle = OracleCustomers()
            .GroupJoin(OracleOrders(), c => c.Id, o => o.CustomerId, (c, os) => new { c.Name, Count = os.Count() })
            .OrderBy(x => x.Name).Select(x => $"{x.Name}:{x.Count}").ToList();

        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Join_then_where_then_order_matches_the_oracle()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Order>()
            .Join(ctx.Query<Customer>(), o => o.CustomerId, c => c.Id, (o, c) => new { c.Name, o.Amount })
            .Where(x => x.Amount >= 50)
            .OrderByDescending(x => x.Amount)
            .ToListAsync()).Select(x => $"{x.Name}:{x.Amount}").ToList();
        var oracle = OracleOrders()
            .Join(OracleCustomers(), o => o.CustomerId, c => c.Id, (o, c) => new { c.Name, o.Amount })
            .Where(x => x.Amount >= 50)
            .OrderByDescending(x => x.Amount)
            .Select(x => $"{x.Name}:{x.Amount}").ToList();

        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Self_join_matches_the_oracle()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Order>()
            .Join(ctx.Query<Order>(), a => a.CustomerId, b => b.CustomerId, (a, b) => new { A = a.Id, B = b.Id })
            .Where(x => x.A < x.B)
            .ToListAsync()).OrderBy(x => x.A).ThenBy(x => x.B).Select(x => $"{x.A}-{x.B}").ToList();
        var oracle = OracleOrders()
            .Join(OracleOrders(), a => a.CustomerId, b => b.CustomerId, (a, b) => new { A = a.Id, B = b.Id })
            .Where(x => x.A < x.B)
            .OrderBy(x => x.A).ThenBy(x => x.B).Select(x => $"{x.A}-{x.B}").ToList();

        Assert.Equal(oracle, norm);
    }
}
