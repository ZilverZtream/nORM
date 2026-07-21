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
/// Subquery / set-membership predicates checked against a LINQ-to-Objects oracle: list Contains (IN),
/// correlated Any (EXISTS), correlated All — including vacuous truth, where a parent with no children
/// satisfies All — and negated Any (NOT EXISTS). The vacuous-truth and NOT-EXISTS cases are the subtle
/// ones nORM must get right.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SubqueryPredicateTests
{
    [Table("SpCustomer")]
    public class Customer { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("SpOrder")]
    public class Order { [Key] public int Id { get; set; } public int CustomerId { get; set; } public int Amount { get; set; } }

    private static readonly (int Id, string Name)[] Customers = { (1, "alice"), (2, "bob"), (3, "carol"), (4, "dave") };
    private static readonly (int Id, int CustomerId, int Amount)[] Orders =
    {
        (10, 1, 100), (11, 1, 50), (12, 2, 30), (13, 2, 0),   // alice: 100,50 ; bob: 30,0 ; carol/dave: none
    };

    private static (SqliteConnection, DbContext) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "CREATE TABLE SpOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL);";
            foreach (var (id, name) in Customers) cmd.CommandText += $"INSERT INTO SpCustomer VALUES ({id},'{name}');";
            foreach (var (id, cid, amt) in Orders) cmd.CommandText += $"INSERT INTO SpOrder VALUES ({id},{cid},{amt});";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static List<Customer> OC() => Customers.Select(c => new Customer { Id = c.Id, Name = c.Name }).ToList();
    private static List<Order> OO() => Orders.Select(o => new Order { Id = o.Id, CustomerId = o.CustomerId, Amount = o.Amount }).ToList();

    [Fact]
    public async Task List_contains_matches_the_oracle()
    {
        var (cn, ctx) = Build(); using var _ = cn; await using var __ = ctx;
        var ids = new[] { 1, 2 };
        var norm = (await ctx.Query<Order>().Where(o => ids.Contains(o.CustomerId)).ToListAsync())
            .OrderBy(o => o.Id).Select(o => o.Id).ToList();
        var oracle = OO().Where(o => ids.Contains(o.CustomerId)).OrderBy(o => o.Id).Select(o => o.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Correlated_any_exists_matches_the_oracle()
    {
        var (cn, ctx) = Build(); using var _ = cn; await using var __ = ctx;
        var norm = (await ctx.Query<Customer>().Where(c => ctx.Query<Order>().Any(o => o.CustomerId == c.Id)).ToListAsync())
            .OrderBy(c => c.Id).Select(c => c.Name).ToList();
        var oracle = OC().Where(c => OO().Any(o => o.CustomerId == c.Id)).OrderBy(c => c.Id).Select(c => c.Name).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Correlated_all_keeps_vacuously_true_parents_matching_the_oracle()
    {
        var (cn, ctx) = Build(); using var _ = cn; await using var __ = ctx;
        var norm = (await ctx.Query<Customer>().Where(c => ctx.Query<Order>().Where(o => o.CustomerId == c.Id).All(o => o.Amount > 0)).ToListAsync())
            .OrderBy(c => c.Id).Select(c => c.Name).ToList();
        var oracle = OC().Where(c => OO().Where(o => o.CustomerId == c.Id).All(o => o.Amount > 0)).OrderBy(c => c.Id).Select(c => c.Name).ToList();
        Assert.Equal(oracle, norm);   // carol/dave (no orders) are vacuously true
    }

    [Fact]
    public async Task Negated_any_not_exists_matches_the_oracle()
    {
        var (cn, ctx) = Build(); using var _ = cn; await using var __ = ctx;
        var norm = (await ctx.Query<Customer>().Where(c => !ctx.Query<Order>().Any(o => o.CustomerId == c.Id)).ToListAsync())
            .OrderBy(c => c.Id).Select(c => c.Name).ToList();
        var oracle = OC().Where(c => !OO().Any(o => o.CustomerId == c.Id)).OrderBy(c => c.Id).Select(c => c.Name).ToList();
        Assert.Equal(oracle, norm);
    }
}
