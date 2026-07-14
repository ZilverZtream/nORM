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
/// Projecting a conditional or coalesce over an OPTIONAL reference-nav member must use SQL
/// null-propagation semantics: with a null FK the nav joins to no row, so the member reads as
/// SQL NULL. A ternary null-guard yields its else branch; a coalesce yields its fallback. A naive
/// INNER JOIN would drop the null-FK rows entirely, silently shortening the projected sequence.
/// (Coalesce cannot be oracled against LINQ-to-Objects — that would NRE on the null nav — so the
/// SQL null-propagation contract is asserted directly.)
/// </summary>
[Trait("Category", "Fast")]
public class OptionalNavProjectionConditionalTests
{
    [Table("OnpCustomer")]
    public class Customer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? Rating { get; set; }
    }

    [Table("OnpOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public int? CustomerId { get; set; }
        [ForeignKey(nameof(CustomerId))] public Customer? Customer { get; set; }
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OnpCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Rating INTEGER NULL);
                CREATE TABLE OnpOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, CustomerId INTEGER NULL);
                INSERT INTO OnpCustomer VALUES (1,'Ann',5),(2,'Bob',NULL);
                INSERT INTO OnpOrder VALUES (1,'o1',1),(2,'o2',NULL),(3,'o3',2),(4,'o4',NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Customer>().HasKey(c => c.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Ternary_null_guard_over_optional_nav_keeps_all_rows()
    {
        await using var ctx = Make();
        var labels = (await ctx.Query<Order>().OrderBy(o => o.Id)
                .Select(o => new { o.Id, Label = o.Customer != null ? o.Customer.Name : "none" })
                .ToListAsync())
            .Select(x => x.Label).ToArray();

        // All four orders present; null-FK orders take the else branch.
        Assert.Equal(new[] { "Ann", "none", "Bob", "none" }, labels);
    }

    [Fact]
    public async Task Coalesce_over_optional_nav_member_propagates_null_to_fallback()
    {
        await using var ctx = Make();
        var labels = (await ctx.Query<Order>().OrderBy(o => o.Id)
                .Select(o => new { o.Id, Label = o.Customer!.Name ?? "none" })
                .ToListAsync())
            .Select(x => x.Label).ToArray();

        // SQL LEFT JOIN => null-FK orders read Name as NULL => coalesce to "none".
        Assert.Equal(new[] { "Ann", "none", "Bob", "none" }, labels);
    }

    [Fact]
    public async Task Coalesce_over_nullable_nav_scalar_uses_both_null_sources()
    {
        await using var ctx = Make();
        // Rating is null for Bob (present customer, null column) AND for null-FK orders (no row).
        // Both null sources must collapse to the fallback 0.
        var ratings = (await ctx.Query<Order>().OrderBy(o => o.Id)
                .Select(o => new { o.Id, R = o.Customer!.Rating ?? 0 })
                .ToListAsync())
            .Select(x => x.R).ToArray();

        Assert.Equal(new[] { 5, 0, 0, 0 }, ratings);   // Ann=5, o2 null-FK=0, Bob null-Rating=0, o4 null-FK=0
    }
}
