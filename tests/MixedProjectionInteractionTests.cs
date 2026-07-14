using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A single projection mixing three distinct materialization paths in one anonymous type —
/// a value-converter column, a reference-navigation scalar, and a correlated aggregate — plus
/// a converter column in the WHERE. Stresses column/converter alignment across combined
/// features against the LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", "Fast")]
public class MixedProjectionInteractionTests
{
    public enum Status { Open = 1, Paid = 2, Cancelled = 3 }

    [Table("MpiCustomer")]
    public class Customer { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("MpiOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        [ForeignKey(nameof(CustomerId))] public Customer? Customer { get; set; }
        public Status Status { get; set; }
    }

    [Table("MpiLine")]
    public class Line { [Key] public int Id { get; set; } public int OrderId { get; set; } public int Qty { get; set; } }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MpiCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE MpiOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Status TEXT NOT NULL);
                CREATE TABLE MpiLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Qty INTEGER NOT NULL);
                INSERT INTO MpiCustomer VALUES (1,'Ann'),(2,'Bob');
                INSERT INTO MpiOrder VALUES (1,1,'Open'),(2,1,'Paid'),(3,2,'Open'),(4,2,'Cancelled');
                INSERT INTO MpiLine VALUES (1,1,5),(2,1,3),(3,2,10),(4,3,7),(5,3,1),(6,3,2);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Customer>().HasKey(c => c.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().Property<Status>(o => o.Status).HasConversion(new EnumToNameConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // In-memory model of the join graph.
    private static readonly (int Id, int Cust, Status St)[] Orders =
        { (1, 1, Status.Open), (2, 1, Status.Paid), (3, 2, Status.Open), (4, 2, Status.Cancelled) };
    private static readonly (int Id, string Name)[] Customers = { (1, "Ann"), (2, "Bob") };
    private static readonly (int Id, int OrderId, int Qty)[] Lines =
        { (1, 1, 5), (2, 1, 3), (3, 2, 10), (4, 3, 7), (5, 3, 1), (6, 3, 2) };

    [Fact]
    public async Task Converter_navscalar_and_correlated_aggregate_in_one_projection()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Order>()
                .Where(o => o.Status != Status.Cancelled)   // converter column in WHERE
                .OrderBy(o => o.Id)
                .Select(o => new
                {
                    o.Id,
                    St = o.Status,                                  // converter column projection
                    Cust = o.Customer!.Name,                        // reference-nav scalar
                    LineSum = ctx.Query<Line>().Where(l => l.OrderId == o.Id).Sum(l => l.Qty),  // correlated aggregate
                    TopQty = ctx.Query<Line>().Where(l => l.OrderId == o.Id)
                                .OrderByDescending(l => l.Qty).Select(l => (int?)l.Qty).FirstOrDefault(), // correlated First
                })
                .ToListAsync());

            var oracle = Orders.Where(o => o.St != Status.Cancelled).OrderBy(o => o.Id)
                .Select(o => new
                {
                    o.Id,
                    St = o.St,
                    Cust = Customers.First(c => c.Id == o.Cust).Name,
                    LineSum = Lines.Where(l => l.OrderId == o.Id).Sum(l => l.Qty),
                    TopQty = Lines.Where(l => l.OrderId == o.Id).OrderByDescending(l => l.Qty)
                                .Select(l => (int?)l.Qty).FirstOrDefault(),
                })
                .ToList();

            Assert.Equal(oracle.Count, got.Count);
            for (int i = 0; i < oracle.Count; i++)
            {
                Assert.Equal(oracle[i].Id, got[i].Id);
                Assert.Equal(oracle[i].St, got[i].St);
                Assert.Equal(oracle[i].Cust, got[i].Cust);
                Assert.Equal(oracle[i].LineSum, got[i].LineSum);
                Assert.Equal(oracle[i].TopQty, got[i].TopQty);
            }
        }
        catch (Exception ex)
        {
            Assert.Fail($"THREW: {ex.GetType().Name}: {ex.Message}");
        }
    }
}
