using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// LINQ parity / fail-loud: a projection that pulls a whole related ROW/entity via a correlated
/// First/FirstOrDefault/Single/Last (returning more than one column) has no single-SQL-value translation.
/// Previously the generic method fall-through blindly uppercased the method into a SQL function, emitting
/// invalid SQL like FIRSTORDEFAULT(...) that failed cryptically at the database. It now fails loud with an
/// actionable NormUnsupportedFeatureException, while the SUPPORTED single-scalar correlated form still works.
/// (Full EF-style OUTER APPLY / lateral row projection is a tracked follow-up.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CorrelatedFirstRowProjectionFailLoudTests
{
    [Table("CfCustomer")]
    public sealed class Customer { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("CfOrder")]
    public sealed class Order { [Key] public int Id { get; set; } public int CustomerId { get; set; } public int Amount { get; set; } public int OrderDate { get; set; } }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CfCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE CfOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL, OrderDate INTEGER NOT NULL);" +
                "INSERT INTO CfCustomer VALUES (1,'alice');" +
                "INSERT INTO CfOrder VALUES (1,1,10,1),(2,1,20,2);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Correlated_first_returning_whole_entity_fails_loud()
    {
        using var ctx = NewCtx();
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Customer>().Select(c => new
            {
                c.Name,
                Last = ctx.Query<Order>().Where(o => o.CustomerId == c.Id).OrderByDescending(o => o.OrderDate).FirstOrDefault()
            }).ToList());
        Assert.Contains("FirstOrDefault", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Correlated_first_of_a_scalar_column_still_works()
    {
        using var ctx = NewCtx();
        // Supported single-scalar correlated form — must keep working (returns the latest order's amount).
        var rows = ctx.Query<Customer>().Select(c => new
        {
            c.Name,
            LastAmount = ctx.Query<Order>().Where(o => o.CustomerId == c.Id).OrderByDescending(o => o.OrderDate).Select(o => o.Amount).FirstOrDefault()
        }).ToList();
        Assert.Single(rows);
        Assert.Equal(20, rows[0].LastAmount);
    }
}
