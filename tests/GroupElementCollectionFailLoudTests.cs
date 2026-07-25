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
/// LINQ parity / fail-loud: a GroupBy projection that materializes a group's ELEMENTS into a collection
/// (<c>g.ToList()</c> / <c>g.Select(x => x.Y).ToList()</c>) has no single-column SQL translation. Previously
/// the SELECT-column was silently dropped and the materializer crashed with a cryptic
/// ArgumentOutOfRangeException. It now fails loud with an actionable NormUnsupportedFeatureException, while the
/// supported per-group scalar aggregates keep working. (Full EF-style group-element materialization via a split
/// query is a tracked follow-up.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupElementCollectionFailLoudTests
{
    [Table("GecOrder")]
    public sealed class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int Amount { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GecOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL);" +
                              "INSERT INTO GecOrder VALUES (1,1,10),(2,1,20),(3,2,30);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Group_elements_to_list_fails_loud_with_actionable_message()
    {
        using var ctx = NewCtx();
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().GroupBy(o => o.CustomerId)
               .Select(g => new { g.Key, Items = g.Select(x => x.Amount).ToList() })
               .ToList());
        Assert.Contains("collection", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GroupBy projection member", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Group_to_list_of_whole_rows_fails_loud()
    {
        using var ctx = NewCtx();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().GroupBy(o => o.CustomerId)
               .Select(g => new { g.Key, Items = g.ToList() })
               .ToList());
    }

    [Fact]
    public void Supported_scalar_aggregates_still_work()
    {
        using var ctx = NewCtx();
        var byCustomer = ctx.Query<Order>().GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key, Count = g.Count(), Total = g.Sum(x => x.Amount) })
            .OrderBy(x => x.Key).ToList();
        Assert.Equal(2, byCustomer.Count);
        Assert.Equal(2, byCustomer[0].Count);
        Assert.Equal(30, byCustomer[0].Total);
        Assert.Equal(30, byCustomer[1].Total);
    }
}
