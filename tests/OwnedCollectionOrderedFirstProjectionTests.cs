using System;
using System.Collections.Generic;
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
/// Projecting an ordered First/Last (and Min/Max) over an OWNED collection (OwnsMany, stored in its own
/// child table) — <c>o.Lines.OrderBy(k).Select(l =&gt; l.Col).First/Last()</c>. Owned collections live in
/// TableMapping.OwnedCollections and are served by the owned projection pipeline; this pins that a scalar
/// ordered-First projection works (the many-to-many analogue crashed until it got its own emit). A
/// value-converter selector column materializes through ConvertFromProvider (a +1000 offset converter makes
/// an unconverted result off-by-1000); an empty collection yields the scalar default.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class OwnedCollectionOrderedFirstProjectionTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("OcofOrder")]
    public sealed class Order
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public sealed class Line
    {
        public int Id { get; set; }
        public int OrderId { get; set; }
        public int Qty { get; set; } // converter (+1000)
    }

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE OcofOrder (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE OcofLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Qty INTEGER NOT NULL);" +
                // Qty stores the PROVIDER value (model + 1000). order1 {10,20}, order2 {30}, order3 {}.
                "INSERT INTO OcofOrder VALUES (1),(2),(3);" +
                "INSERT INTO OcofLine VALUES (1,1,1010),(2,1,1020),(3,2,1030);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "OcofLine", foreignKey: "OrderId",
                    buildAction: b => { b.HasKey(l => l.Id); b.Property(l => l.Qty).HasConversion(new OffsetConverter()); });
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void First_selecting_converter_column_over_owned_applies_conversion()
    {
        using var ctx = Ctx();
        var got = ctx.Query<Order>().OrderBy(o => o.Id)
            .Select(o => o.Lines.OrderBy(l => l.Id).Select(l => l.Qty).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 10, 30, 0 }, got); // model values (not stored 1010/1030), empty -> 0
    }

    [Fact]
    public void First_selecting_nonconverter_column_over_owned()
    {
        using var ctx = Ctx();
        var got = ctx.Query<Order>().OrderBy(o => o.Id)
            .Select(o => o.Lines.OrderBy(l => l.Id).Select(l => l.Id).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 1, 3, 0 }, got);
    }

    [Fact]
    public void Last_over_owned_reverses_ordering()
    {
        using var ctx = Ctx();
        var got = ctx.Query<Order>().OrderBy(o => o.Id)
            .Select(o => o.Lines.OrderBy(l => l.Id).Select(l => l.Qty).LastOrDefault()).ToList();
        Assert.Equal(new[] { 20, 30, 0 }, got);
    }

    [Fact]
    public void Min_max_count_over_owned_converter_column()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 20, 30, 0 }, ctx.Query<Order>().OrderBy(o => o.Id).Select(o => o.Lines.Max(l => l.Qty)).ToList());
        Assert.Equal(new[] { 10, 30, 0 }, ctx.Query<Order>().OrderBy(o => o.Id).Select(o => o.Lines.Min(l => l.Qty)).ToList());
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Order>().OrderBy(o => o.Id).Select(o => o.Lines.Count()).ToList());
    }
}
