using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins enum comparisons in the navigation-filter grammar (filtered Include, shaped-collection filters,
/// global filters): <c>l.Kind == Kind.B</c> now translates for an int-stored enum column (the compiler's
/// enum→int Convert is peeled and the constant folds to its underlying value). An enum column with a VALUE
/// CONVERTER stays fail-loud rather than comparing the stored provider value against the raw integer — that
/// would be silently wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationFilterEnumComparisonContractTests
{
    public enum Kind { A = 1, B = 2, C = 3 }

    [Table("NfeOrder")]
    public class Order { [Key] public int Id { get; set; } public List<Line> Lines { get; set; } = new(); }

    [Table("NfeLine")]
    public class Line { [Key] public int Id { get; set; } public int OrderId { get; set; } public Kind Kind { get; set; } }

    private sealed class KindToNameConverter : ValueConverter<Kind, string>
    {
        public override object? ConvertToProvider(Kind value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Kind>(value);
    }

    private static DbContext Bootstrap(SqliteConnection cn, bool kindConverter = false)
    {
        using (var cmd = cn.CreateCommand())
        {
            // With the converter, Kind is stored as its NAME (A/B/C); without it, as the underlying int.
            var lineTable = kindConverter
                ? "CREATE TABLE NfeLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Kind TEXT NOT NULL); INSERT INTO NfeLine VALUES (1,1,'A'),(2,1,'B'),(3,1,'C');"
                : "CREATE TABLE NfeLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Kind INTEGER NOT NULL); INSERT INTO NfeLine VALUES (1,1,1),(2,1,2),(3,1,3);";
            cmd.CommandText = "CREATE TABLE NfeOrder (Id INTEGER PRIMARY KEY); INSERT INTO NfeOrder VALUES (1); " + lineTable;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            if (kindConverter)
                mb.Entity<Line>().Property<Kind>(l => l.Kind).HasConversion(new KindToNameConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static int[] Load(DbContext ctx, Func<INormQueryable<Order>, IQueryable<Order>> shape)
        => shape((INormQueryable<Order>)ctx.Query<Order>()).ToList().Single()
            .Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray();

    [Fact]
    public void Enum_equality_filters_int_stored_enum()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 2 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Kind == Kind.B))));
    }

    [Fact]
    public void Enum_inequality_filters()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 1, 3 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Kind != Kind.B))));
    }

    [Fact]
    public void Enum_comparison_composes_with_conjunction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 3 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Kind != Kind.B && l.Id >= 3))));
    }

    [Fact]
    public void Enum_comparison_with_a_captured_value_binds_and_rebinds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        int[] RunFor(Kind k) => Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Kind == k)));

        // A captured enum binds through the compiled-parameter path; the same query shape with a different
        // captured value must re-bind (not replay the first run's) across the plan-cache hit.
        Assert.Equal(new[] { 2 }, RunFor(Kind.B));
        Assert.Equal(new[] { 3 }, RunFor(Kind.C));
    }

    [Fact]
    public void Converter_backed_enum_column_matches_the_converted_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn, kindConverter: true);
        // Kind is stored as its NAME via the converter; the constant Kind.B is converted to "B" so the
        // comparison matches the stored value (previously this was fail-loud).
        Assert.Equal(new[] { 2 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Kind == Kind.B))));
    }

    [Fact]
    public void Converter_backed_enum_column_binds_a_captured_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn, kindConverter: true);
        var k = Kind.C;
        Assert.Equal(new[] { 3 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Kind == k))));
    }
}
