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
/// A property mapped through a value converter to a DECIMAL provider type (the mainstream Money/Quantity
/// value-object pattern) is stored on SQLite as TEXT. ORDER BY / relational WHERE / projected comparisons
/// over it must use numeric semantics (the NORM_DECIMAL collation / CAST), exactly as a bare decimal column
/// does — but the normalization decision gated on the CLR MODEL type (Money), not the converter's provider
/// type (decimal), so those comparisons ran as raw lexicographic TEXT operations, silently returning inverted
/// row sets.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ConverterDecimalComparisonTests
{
    public readonly record struct Money(decimal Amount) : IComparable<Money>
    {
        public int CompareTo(Money other) => Amount.CompareTo(other.Amount);
        public static bool operator >(Money a, Money b) => a.Amount > b.Amount;
        public static bool operator <(Money a, Money b) => a.Amount < b.Amount;
        public static bool operator >=(Money a, Money b) => a.Amount >= b.Amount;
        public static bool operator <=(Money a, Money b) => a.Amount <= b.Amount;
    }

    private sealed class MoneyConverter : ValueConverter<Money, decimal>
    {
        public override object ConvertToProvider(Money value) => value.Amount;
        public override object ConvertFromProvider(decimal value) => new Money(value);
    }

    [Table("CdcEnt")]
    private class Ent
    {
        [Key] public int Id { get; set; }
        public Money Cost { get; set; }
    }

    private static readonly Money _threshold = new(50m);

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Store the decimals as TEXT exactly as nORM's decimal binding produces them.
            cmd.CommandText =
                "CREATE TABLE CdcEnt (Id INTEGER PRIMARY KEY, Cost TEXT NOT NULL);" +
                "INSERT INTO CdcEnt VALUES (1,'100.0'),(2,'9.0'),(3,'429.0'),(4,'24.5'),(5,'9.0');";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Ent>().Property(e => e.Cost).HasConversion(new MoneyConverter())
        });
        return (cn, ctx);
    }

    [Fact]
    public void OrderBy_converter_decimal_is_numeric_not_lexicographic()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var ids = ctx.Query<Ent>().OrderBy(x => x.Cost).ThenBy(x => x.Id).Select(x => x.Id).ToList();

        // Numeric order of {100,9,429,24.5,9}: 9(2),9(5),24.5(4),100(1),429(3). Lexicographic gives [1,4,3,2,5].
        Assert.Equal(new[] { 2, 5, 4, 1, 3 }, ids.ToArray());
    }

    [Fact]
    public void Where_converter_decimal_relational_is_numeric_not_lexicographic()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var ids = ctx.Query<Ent>().Where(x => x.Cost > _threshold).Select(x => x.Id).OrderBy(i => i).ToList();

        // Cost > 50 -> 100(1) and 429(3). Lexicographic '9.0' > '50.0' would wrongly return [2,5].
        Assert.Equal(new[] { 1, 3 }, ids.ToArray());
    }

    [Fact]
    public void Projected_converter_decimal_comparison_is_numeric_not_lexicographic()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var flags = ctx.Query<Ent>().OrderBy(x => x.Id).Select(x => x.Cost > _threshold).ToList();

        Assert.Equal(new[] { true, false, true, false, false }, flags.ToArray());
    }

    [Fact]
    public void FastPath_bare_orderby_converter_decimal_is_numeric()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Bare OrderBy with no Select projection routes through the fast path.
        var ids = ctx.Query<Ent>().OrderBy(x => x.Cost).ToList().Select(e => e.Id).ToList();

        // Numeric order (ties 9(2),9(5) keep insertion order); a lexicographic fast path gives [1,4,3,2,5].
        Assert.Equal(2, ids[0]);
        Assert.Equal(3, ids[4]);   // 429 is the largest, must sort last
    }

    [Fact]
    public void FastPath_bare_where_converter_decimal_is_numeric()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var ids = ctx.Query<Ent>().Where(x => x.Cost > _threshold).ToList()
            .Select(e => e.Id).OrderBy(i => i).ToList();

        Assert.Equal(new[] { 1, 3 }, ids.ToArray());
    }
}
