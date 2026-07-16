using System;
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
/// Contract for ordering on value-converter columns (Query/LINQ matrix cell: OrderBy x converted operand).
///
/// A translated <c>OrderBy</c> on a value-converter property orders by the STORED PROVIDER value, because
/// ORDER BY is emitted against the stored column - identical to EF Core. Ordering by the CLR key would
/// require applying the inverse converter inside SQL, which is impossible for an arbitrary C#
/// <c>ConvertFromProvider</c>. Consequences: an ORDER-PRESERVING converter (e.g. a monotonic offset)
/// orders identically to the CLR key; an ORDER-CHANGING converter does NOT. Notably, an enum-to-string
/// converter orders ALPHABETICALLY by the stored name, not by the enum's underlying value, and a negating
/// converter reverses the order. This test pins that provider-value ordering contract so it can't
/// silently change. For CLR-key ordering, order by an order-preserving stored representation. See
/// docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ConverterColumnOrderingContractTests
{
    private enum Prio { Low = 0, Medium = 1, High = 2 }

    [Table("CvOrdContract")]
    private sealed class CvRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public int Offset { get; set; }
        public Prio Prio { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }
    private sealed class PlusConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => v - 1000;
    }
    private sealed class PrioToNameConverter : ValueConverter<Prio, string>
    {
        public override object? ConvertToProvider(Prio v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Prio>(v);
    }

    private static readonly (int Id, int Score, int Offset, Prio Prio)[] Rows =
    {
        (1, 10, 10, Prio.High),
        (2, -5, -5, Prio.Low),
        (3, 20, 20, Prio.Medium),
        (4, 0, 0, Prio.Low),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE CvOrdContract (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Offset INTEGER NOT NULL, Prio TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CvRow>().Property<int>(p => p.Score).HasConversion(new NegatingConverter());
                mb.Entity<CvRow>().Property<int>(p => p.Offset).HasConversion(new PlusConverter());
                mb.Entity<CvRow>().Property<Prio>(p => p.Prio).HasConversion(new PrioToNameConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in Rows) await ctx.InsertAsync(new CvRow { Id = r.Id, Score = r.Score, Offset = r.Offset, Prio = r.Prio });
        return ctx;
    }

    [Fact]
    public async Task OrderBy_negating_converter_orders_by_stored_value_not_clr_key()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<CvRow>)ctx.Query<CvRow>())
            .AsNoTracking().OrderBy(e => e.Score).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        // Stored order is by -Score ascending: -20(3), -10(1), 0(4), 5(2).
        Assert.Equal(new[] { 3, 1, 4, 2 }, norm);
        // NOT the CLR-key order (-5, 0, 10, 20 -> 2,4,1,3).
        Assert.NotEqual(Rows.OrderBy(r => r.Score).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);

        // The converter still round-trips the value on read.
        var scores = ((INormQueryable<CvRow>)ctx.Query<CvRow>())
            .AsNoTracking().OrderBy(e => e.Id).Select(e => e.Score).ToList();
        Assert.Equal(new[] { 10, -5, 20, 0 }, scores);
    }

    [Fact]
    public async Task OrderBy_order_preserving_converter_matches_clr_key()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<CvRow>)ctx.Query<CvRow>())
            .AsNoTracking().OrderBy(e => e.Offset).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        // +1000 is monotonic, so stored order == CLR order: -5, 0, 10, 20 -> 2,4,1,3.
        Assert.Equal(new[] { 2, 4, 1, 3 }, norm);
        Assert.Equal(Rows.OrderBy(r => r.Offset).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
    }

    [Fact]
    public async Task OrderBy_enum_to_string_converter_orders_alphabetically_not_by_enum_value()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<CvRow>)ctx.Query<CvRow>())
            .AsNoTracking().OrderBy(e => e.Prio).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        // Stored names sort alphabetically: "High"(1), "Low"(2), "Low"(4), "Medium"(3).
        Assert.Equal(new[] { 1, 2, 4, 3 }, norm);
        // NOT the enum-value order (Low, Low, Medium, High -> 2,4,3,1).
        Assert.NotEqual(Rows.OrderBy(r => r.Prio).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
    }
}
