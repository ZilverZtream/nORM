using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// A value converter whose provider type is a numeric type (decimal/double/float) must round-trip
/// exactly under any thread culture. On SQLite a decimal/double column has TEXT affinity, so the raw
/// reader value is the invariant-formatted string "1.5"; the converter's read coercion parsed it with
/// Convert.ChangeType WITHOUT a format provider (current culture), turning "1.5" into 15 on a
/// group-separator-is-dot locale (de-DE) — silent data corruption — or throwing on a comma-decimal
/// locale (sv-SE). Coercion must use the invariant culture, like every other read path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ValueConverterCultureSafetyTests
{
    private readonly struct Money
    {
        public Money(decimal amount) => Amount = amount;
        public decimal Amount { get; }
    }

    private sealed class MoneyConverter : ValueConverter<Money, decimal>
    {
        public override object ConvertToProvider(Money value) => value.Amount;
        public override object ConvertFromProvider(decimal value) => new Money(value);
    }

    [Table("VccOrder")]
    private class VccOrder
    {
        [Key] public int Id { get; set; }
        public Money Price { get; set; }
    }

    private static IDisposable ForceCulture(string name)
    {
        var prev = (CultureInfo.CurrentCulture, CultureInfo.CurrentUICulture);
        var c = new CultureInfo(name);
        CultureInfo.CurrentCulture = c;
        CultureInfo.CurrentUICulture = c;
        return new Restore(prev);
    }

    private sealed class Restore : IDisposable
    {
        private readonly (CultureInfo, CultureInfo) _prev;
        public Restore((CultureInfo, CultureInfo) prev) => _prev = prev;
        public void Dispose()
        {
            CultureInfo.CurrentCulture = _prev.Item1;
            CultureInfo.CurrentUICulture = _prev.Item2;
        }
    }

    [Theory]
    [InlineData("de-DE")]  // group separator '.', decimal ',' — parses "1.5" as 15 under current culture
    [InlineData("sv-SE")]  // comma decimal — throws under current culture
    [InlineData("en-US")]  // baseline
    public async Task Decimal_provider_converter_round_trips_under_any_culture(string culture)
    {
        using var _culture = ForceCulture(culture);

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Price column has TEXT affinity so the raw reader value is the string "1.5" / "1234.5".
            cmd.CommandText = "CREATE TABLE VccOrder (Id INTEGER PRIMARY KEY, Price TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using var _cn = cn;
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<VccOrder>().Property(o => o.Price).HasConversion(new MoneyConverter())
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        ctx.Add(new VccOrder { Id = 1, Price = new Money(1.5m) });
        ctx.Add(new VccOrder { Id = 2, Price = new Money(1234.5m) });
        await ctx.SaveChangesAsync();

        var rows = (await ctx.Query<VccOrder>().OrderBy(o => o.Id).ToListAsync()).ToArray();

        Assert.Equal(1.5m, rows[0].Price.Amount);
        Assert.Equal(1234.5m, rows[1].Price.Amount);
    }
}
