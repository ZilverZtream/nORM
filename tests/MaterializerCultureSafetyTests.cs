using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Materialization must convert database values using invariant-culture rules.
/// The database stores numeric/temporal scalars with invariant formatting, so a
/// value read as a string (SQLite TEXT affinity) must never be reparsed under a
/// comma-decimal locale — Convert.ChangeType(object, Type) uses CurrentCulture
/// and would turn "1.5" into 15 (or throw) on Swedish/German machines. These
/// pins run the pipeline under de-DE.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class MaterializerCultureSafetyTests
{
    [Table("McsItem")]
    private class McsItem
    {
        [Key] public int Id { get; set; }
        public double Ratio { get; set; }
        public decimal Amount { get; set; }
        public long Big { get; set; }
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
    [InlineData("de-DE")]  // comma decimal separator
    [InlineData("sv-SE")]  // comma decimal separator
    [InlineData("en-US")]  // baseline
    public async Task Numeric_values_materialize_exactly_under_any_culture(string culture)
    {
        using var _culture = ForceCulture(culture);

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Store the numeric columns as TEXT so materialization goes through the
            // string→numeric conversion path where culture matters.
            cmd.CommandText =
                "CREATE TABLE McsItem (Id INTEGER PRIMARY KEY, Ratio TEXT NOT NULL, Amount TEXT NOT NULL, Big TEXT NOT NULL);" +
                "INSERT INTO McsItem VALUES (1, '1.5', '19.99', '9000000000'), (2, '0.001', '1234.5678', '-42');";
            cmd.ExecuteNonQuery();
        }
        using var _cn = cn;
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<McsItem>() };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var rows = (await ctx.Query<McsItem>().OrderBy(x => x.Id).ToListAsync()).ToArray();

        Assert.Equal(2, rows.Length);
        Assert.Equal(1.5, rows[0].Ratio);
        Assert.Equal(19.99m, rows[0].Amount);
        Assert.Equal(9000000000L, rows[0].Big);
        Assert.Equal(0.001, rows[1].Ratio);
        Assert.Equal(1234.5678m, rows[1].Amount);
        Assert.Equal(-42L, rows[1].Big);
    }
}
