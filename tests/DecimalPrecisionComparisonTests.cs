using System;
using System.Collections.Generic;
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
/// SQLite stores decimal as TEXT; comparisons coerce through CAST(... AS REAL), whose
/// IEEE-754 double precision tops out around 15-17 significant digits. Two distinct
/// decimals that collapse to the same double must still compare, filter, group, and
/// deduplicate as DIFFERENT values — C# decimal keeps 28-29 significant digits.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DecimalPrecisionComparisonTests
{
    [Table("DecPrec_Account")]
    private class Account
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public decimal Balance { get; set; }
    }

    // 19 significant digits; both collapse to the same double.
    private const string HiA = "1234567890123456.78";
    private const string HiB = "1234567890123456.79";
    private static readonly decimal HiADec = decimal.Parse(HiA, System.Globalization.CultureInfo.InvariantCulture);
    private static readonly decimal HiBDec = decimal.Parse(HiB, System.Globalization.CultureInfo.InvariantCulture);

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DecPrec_Account (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Balance TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Account { Balance = HiADec });
        ctx.Add(new Account { Balance = HiBDec });
        ctx.Add(new Account { Balance = 42.50m });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return (cn, ctx);
    }

    [Fact]
    public void Equality_distinguishes_beyond_double_precision()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Account>().Where(a => a.Balance == HiADec).Select(a => a.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Inequality_distinguishes_beyond_double_precision()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Account>().Where(a => a.Balance != HiADec).Select(a => a.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void In_list_distinguishes_beyond_double_precision()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var wanted = new[] { HiBDec };
        var ids = ctx.Query<Account>().Where(a => wanted.Contains(a.Balance)).Select(a => a.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void Distinct_keeps_values_that_differ_beyond_double_precision()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var distinct = ctx.Query<Account>().Select(a => a.Balance).Distinct().ToList();
        Assert.Equal(3, distinct.Count);
    }

    [Fact]
    public void GroupBy_keeps_groups_that_differ_beyond_double_precision()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var groups = ctx.Query<Account>()
            .GroupBy(a => a.Balance)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToList();
        Assert.Equal(3, groups.Count);
        Assert.All(groups, g => Assert.Equal(1, g.Count));
    }

    [Fact]
    public void OrderBy_sorts_values_that_differ_beyond_double_precision()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ordered = ctx.Query<Account>().OrderBy(a => a.Balance).Select(a => a.Id).ToList();
        Assert.Equal(new[] { 3, 1, 2 }, ordered);
    }

    [Fact]
    public void Comparison_operators_stay_correct_for_ordinary_magnitudes()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Account>().Where(a => a.Balance > 100m).Select(a => a.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2 }, ids);
    }
}
