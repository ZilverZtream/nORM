using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the deterministic-throw contract of the string comparison-mode and date-format
/// guards: a per-row (non-constant) StringComparison mode cannot pick a SQL shape at
/// translation time, an ignore-case Replace has no provider-mobile rewrite, and a
/// DateTime.ParseExact format outside the translatable set must fail loud. Each guard
/// is pinned NEXT TO its working sibling so the tests also prove the guards are
/// reachable boundaries of real features, not blanket rejections.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringComparisonModeGuardContractTests
{
    [Table("StrModeGuard_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string DateText { get; set; } = "";
    }

    private static DbContext NewCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE StrModeGuard_Test (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DateText TEXT NOT NULL);" +
                "INSERT INTO StrModeGuard_Test VALUES (1,'alpha','2024-03-15'),(2,'Beta','2019-01-02');";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Per_row_comparison_mode_in_compare_throws_deterministically()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // Constant mode works…
        var ok = ctx.Query<Row>()
            .Where(x => string.Compare(x.Name, "alpha", StringComparison.Ordinal) == 0)
            .Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ok);

        // …a per-row mode cannot choose the SQL shape at translation time.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Row>()
            .Where(x => string.Compare(x.Name, "m",
                x.Id % 2 == 0 ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase) > 0)
            .ToList());
        Assert.Contains("comparison mode must be a constant", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Per_row_comparison_mode_in_index_of_throws_deterministically()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var ok = ctx.Query<Row>()
            .Where(x => x.Name.IndexOf("a", StringComparison.Ordinal) == 0)
            .Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ok);

        var ex = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Row>()
            .Where(x => x.Name.IndexOf("a",
                x.Id % 2 == 0 ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase) == 0)
            .ToList());
        Assert.Contains("comparison mode must be a constant", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Per_row_comparison_mode_in_replace_throws_deterministically()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var ex = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Row>()
            .Where(x => x.Name.Replace("a", "b",
                x.Id % 2 == 0 ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase) == "blphb")
            .ToList());
        Assert.Contains("comparison mode must be a constant", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Ignore_case_replace_throws_while_case_sensitive_replace_works()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // Case-sensitive mode translates (native REPLACE is ordinal)…
        var ok = ctx.Query<Row>()
            .Where(x => x.Name.Replace("a", "o", StringComparison.Ordinal) == "olpho")
            .Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ok);

        // …ignore-case has no provider-mobile rewrite and must fail loud rather than
        // silently running a case-sensitive REPLACE.
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Row>()
            .Where(x => x.Name.Replace("A", "o", StringComparison.OrdinalIgnoreCase) == "olpho")
            .ToList());
    }

    [Fact]
    public void Parse_exact_outside_the_translatable_format_set_throws_deterministically()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // A supported format translates…
        var ok = ctx.Query<Row>()
            .Where(x => DateTime.ParseExact(x.DateText, "yyyy-MM-dd", CultureInfo.InvariantCulture)
                > new DateTime(2020, 1, 1))
            .Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ok);

        // …an arbitrary format cannot be re-implemented in SQL and must fail loud.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Row>()
            .Where(x => DateTime.ParseExact(x.DateText, "dd/MM/yyyy", CultureInfo.InvariantCulture)
                > new DateTime(2020, 1, 1))
            .ToList());
        Assert.Contains("not supported in SQL translation", ex.Message, StringComparison.Ordinal);
    }
}
