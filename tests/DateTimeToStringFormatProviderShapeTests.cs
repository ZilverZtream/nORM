using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `DateTime.ToString("yyyy-MM-dd")` across providers.
/// ETSV / SCV unconditionally emitted `strftime('%Y-%m-%d', col)` --
/// strftime is SQLite-only and fails on SqlServer / Postgres / MySQL.
/// Verifies the generated SQL routes through the provider hook
/// FormatDateUsingDotNetPattern so each provider produces working SQL.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeToStringFormatProviderShapeTests : TestBase
{
    private sealed class DtItem
    {
        public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTime_ToString_yyyyMMdd_uses_provider_specific_date_format(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        Expression<Func<DtItem, bool>> expr = e => e.Stamp.ToString("yyyy-MM-dd") == "2026-05-25";
        var (sql, _) = Translate<DtItem>(expr, connection, provider);

        var col = $"{provider.Escape("T0")}.{provider.Escape("Stamp")}";
        var expected = provider.FormatDateUsingDotNetPattern(col, "yyyy-MM-dd");
        Assert.NotNull(expected);
        Assert.Contains(expected!, sql);
        // SQLite uses strftime; others should NOT contain strftime.
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("strftime(", sql);
    }
}
