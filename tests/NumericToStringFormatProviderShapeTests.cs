using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `numeric.ToString("F2")` across providers. The ETSV
/// and SCV used to emit `printf('%.Nf', col)` unconditionally -- printf
/// is SQLite-only. Other providers need their native fixed-decimal
/// formatting function: SQL Server FORMAT(...,'F2','en-US'); Postgres
/// to_char(...,'FM...'); MySQL REPLACE(FORMAT(...,2),',',''). Verifies
/// the generated SQL routes through the provider hook
/// FormatFixedDecimalSql so each provider produces working SQL.
/// </summary>
[Trait("Category", "Fast")]
public sealed class NumericToStringFormatProviderShapeTests : TestBase
{
    private sealed class NfItem
    {
        public int Id { get; set; }
        public decimal Price { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_numeric_ToString_F2_uses_provider_specific_format_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        Expression<Func<NfItem, bool>> expr = e => e.Price.ToString("F2") == "3.14";
        var (sql, _) = Translate<NfItem>(expr, connection, provider);

        // The provider hook is identity-free: each provider returns a working
        // fixed-decimal expression. Verify the formatted expression is what
        // the provider's hook produces (route through, not hardcoded printf).
        var col = $"{provider.Escape("T0")}.{provider.Escape("Price")}";
        var expectedFmtSql = provider.FormatFixedDecimalSql(col, 2);
        Assert.Contains(expectedFmtSql, sql);
        // SQLite uses printf; other providers should NOT contain printf.
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("printf(", sql);
    }
}
