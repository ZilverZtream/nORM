using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; char.IsPunctuation(p.Code[0]))` and the
/// related char.IsSymbol / IsControl / GetNumericValue checks. ETSV
/// emitted `unicode(...)` unconditionally to convert char to code point.
/// SQLite has unicode(); SQL Server uses UNICODE(); Postgres uses ascii();
/// MySQL uses ORD(). Routes through provider hook GetCharCodeSql now.
/// </summary>
[Trait("Category", "Fast")]
public sealed class CharIsPunctuationProviderShapeTests : TestBase
{
    private sealed class CcItem
    {
        public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_char_IsPunctuation_uses_provider_specific_codepoint_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        Expression<Func<CcItem, bool>> expr = e => char.IsPunctuation(e.Code[0]);
        var (sql, _) = Translate<CcItem>(expr, connection, provider);

        // Provider hook should fire instead of literal "unicode(".
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("unicode(", sql);
        // Each provider's char-code function should appear at least once.
        var fnPrefix = providerKind switch
        {
            ProviderKind.Sqlite => "unicode(",
            ProviderKind.SqlServer => "UNICODE(",
            ProviderKind.Postgres => "ascii(",
            ProviderKind.MySql => "ORD(",
            _ => throw new NotImplementedException(),
        };
        Assert.Contains(fnPrefix, sql);
    }
}
