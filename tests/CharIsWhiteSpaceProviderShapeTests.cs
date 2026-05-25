using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; char.IsWhiteSpace(p.Code[0]))`. The
/// ASCII-whitespace check emits `c = CHAR(9) OR c = CHAR(10) OR
/// c = CHAR(13)`. SQLite / SQL Server / MySQL accept <c>CHAR(N)</c>;
/// PostgreSQL requires <c>chr(N)</c>. Routed through GetCharFromCodeSql
/// provider hook now.
/// </summary>
[Trait("Category", "Fast")]
public sealed class CharIsWhiteSpaceProviderShapeTests : TestBase
{
    private sealed class CcwsItem
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
    public void Where_with_char_IsWhiteSpace_uses_provider_specific_char_from_code(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        Expression<Func<CcwsItem, bool>> expr = e => char.IsWhiteSpace(e.Code[0]);
        var (sql, _) = Translate<CcwsItem>(expr, connection, provider);

        var expectedTab = provider.GetCharFromCodeSql("9");
        Assert.Contains(expectedTab, sql);
        if (providerKind == ProviderKind.Postgres)
            Assert.DoesNotContain("CHAR(9)", sql);
        else
            Assert.DoesNotContain("chr(9)", sql);
    }
}
