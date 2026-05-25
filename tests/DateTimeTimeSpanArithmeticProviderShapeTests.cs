using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; p.Stamp + TimeSpan.FromHours(1) &gt; threshold)`
/// and similar DateTime+TimeSpan arithmetic across providers. ETSV
/// unconditionally emitted `RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f',
/// col, '+N seconds'), '0'), '.')`. strftime() and its modifier-string
/// form are SQLite-only -- SQL Server uses DATEADD, Postgres uses
/// INTERVAL arithmetic, MySQL uses DATE_ADD. Sister to e6c55b9 /
/// d94b3b1 / 877ac03 -- same provider-hook pattern.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeTimeSpanArithmeticProviderShapeTests : TestBase
{
    private sealed class DtaItem
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
    public void Where_with_DateTime_plus_constant_TimeSpan_uses_provider_specific_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateTime(2026, 5, 25, 12, 0, 0);
        // Closure-captured TimeSpan local so node.Right is a MemberExpression
        // that TryGetConstantValue can fold (TimeSpan.FromHours is a MethodCall
        // which RCE-protected extraction rejects).
        var span = TimeSpan.FromHours(1);
        Expression<Func<DtaItem, bool>> expr = e => e.Stamp + span > threshold;
        var (sql, _) = Translate<DtaItem>(expr, connection, provider);

        var col = $"{provider.Escape("T0")}.{provider.Escape("Stamp")}";
        var expected = provider.AddSecondsToDateTimeSql(col, "3600");
        Assert.NotNull(expected);
        Assert.Contains(expected!, sql);
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("strftime(", sql);
    }
}
