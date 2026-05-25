using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; p.Stamp + p.Duration &gt; threshold)` where
/// Duration is a TimeSpan column. The constant-TimeSpan path (c56cb4d)
/// now works on all providers, but the COLUMN-TimeSpan path uses
/// SQLite-specific `substr()` to parse 'HH:mm:ss' text from the column.
/// On non-SQLite providers the TimeSpan column may be a native TIME /
/// INTERVAL type that doesn't need text parsing -- the emit needs a
/// different shape per provider. Pin the current state so we know when
/// this path needs cross-provider work.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimePlusTimeSpanColumnProviderShapeTests : TestBase
{
    private sealed class DtcItem
    {
        public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Duration { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTime_plus_TimeSpan_column_emits_provider_acceptable_sql(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateTime(2026, 5, 25, 12, 0, 0);
        Expression<Func<DtcItem, bool>> expr = e => e.Stamp + e.Duration > threshold;
        var (sql, _) = Translate<DtcItem>(expr, connection, provider);

        // On non-SQLite providers, the SQL should NOT contain SQLite-specific
        // substr() that would fail at execution. (SQL Server has SUBSTRING,
        // not substr; Postgres and MySQL have both but the 'HH:mm:ss' parsing
        // assumption itself breaks because TimeSpan columns are native types.)
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("substr(", sql);
    }
}
