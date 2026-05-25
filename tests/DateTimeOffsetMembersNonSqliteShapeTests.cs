using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// `DateTimeOffset.UtcDateTime`, `DateTimeOffset.DateTime`, and
/// `DateTimeOffset.Offset` only translated on SQLite via its substr-based
/// text-offset parser. The other three providers fell through to the
/// generic member-resolution path and threw at translation time. With a
/// per-provider lowering using native DATETIMEOFFSET / TIMESTAMPTZ /
/// DATETIME primitives each translates to a portable scalar.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeOffsetMembersNonSqliteShapeTests : TestBase
{
    private sealed class DtoItem
    {
        public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }

    public static IEnumerable<object[]> ProvidersAndMembers()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(DateTimeOffset.UtcDateTime) };
            yield return new object[] { k, nameof(DateTimeOffset.DateTime) };
            yield return new object[] { k, nameof(DateTimeOffset.Offset) };
        }
    }

    [Theory]
    [MemberData(nameof(ProvidersAndMembers))]
    public void Where_with_DateTimeOffset_member_translates_per_provider(ProviderKind providerKind, string memberName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // For .UtcDateTime / .DateTime compare against a DateTime literal; for
        // .Offset (TimeSpan) compare against TimeSpan.Zero. Build the
        // expression dynamically per member.
        var param = Expression.Parameter(typeof(DtoItem), "e");
        var stamp = Expression.Property(param, nameof(DtoItem.Stamp));
        var member = Expression.Property(stamp, memberName);
        Expression rhs = memberName == nameof(DateTimeOffset.Offset)
            ? Expression.Constant(TimeSpan.Zero)
            : Expression.Constant(new DateTime(2026, 1, 1));
        var body = Expression.GreaterThan(member, rhs);
        var lambda = Expression.Lambda<Func<DtoItem, bool>>(body, param);

        var (sql, _) = Translate<DtoItem>(lambda, connection, provider);
        // Per-provider anchors -- the generic fallback would have thrown
        // before SQL emit, so a successful Translate is itself the pin;
        // these checks lock in the chosen primitive.
        switch (providerKind)
        {
            case ProviderKind.SqlServer when memberName == nameof(DateTimeOffset.UtcDateTime):
                Assert.Contains("SWITCHOFFSET", sql);
                break;
            case ProviderKind.SqlServer when memberName == nameof(DateTimeOffset.Offset):
                Assert.Contains("DATEPART", sql);
                break;
            case ProviderKind.Postgres when memberName == nameof(DateTimeOffset.UtcDateTime):
                Assert.Contains("AT TIME ZONE", sql);
                break;
        }
    }
}
