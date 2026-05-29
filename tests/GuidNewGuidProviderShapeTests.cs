using System;
using System.Linq.Expressions;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Guid.NewGuid inside a query is a nORM-owned server-generation translation,
/// not an unsupported CLR-only call.
/// </summary>
[Trait("Category", "Fast")]
public sealed class GuidNewGuidProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public Guid ExternalId { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite, "randomblob")]
    [InlineData(ProviderKind.SqlServer, "NEWID()")]
    [InlineData(ProviderKind.Postgres, "gen_random_uuid()")]
    [InlineData(ProviderKind.MySql, "UUID()")]
    public void Where_with_Guid_NewGuid_emits_provider_guid_primitive(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Guid).GetMethod(nameof(Guid.NewGuid), Type.EmptyTypes)!;
        var param = Expression.Parameter(typeof(Row), "r");
        var externalId = Expression.Property(param, nameof(Row.ExternalId));
        var call = Expression.Call(method);
        var body = Expression.NotEqual(externalId, call);
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_Guid_Empty_emits_zero_guid_literal(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var externalId = Expression.Property(param, nameof(Row.ExternalId));
        var empty = Expression.MakeMemberAccess(null, typeof(Guid).GetField(nameof(Guid.Empty))!);
        var body = Expression.Equal(externalId, empty);
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, parameters) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("00000000-0000-0000-0000-000000000000", sql, StringComparison.Ordinal);
        Assert.Empty(parameters);
    }
}
