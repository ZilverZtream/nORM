using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class DistinctByProviderShapeTests : TestBase
{
    private sealed class Row
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void DistinctBy_emits_server_side_row_number_partition(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Row, Row>(
            q => q.OrderBy(r => r.Id).DistinctBy(r => r.Category),
            connection,
            provider);

        Assert.Contains("ROW_NUMBER() OVER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite, "CAST")]
    [InlineData(ProviderKind.SqlServer, "Amount")]
    [InlineData(ProviderKind.Postgres, "Amount")]
    [InlineData(ProviderKind.MySql, "Amount")]
    public void DistinctBy_decimal_key_uses_provider_numeric_partition(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Row, Row>(
            q => q.OrderBy(r => r.Id).DistinctBy(r => r.Amount),
            connection,
            provider);

        Assert.Contains("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedFragment, sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void ExceptBy_and_IntersectBy_rewrite_to_server_filter_plus_row_number(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var keys = new[] { "A" };

        var (exceptSql, _, _) = TranslateQuery<Row, Row>(
            q => q.OrderBy(r => r.Id).ExceptBy(keys, r => r.Category),
            connection,
            provider);
        var (intersectSql, _, _) = TranslateQuery<Row, Row>(
            q => q.OrderBy(r => r.Id).IntersectBy(keys, r => r.Category),
            connection,
            provider);

        Assert.Contains("ROW_NUMBER() OVER", exceptSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NOT", exceptSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ROW_NUMBER() OVER", intersectSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(" IN ", intersectSql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void UnionBy_with_local_rows_uses_parameterized_derived_table_and_row_number(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var other = new[]
        {
            new Row { Id = 98, Category = "A", Amount = 1m },
            new Row { Id = 99, Category = "X", Amount = 2m },
        };

        var (sql, parameters, _) = TranslateQuery<Row, Row>(
            q => q.OrderBy(r => r.Id).UnionBy(other, r => r.Category),
            connection,
            provider);

        Assert.Contains("UNION ALL SELECT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ROW_NUMBER() OVER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PARTITION BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.NotEmpty(parameters);
    }
}
