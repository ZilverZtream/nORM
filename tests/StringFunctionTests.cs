using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>Verify LIKE helpers emit provider-specific concat.</summary>
public class StringFunctionTests : TestBase
{
    private class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        // Second string column — used as argument to force the "variable path" in LIKE helpers
        public string Tag { get; set; } = string.Empty;
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
            yield return new object[] { provider };
    }

    // Use a column reference as the argument to force the "variable path" in LIKE helpers
    // (Captured closure variables are extracted as constants and take the literal-pattern path)

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Contains_ColumnArg_EmitsProviderConcat(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Name.Contains(p.Tag)),
            connection, provider);

        if (provider is SqliteProvider)
        {
            Assert.Contains("||", sql);
            Assert.DoesNotContain("CONCAT(", sql);
        }
        else
        {
            Assert.Contains("CONCAT(", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void StartsWith_ColumnArg_EmitsProviderConcat(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Name.StartsWith(p.Tag)),
            connection, provider);

        if (provider is SqliteProvider)
        {
            Assert.Contains("||", sql);
            Assert.DoesNotContain("CONCAT(", sql);
        }
        else
        {
            Assert.Contains("CONCAT(", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void EndsWith_ColumnArg_EmitsProviderConcat(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Name.EndsWith(p.Tag)),
            connection, provider);

        if (provider is SqliteProvider)
        {
            Assert.Contains("||", sql);
            Assert.DoesNotContain("CONCAT(", sql);
        }
        else
        {
            Assert.Contains("CONCAT(", sql);
        }
    }

    [Fact]
    public void SqliteProvider_GetConcatSql_UsesPipeOperator()
    {
        var provider = new SqliteProvider();
        var result = provider.GetConcatSql("'%'", "@p0");
        Assert.Equal("('%' || @p0)", result);
    }

    [Fact]
    public void SqlServerProvider_GetConcatSql_UsesConcatFunction()
    {
        var provider = new SqlServerProvider();
        var result = provider.GetConcatSql("'%'", "@p0");
        Assert.Equal("CONCAT('%', @p0)", result);
    }
}
