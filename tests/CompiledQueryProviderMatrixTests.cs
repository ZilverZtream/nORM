using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>Translation-only cross-provider tests for compiled queries and SQL generation.</summary>
public class CompiledQueryProviderMatrixTests : TestBase
{
    private class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void NoDuplicateParameterNames_InGeneratedSql(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, parameters, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Id == 1 && p.Price > 10m),
            connection, provider);

        // No duplicate parameter names
        var paramKeys = parameters.Keys.ToList();
        Assert.Equal(paramKeys.Count, paramKeys.Distinct().Count());
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void NullComparison_EmitsIsNull_NotEqualsParam(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, parameters, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Name == null),
            connection, provider);

        Assert.Contains("IS NULL", sql);
        Assert.DoesNotContain("= @", sql);
        Assert.Empty(parameters);
    }

    [Fact]
    public void SqliteProvider_UsesDoubleQuoteEscaping()
    {
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Id == 1),
            connection, provider);

        Assert.Contains("\"", sql); // SQLite uses double-quotes
    }

    [Fact]
    public void SqlServerProvider_UsesBracketEscaping()
    {
        var setup = CreateProvider(ProviderKind.SqlServer);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Id == 1),
            connection, provider);

        Assert.Contains("[", sql); // SQL Server uses brackets
        Assert.Contains("]", sql);
    }

    [Fact]
    public void MySqlProvider_UsesBacktickEscaping()
    {
        var setup = CreateProvider(ProviderKind.MySql);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Id == 1),
            connection, provider);

        Assert.Contains("`", sql); // MySQL uses backticks
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Contains_RuntimeVar_NoDuplicateParamInSql(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        string search = "widget";
        var (sql, parameters, _) = TranslateQuery<Product, Product>(
            q => q.Where(p => p.Name.Contains(search)),
            connection, provider);

        // Only one parameter for the search value
        Assert.Single(parameters);
        var paramKeys = parameters.Keys.ToList();
        Assert.Equal(paramKeys.Count, paramKeys.Distinct().Count());
    }
}
