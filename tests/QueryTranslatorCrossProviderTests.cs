using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;
using nORM.Core;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

public class QueryTranslatorCrossProviderTests : TestBase
{
    private class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public int CategoryId { get; set; }
        public bool IsAvailable { get; set; }
    }

    private class ProductDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public static IEnumerable<object[]> Providers()
    {
        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
            yield return new object[] { provider };
    }

    private static string Columns(DatabaseProvider provider) => string.Join(", ", new[]
    {
        provider.Escape("Id"),
        provider.Escape("Name"),
        provider.Escape("Price"),
        provider.Escape("CategoryId"),
        provider.Escape("IsAvailable")
    });

    private static string BaseSelect(DatabaseProvider provider, bool alias = false)
    {
        var cols = Columns(provider);
        var table = provider.Escape("Product");
        return $"SELECT {cols} FROM {table}" + (alias ? $" {provider.Escape("T0")}" : string.Empty);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Select_into_anonymous_type(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, elementType) = TranslateQuery<Product, object>(q => q.Select(p => new { p.Id, p.Name }), connection, provider);
        var expected = $"SELECT {provider.Escape("Id")} AS {provider.Escape("Id")}, {provider.Escape("Name")} AS {provider.Escape("Name")} FROM {provider.Escape("Product")}";
        Assert.Equal(expected, sql);
        Assert.Empty(parameters);
        Assert.StartsWith("<>", elementType.Name);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Select_into_named_dto_class(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, elementType) = TranslateQuery<Product, ProductDto>(q => q.Select(p => new ProductDto { Id = p.Id, Name = p.Name }), connection, provider);
        var expected = $"SELECT {provider.Escape("Id")} AS {provider.Escape("Id")}, {provider.Escape("Name")} AS {provider.Escape("Name")} FROM {provider.Escape("Product")}";
        Assert.Equal(expected, sql);
        Assert.Empty(parameters);
        Assert.Equal(typeof(ProductDto), elementType);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void OrderBy_followed_by_ThenBy(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.OrderBy(p => p.Name).ThenBy(p => p.Id), connection, provider);
        var t0 = provider.Escape("T0");
        var expected = $"{BaseSelect(provider, true)} ORDER BY {t0}.{provider.Escape("Name")} ASC, {t0}.{provider.Escape("Id")} ASC";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void OrderByDescending_followed_by_ThenByDescending(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.OrderByDescending(p => p.Name).ThenByDescending(p => p.Id), connection, provider);
        var t0 = provider.Escape("T0");
        var expected = $"{BaseSelect(provider, true)} ORDER BY {t0}.{provider.Escape("Name")} DESC, {t0}.{provider.Escape("Id")} DESC";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Take_applies_limit(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, _) = TranslateQuery<Product, Product>(q => q.Take(5), connection, provider);
        var sb = new OptimizedSqlBuilder();
        sb.Append(BaseSelect(provider));
        var paramName = provider.ParamPrefix + "p0";
        provider.ApplyPaging(sb, 5, null, paramName, null);
        var expected = sb.ToSqlString();
        Assert.Equal(expected, sql);
        var param = Assert.Single(parameters);
        Assert.Equal(paramName, param.Key);
        Assert.Equal(5, param.Value);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Skip_applies_offset(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, _) = TranslateQuery<Product, Product>(q => q.Skip(10), connection, provider);
        var sb = new OptimizedSqlBuilder();
        sb.Append(BaseSelect(provider));
        var paramName = provider.ParamPrefix + "p0";
        provider.ApplyPaging(sb, null, 10, null, paramName);
        var expected = sb.ToSqlString();
        Assert.Equal(expected, sql);
        var param = Assert.Single(parameters);
        Assert.Equal(paramName, param.Key);
        Assert.Equal(10, param.Value);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Skip_and_Take_for_paging(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, _) = TranslateQuery<Product, Product>(q => q.OrderBy(p => p.Id).Skip(20).Take(10), connection, provider);
        var t0 = provider.Escape("T0");
        var sb = new OptimizedSqlBuilder();
        sb.Append(BaseSelect(provider, true)).Append(" ORDER BY ").Append(t0).Append('.').Append(provider.Escape("Id")).Append(" ASC");
        var offsetParam = provider.ParamPrefix + "p0";
        var limitParam = provider.ParamPrefix + "p1";
        provider.ApplyPaging(sb, 10, 20, limitParam, offsetParam);
        var expected = sb.ToSqlString();
        Assert.Equal(expected, sql);
        Assert.Equal(2, parameters.Count);
        Assert.Equal(20, parameters[offsetParam]);
        Assert.Equal(10, parameters[limitParam]);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void ApplyPaging_throws_on_invalid_parameter_name(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var sb = new OptimizedSqlBuilder();
        sb.Append(BaseSelect(provider));
        Assert.Throws<ArgumentException>(() => provider.ApplyPaging(sb, 5, null, "p0", null));
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void SelectMany_creates_cross_join(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, elementType) = TranslateQuery<Product, Product>(q => q.SelectMany(p => q), connection, provider);
        var cols = Columns(provider);
        var table = provider.Escape("Product");
        var t0 = provider.Escape("T0");
        var t1 = provider.Escape("T1");
        var colsWithAlias = string.Join(", ", new[] {
            $"{t1}.{provider.Escape("Id")}",
            $"{t1}.{provider.Escape("Name")}",
            $"{t1}.{provider.Escape("Price")}",
            $"{t1}.{provider.Escape("CategoryId")}",
            $"{t1}.{provider.Escape("IsAvailable")}"
        });
        var expected = $"SELECT {colsWithAlias} FROM {table} {t0} CROSS JOIN {table} {t1}";
        Assert.Equal(expected, sql);
        Assert.Empty(parameters);
        Assert.Equal(typeof(Product), elementType);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Distinct_adds_keyword(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.Distinct(), connection, provider);
        var expected = $"SELECT DISTINCT {Columns(provider)} FROM {provider.Escape("Product")}";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void WithRowNumber_projects_row_number(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, object>(
            q => q.OrderBy(p => p.Id).WithRowNumber((p, rn) => new { p.Id, RowNumber = rn }),
            connection,
            provider);

        var t0 = provider.Escape("T0");
        var orderBy = $"ORDER BY {t0}.{provider.Escape("Id")} ASC";
        var expected = $"SELECT {provider.Escape("Id")} AS {provider.Escape("Id")}, ROW_NUMBER() OVER ({orderBy}) AS {provider.Escape("RowNumber")} FROM {provider.Escape("Product")} {t0} {orderBy}";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Union_combines_queries(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.Union(q), connection, provider);
        var select = $"SELECT {Columns(provider)} FROM {provider.Escape("Product")}";
        var expected = $"({select}) UNION ({select})";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Intersect_combines_queries(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.Intersect(q), connection, provider);
        var select = $"SELECT {Columns(provider)} FROM {provider.Escape("Product")}";
        var expected = $"({select}) INTERSECT ({select})";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Except_combines_queries(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.Except(q), connection, provider);
        var select = $"SELECT {Columns(provider)} FROM {provider.Escape("Product")}";
        var expected = $"({select}) EXCEPT ({select})";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void GroupBy_followed_by_having(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, parameters, elementType) = TranslateQuery<Product, int>(
            q => q.GroupBy(p => p.CategoryId)
                  .Where(g => g.Count() > 10)
                  .Select(g => g.Key),
            connection, provider);
        var table = provider.Escape("Product");
        var col = provider.Escape("CategoryId");
        var paramName = provider.ParamPrefix + "p0";
        var t0 = provider.Escape("T0");
        var expected = $"SELECT {t0}.{col} FROM {table} {t0} GROUP BY {t0}.{col} HAVING ((COUNT(*) > {paramName}))";
        Assert.Equal(expected, sql);
        Assert.Equal(10, parameters[paramName]);
        Assert.Equal(typeof(int), elementType);
    }

}
