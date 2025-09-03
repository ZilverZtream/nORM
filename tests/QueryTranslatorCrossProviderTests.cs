using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using nORM.Providers;
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
        return $"SELECT {cols} FROM {table}" + (alias ? " T0" : string.Empty);
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
        var expected = $"{BaseSelect(provider, true)} ORDER BY T0.{provider.Escape("Name")} ASC, T0.{provider.Escape("Id")} ASC";
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
        var expected = $"{BaseSelect(provider, true)} ORDER BY T0.{provider.Escape("Name")} DESC, T0.{provider.Escape("Id")} DESC";
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Take_applies_limit(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.Take(5), connection, provider);
        var sb = new StringBuilder(BaseSelect(provider));
        provider.ApplyPaging(sb, 5, null, null, null);
        var expected = sb.ToString();
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Skip_applies_offset(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.Skip(10), connection, provider);
        var sb = new StringBuilder(BaseSelect(provider));
        provider.ApplyPaging(sb, null, 10, null, null);
        var expected = sb.ToString();
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Skip_and_Take_for_paging(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var (sql, _, _) = TranslateQuery<Product, Product>(q => q.OrderBy(p => p.Id).Skip(20).Take(10), connection, provider);
        var sb = new StringBuilder($"{BaseSelect(provider, true)} ORDER BY T0.{provider.Escape("Id")} ASC");
        provider.ApplyPaging(sb, 10, 20, null, null);
        var expected = sb.ToString();
        Assert.Equal(expected, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void ApplyPaging_throws_on_invalid_parameter_name(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var sb = new StringBuilder(BaseSelect(provider));
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
        var expected = $"SELECT T1.{cols.Replace(", ", ", T1.")} FROM {table} T0 CROSS JOIN {table} T1";
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
        var expected = $"SELECT T0.{col} FROM {table} T0 GROUP BY T0.{col} HAVING ((COUNT(*) > {paramName}))";
        Assert.Equal(expected, sql);
        Assert.Equal(10, parameters[paramName]);
        Assert.Equal(typeof(int), elementType);
    }

}
