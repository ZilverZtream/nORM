using System.Linq;
using System.Reflection;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class QueryTranslatorRecursionTests : TestBase
{
    private class Item
    {
        public int Id { get; set; }
    }

    private static IQueryable<Item> BuildNestedUnion(IQueryable<Item> source, int depth)
    {
        if (depth <= 0) return source;
        return source.Union(BuildNestedUnion(source, depth - 1));
    }

    [Fact]
    public void Deeply_nested_unions_throw()
    {
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ex = Assert.Throws<TargetInvocationException>(() =>
            TranslateQuery<Item, Item>(q => BuildNestedUnion(q, 110), connection, provider));
        Assert.IsType<NormQueryException>(ex.InnerException);
    }
}
