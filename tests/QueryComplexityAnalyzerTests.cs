using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using nORM.Core;

namespace nORM.Tests;

public class QueryComplexityAnalyzerTests : TestBase
{
    private class Product
    {
        public int Id { get; set; }
    }

    private static IQueryable<Product> BuildDeepJoins(IQueryable<Product> q)
    {
        var query = q;
        for (int i = 0; i < 11; i++)
        {
            query = query.Join(q, o => o.Id, i2 => i2.Id, (o, i2) => o);
        }
        return query;
    }

    [Fact]
    public void Excessive_join_depth_throws()
    {
        var query = BuildDeepJoins(new List<Product>().AsQueryable());
        var analyzerType = typeof(nORM.Query.QueryTranslator).Assembly.GetType("nORM.Query.QueryComplexityAnalyzer", true)!;
        var analyzeMethod = analyzerType.GetMethod("AnalyzeQuery", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!;
        var ex = Assert.Throws<System.Reflection.TargetInvocationException>(() =>
            analyzeMethod.Invoke(null, new object[] { query.Expression }));
        Assert.IsType<NormQueryTranslationException>(ex.InnerException);
    }
}
