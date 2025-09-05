using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Xunit;
using nORM.Query;
using nORM.Configuration;
using nORM.Core;

namespace nORM.Tests;

public class QueryComplexityAnalyzerTests : TestBase
{
    private class Product
    {
        public int Id { get; set; }
    }

    private class CountingEnumerable : IEnumerable<int>
    {
        public int IterationCount { get; private set; }
        private readonly int _limit;

        public CountingEnumerable(int limit) => _limit = limit;

        public IEnumerator<int> GetEnumerator()
        {
            while (true)
            {
                IterationCount++;
                if (IterationCount > _limit)
                    throw new InvalidOperationException("Enumeration exceeded limit");
                yield return 0;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class CountingCollection : IEnumerable<int>, ICollection
    {
        private readonly int _count;
        public int EnumerationCount { get; private set; }

        public CountingCollection(int count) => _count = count;

        public int Count => _count;
        public object SyncRoot => this;
        public bool IsSynchronized => false;
        public void CopyTo(Array array, int index) => throw new NotImplementedException();

        public IEnumerator<int> GetEnumerator()
        {
            EnumerationCount++;
            return Enumerable.Range(0, _count).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class TestMemoryMonitor : IMemoryMonitor
    {
        public long Available { get; set; } = 512L * 1024 * 1024;
        public long GetAvailableMemory() => Available;
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
        var analyzer = new AdaptiveQueryComplexityAnalyzer(new TestMemoryMonitor());
        Assert.Throws<NormQueryException>(() => analyzer.AnalyzeQuery(query.Expression, new DbContextOptions()));
    }

    [Fact]
    public void Large_IEnumerable_parameter_is_not_fully_enumerated()
    {
        const int MaxParameterCount = 2000;
        var ids = new CountingEnumerable(5000);
        var expr = Expression.Constant(ids);
        var analyzer = new AdaptiveQueryComplexityAnalyzer(new TestMemoryMonitor());
        Assert.Throws<NormQueryException>(() => analyzer.AnalyzeQuery(expr, new DbContextOptions()));
        Assert.True(ids.IterationCount <= MaxParameterCount + 1);
    }

    [Fact]
    public void ICollection_parameter_uses_count_property()
    {
        var ids = new CountingCollection(5);
        var expr = Expression.Constant(ids);
        var analyzer = new AdaptiveQueryComplexityAnalyzer(new TestMemoryMonitor());
        var result = analyzer.AnalyzeQuery(expr, new DbContextOptions());
        var paramCount = result.ParameterCount;

        Assert.Equal(5, paramCount);
        Assert.Equal(0, ids.EnumerationCount);
    }
}
