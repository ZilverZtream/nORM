using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private static readonly Dictionary<string, IMethodCallTranslator> _methodTranslators = new()
        {
            { "Cacheable", new CacheableTranslator() },
            { "Where", new WhereTranslator() },
            { "Select", new SelectTranslator() },
            { "OrderBy", new OrderByTranslator() },
            { "OrderByDescending", new OrderByTranslator() },
            { "ThenBy", new OrderByTranslator() },
            { "ThenByDescending", new OrderByTranslator() },
            { "Take", new TakeTranslator() },
            { "Skip", new SkipTranslator() },
            { "TakeWhile", new TakeSkipWhileTranslator(takeWhile: true) },
            { "SkipWhile", new TakeSkipWhileTranslator(takeWhile: false) },
            { "TakeLast", new TakeLastTranslator() },
            { "SkipLast", new SkipLastTranslator() },
            { "Join", new JoinTranslator(false) },
            { "GroupJoin", new JoinTranslator(true) },
            { "SelectMany", new SelectManyTranslator() },
            { "Distinct", new DistinctTranslator() },
            { "DistinctBy", new DistinctByTranslator() },
            { "ExceptBy", new ExceptByTranslator() },
            { "IntersectBy", new IntersectByTranslator() },
            { "UnionBy", new UnionByTranslator() },
            { "DefaultIfEmpty", new DefaultIfEmptyTranslator() },
            { "Chunk", new ChunkTranslator() },
            { "Append", new AppendPrependTranslator(append: true) },
            { "Prepend", new AppendPrependTranslator(append: false) },
            { "Zip", new ZipTranslator() },
            { "Reverse", new ReverseTranslator() },
            { "Union", new SetOperationTranslator() },
            { "Concat", new SetOperationTranslator() },
            { "Intersect", new SetOperationTranslator() },
            { "Except", new SetOperationTranslator() },
            { "Any", new SetPredicateTranslator() },
            { "Contains", new SetPredicateTranslator() },
            { "SequenceEqual", new SequenceEqualTranslator() },
            { "ElementAt", new ElementAtTranslator() },
            { "ElementAtOrDefault", new ElementAtTranslator() },
            { "First", new FirstSingleTranslator() },
            { "FirstOrDefault", new FirstSingleTranslator() },
            { "Single", new FirstSingleTranslator() },
            { "SingleOrDefault", new FirstSingleTranslator() },
            { "Last", new LastTranslator() },
            { "LastOrDefault", new LastTranslator() },
            { "MinBy", new MinByMaxByTranslator() },
            { "MaxBy", new MinByMaxByTranslator() },
            { "Count", new CountTranslator() },
            { "LongCount", new CountTranslator() },
            { "InternalSumExpression", new AggregateExpressionTranslator() },
            { "InternalAverageExpression", new AggregateExpressionTranslator() },
            { "InternalMinExpression", new AggregateExpressionTranslator() },
            { "InternalMaxExpression", new AggregateExpressionTranslator() },
            { "GroupBy", new GroupByTranslator() },
            { "Sum", new DirectAggregateTranslator() },
            { "Average", new DirectAggregateTranslator() },
            { "Min", new DirectAggregateTranslator() },
            { "Max", new DirectAggregateTranslator() },
            { "All", new AllTranslator() },
            { "WithRowNumber", new RowNumberTranslator() },
            { "WithRank", new RankTranslator() },
            { "WithDenseRank", new DenseRankTranslator() },
            { "WithLag", new LagTranslator() },
            { "WithLead", new LeadTranslator() },
            { "Include", new IncludeTranslator() },
            { "ThenInclude", new ThenIncludeTranslator() },
            { "AsNoTracking", new AsNoTrackingTranslator() },
            { "AsSplitQuery", new AsSplitQueryTranslator() },
            { "IgnoreQueryFilters", new IgnoreQueryFiltersTranslator() },
            { "AsOf", new AsOfTranslator() },
            { "Cast", new CastOrOfTypeTranslator() },
            { "OfType", new CastOrOfTypeTranslator() }
        };

    }
}
