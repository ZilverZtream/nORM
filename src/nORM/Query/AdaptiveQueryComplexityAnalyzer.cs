using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using nORM.Core;
using nORM.Configuration;
namespace nORM.Query
{
    /// <summary>
    /// Analyzes query expression trees and adapts complexity limits based on current system resources.
    /// </summary>
    internal sealed class AdaptiveQueryComplexityAnalyzer : IDisposable
    {
        private readonly IMemoryMonitor _memoryMonitor;
        private readonly ConcurrentDictionary<ExpressionFingerprint, QueryComplexityInfo> _analysisCache = new();
        private const int DefaultEnumerationLimit = 2000;
        private const int MaxCacheSize = 10000;

        // Memory-factor scaling constants used by CalculateAdaptiveLimits.
        private const long BytesPerGigabyte = 1024L * 1024L * 1024L;
        private const int MinMemoryFactorGb = 1;
        private const int MaxMemoryFactorGb = 16;
        private const int BaseJoinDepthPerGb = 10;
        private const int BaseWhereConditionsPerGb = 50;
        private const int BaseParameterCountPerGb = 2000;
        private const int BaseMaxEstimatedCostPerGb = 10000;
        private const int BaseHighCostThresholdPerGb = 5000;

        // Cost weights for query complexity estimation.
        private const int BaseCost = 100;
        private const int JoinCostMultiplier = 500;
        private const int WhereCostMultiplier = 50;
        private const int CartesianPenalty = 5000;
        private const int NestingCostMultiplier = 200;
        private const int OrderByCost = 100;
        private const int SelectManyCostMultiplier = 500;
        private const int NestedSelectDepthCartesianThreshold = 5;
        private const int JoinCostPerOccurrence = 1000;
        public AdaptiveQueryComplexityAnalyzer(IMemoryMonitor memoryMonitor)
        {
            _memoryMonitor = memoryMonitor;
        }
        internal sealed class QueryComplexityInfo
        {
            public int JoinCount { get; set; }
            public int WhereConditionCount { get; set; }
            public int ParameterCount { get; set; }
            public int EstimatedCost { get; set; }
            public bool HasCartesianProduct { get; set; }
            public List<string> WarningMessages { get; } = new();
        }
        private sealed class AdaptiveLimits
        {
            public int MaxJoinDepth { get; init; }
            public int MaxWhereConditions { get; init; }
            public int MaxParameterCount { get; init; }
            public int MaxEstimatedCost { get; init; }
            public int HighCostThreshold { get; init; }
        }
        /// <summary>
        /// Performs a complexity analysis for the specified query expression and adapts the
        /// permissible limits based on current system resources and configured options. Results
        /// are cached using an expression fingerprint so repeated analyses are inexpensive.
        /// </summary>
        /// <param name="query">The expression tree representing the query to analyze.</param>
        /// <param name="options">Context options influencing the adaptive limits.</param>
        /// <returns>Detailed information about the query's complexity characteristics.</returns>
        public QueryComplexityInfo AnalyzeQuery(Expression query, DbContextOptions options)
        {
            if (query is null) throw new ArgumentNullException(nameof(query));
            if (options is null) throw new ArgumentNullException(nameof(options));
            var fingerprint = ExpressionFingerprint.Compute(query);
            if (_analysisCache.TryGetValue(fingerprint, out var cached))
                return cached;
            if (_analysisCache.Count >= MaxCacheSize)
            {
                // Evict oldest half rather than wiping the entire cache to avoid
                // a thundering-herd where every concurrent miss repopulates from scratch.
                var toRemove = _analysisCache.Keys.Take(MaxCacheSize / 2).ToList();
                foreach (var k in toRemove)
                    _analysisCache.TryRemove(k, out _);
            }
            if (IsSimpleQuery(query))
            {
                var simpleInfo = new QueryComplexityInfo();
                _analysisCache[fingerprint] = simpleInfo;
                return simpleInfo;
            }
            var baseAnalysis = AnalyzeQueryStructure(query);
            var availableMemory = _memoryMonitor.GetAvailableMemory();
            var adaptedLimits = CalculateAdaptiveLimits(availableMemory, options);
            ValidateAgainstAdaptiveLimits(baseAnalysis, adaptedLimits);
            _analysisCache[fingerprint] = baseAnalysis;
            return baseAnalysis;
        }
        private static QueryComplexityInfo AnalyzeQueryStructure(Expression query)
        {
            var visitor = new ComplexityVisitor();
            visitor.Visit(query);
            return visitor.GetComplexityInfo();
        }
        private static bool IsSimpleQuery(Expression expr)
        {
            if (expr is not MethodCallExpression)
                return false;
            MethodCallExpression? whereCall = null;
            Expression current = expr;
            while (current is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Queryable))
            {
                if (mc.Method.Name == nameof(Queryable.Where))
                {
                    if (whereCall != null) return false;
                    whereCall = mc;
                    current = mc.Arguments[0];
                }
                else if (mc.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                    or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault))
                {
                    current = mc.Arguments[0];
                }
                else
                {
                    return false;
                }
            }
            if (current is not ConstantExpression)
                return false;
            if (whereCall == null)
                return true;
            var lambda = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
            return lambda.Body switch
            {
                MemberExpression me when me.Type == typeof(bool) => true,
                BinaryExpression be when be.NodeType == ExpressionType.Equal => true,
                _ => false
            };
        }
        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
                e = ((UnaryExpression)e).Operand;
            return e;
        }
        private static AdaptiveLimits CalculateAdaptiveLimits(long availableMemory, DbContextOptions options)
        {
            // Scale limits based on available memory in GB, clamped between MinMemoryFactorGb and MaxMemoryFactorGb
            var memoryFactor = Math.Clamp((int)(availableMemory / BytesPerGigabyte), MinMemoryFactorGb, MaxMemoryFactorGb);
            return new AdaptiveLimits
            {
                MaxJoinDepth = BaseJoinDepthPerGb * memoryFactor,
                MaxWhereConditions = BaseWhereConditionsPerGb * memoryFactor,
                MaxParameterCount = BaseParameterCountPerGb * memoryFactor,
                MaxEstimatedCost = BaseMaxEstimatedCostPerGb * memoryFactor,
                HighCostThreshold = BaseHighCostThresholdPerGb * memoryFactor
            };
        }
        private static void ValidateAgainstAdaptiveLimits(QueryComplexityInfo info, AdaptiveLimits limits)
        {
            if (info.JoinCount > limits.MaxJoinDepth)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Query exceeds maximum join depth of {limits.MaxJoinDepth}"));
            if (info.WhereConditionCount > limits.MaxWhereConditions)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Query exceeds maximum WHERE conditions of {limits.MaxWhereConditions}"));
            if (info.ParameterCount > limits.MaxParameterCount)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Query exceeds maximum parameter count of {limits.MaxParameterCount}"));
            if (info.EstimatedCost > limits.MaxEstimatedCost)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Query complexity too high (cost: {info.EstimatedCost}, max: {limits.MaxEstimatedCost}). Consider simplifying the query or breaking it into smaller operations."));
            if (info.EstimatedCost > limits.HighCostThreshold)
                info.WarningMessages.Add($"High query cost ({info.EstimatedCost}) - may impact performance");
            if (info.HasCartesianProduct)
                info.WarningMessages.Add("Query may produce cartesian product - verify join conditions");
        }
        private sealed class ComplexityVisitor : ExpressionVisitor
        {
            private readonly QueryComplexityInfo _complexity = new();
            private readonly HashSet<Type> _joinedTypes = new();
            private int _nestedSelectDepth;
            /// <summary>
            /// Finalizes the collected statistics and returns an immutable snapshot describing the
            /// complexity of the visited expression tree.
            /// </summary>
            /// <returns>An instance of <see cref="QueryComplexityInfo"/> containing the metrics.</returns>
            public QueryComplexityInfo GetComplexityInfo()
            {
                _complexity.EstimatedCost = CalculateEstimatedCost();
                return _complexity;
            }
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                switch (node.Method.Name)
                {
                    case "Join":
                    case "GroupJoin":
                        AnalyzeJoin(node);
                        break;
                    case "Where":
                        AnalyzeWhere(node);
                        break;
                    case "SelectMany":
                        AnalyzeSelectMany(node);
                        break;
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                        _complexity.EstimatedCost += OrderByCost;
                        break;
                }
                return base.VisitMethodCall(node);
            }
            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value is System.Collections.IEnumerable enumerable &&
                    node.Value is not string &&
                    node.Value is not IQueryable)
                {
                    CountEnumerable(enumerable);
                }
                return base.VisitConstant(node);
            }

            protected override Expression VisitMember(System.Linq.Expressions.MemberExpression node)
            {
                // Evaluate closure-captured IEnumerable fields/properties
                if (node.Expression is System.Linq.Expressions.ConstantExpression constExpr)
                {
                    object? value = node.Member switch
                    {
                        System.Reflection.FieldInfo fi => fi.GetValue(constExpr.Value),
                        System.Reflection.PropertyInfo pi => pi.GetValue(constExpr.Value),
                        _ => null
                    };
                    if (value is System.Collections.IEnumerable captured &&
                        value is not string &&
                        value is not IQueryable)
                    {
                        CountEnumerable(captured);
                        return node; // Don't descend further; the constant was already handled
                    }
                }
                return base.VisitMember(node);
            }

            private void CountEnumerable(System.Collections.IEnumerable enumerable)
            {
                int count;
                if (enumerable is System.Collections.ICollection collection)
                {
                    count = collection.Count;
                }
                else
                {
                    count = 0;
                    foreach (var _ in enumerable)
                    {
                        count++;
                        if (count > DefaultEnumerationLimit)
                            break;
                    }
                }
                _complexity.ParameterCount += count;
            }
            private void AnalyzeJoin(MethodCallExpression node)
            {
                _complexity.JoinCount++;
                var outerType = GetElementType(node.Arguments[0]);
                var innerType = GetElementType(node.Arguments[1]);
                if (_joinedTypes.Contains(outerType) && _joinedTypes.Contains(innerType))
                {
                    _complexity.HasCartesianProduct = true;
                    _complexity.WarningMessages.Add($"Potential cartesian product detected between {outerType.Name} and {innerType.Name}");
                }
                _joinedTypes.Add(outerType);
                _joinedTypes.Add(innerType);
                _complexity.EstimatedCost += _complexity.JoinCount * JoinCostPerOccurrence;
            }
            private void AnalyzeWhere(MethodCallExpression node)
            {
                if (node.Arguments.Count > 1)
                {
                    // Handle quoted lambdas: LINQ expression trees may wrap lambda args in UnaryExpression(Quote).
                    var arg = node.Arguments[1];
                    while (arg is UnaryExpression { NodeType: ExpressionType.Quote } q)
                        arg = q.Operand;
                    if (arg is LambdaExpression lambda)
                    {
                        var conditionCount = CountConditions(lambda.Body);
                        _complexity.WhereConditionCount += conditionCount;
                    }
                }
            }
            private void AnalyzeSelectMany(MethodCallExpression node)
            {
                _nestedSelectDepth++;
                if (_nestedSelectDepth > NestedSelectDepthCartesianThreshold)
                {
                    _complexity.HasCartesianProduct = true;
                    _complexity.WarningMessages.Add("Deep SelectMany nesting detected - potential performance issue");
                }
                _complexity.EstimatedCost += _nestedSelectDepth * SelectManyCostMultiplier;
            }
            private static int CountConditions(Expression expression)
            {
                return expression switch
                {
                    BinaryExpression binary when binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse
                        => CountConditions(binary.Left) + CountConditions(binary.Right),
                    BinaryExpression => 1,
                    MethodCallExpression method when method.Method.Name == "Contains" => 1,
                    _ => 0
                };
            }
            private int CalculateEstimatedCost()
            {
                var baseCost = BaseCost;
                var joinCost = _complexity.JoinCount * _complexity.JoinCount * JoinCostMultiplier;
                var whereCost = _complexity.WhereConditionCount * WhereCostMultiplier;
                var cartesianPenalty = _complexity.HasCartesianProduct ? CartesianPenalty : 0;
                var nestingPenalty = _nestedSelectDepth * _nestedSelectDepth * NestingCostMultiplier;
                return baseCost + joinCost + whereCost + cartesianPenalty + nestingPenalty;
            }
            private static Type GetElementType(Expression expression)
            {
                var type = expression.Type;
                if (type.IsGenericType)
                    return type.GetGenericArguments()[0];
                var queryableInterface = type.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
                return queryableInterface?.GetGenericArguments()[0] ?? typeof(object);
            }
        }
        /// <summary>
        /// Releases resources held by the analyzer. The current implementation is stateless but
        /// the method is provided for future extensibility and to conform to <see cref="IDisposable"/>.
        /// </summary>
        public void Dispose()
        {
        }
    }
}