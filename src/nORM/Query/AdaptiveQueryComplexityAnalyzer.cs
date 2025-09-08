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
    internal sealed class AdaptiveQueryComplexityAnalyzer
    {
        private readonly IMemoryMonitor _memoryMonitor;
        private readonly SemaphoreSlim _complexQuerySemaphore;
        private static readonly ConcurrentDictionary<int, QueryComplexityInfo> _analysisCache = new();

        private const int DefaultEnumerationLimit = 2000;

        public AdaptiveQueryComplexityAnalyzer(IMemoryMonitor memoryMonitor, int maxConcurrentComplexQueries = 2)
        {
            _memoryMonitor = memoryMonitor;
            _complexQuerySemaphore = new SemaphoreSlim(maxConcurrentComplexQueries);
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

        public QueryComplexityInfo AnalyzeQuery(Expression query, DbContextOptions options)
        {
            if (query is null) throw new ArgumentNullException(nameof(query));
            if (options is null) throw new ArgumentNullException(nameof(options));

            var fingerprint = ExpressionFingerprint.Compute(query);

            if (_analysisCache.TryGetValue(fingerprint, out var cached))
                return cached;

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

            if (baseAnalysis.EstimatedCost > adaptedLimits.HighCostThreshold)
            {
                if (!_complexQuerySemaphore.Wait(TimeSpan.FromSeconds(1)))
                    throw new NormQueryException("System is under high query load. Please retry later.");
                _complexQuerySemaphore.Release();
                throw new NormQueryException(string.Format(
                    ErrorMessages.QueryTranslationFailed,
                    $"Query complexity too high (cost: {baseAnalysis.EstimatedCost}, threshold: {adaptedLimits.HighCostThreshold})."));
            }

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
            // Scale limits based on available memory in GB, clamped between 1 and 16
            var memoryFactor = Math.Clamp((int)(availableMemory / (1024L * 1024L * 1024L)), 1, 16);
            return new AdaptiveLimits
            {
                MaxJoinDepth = 10 * memoryFactor,
                MaxWhereConditions = 50 * memoryFactor,
                MaxParameterCount = 2000 * memoryFactor,
                MaxEstimatedCost = 10000 * memoryFactor,
                HighCostThreshold = 5000 * memoryFactor
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

            if (info.HasCartesianProduct)
                info.WarningMessages.Add("Query may produce cartesian product - verify join conditions");
        }

        private sealed class ComplexityVisitor : ExpressionVisitor
        {
            private readonly QueryComplexityInfo _complexity = new();
            private readonly HashSet<Type> _joinedTypes = new();
            private int _nestedSelectDepth;

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
                        _complexity.EstimatedCost += 100;
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
                return base.VisitConstant(node);
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

                _complexity.EstimatedCost += _complexity.JoinCount * 1000;
            }

            private void AnalyzeWhere(MethodCallExpression node)
            {
                if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression lambda)
                {
                    var conditionCount = CountConditions(lambda.Body);
                    _complexity.WhereConditionCount += conditionCount;
                }
            }

            private void AnalyzeSelectMany(MethodCallExpression node)
            {
                _nestedSelectDepth++;
                if (_nestedSelectDepth > 5)
                {
                    _complexity.HasCartesianProduct = true;
                    _complexity.WarningMessages.Add("Deep SelectMany nesting detected - potential performance issue");
                }
                _complexity.EstimatedCost += _nestedSelectDepth * 500;
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
                var baseCost = 100;
                var joinCost = _complexity.JoinCount * _complexity.JoinCount * 500;
                var whereCost = _complexity.WhereConditionCount * 50;
                var cartesianPenalty = _complexity.HasCartesianProduct ? 5000 : 0;
                var nestingPenalty = _nestedSelectDepth * _nestedSelectDepth * 200;
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
    }
}
