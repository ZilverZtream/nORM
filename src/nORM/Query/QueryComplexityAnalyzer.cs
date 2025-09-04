using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;

namespace nORM.Query
{
    internal static class QueryComplexityAnalyzer
    {
        private const int MaxJoinDepth = 10;
        private const int MaxWhereConditions = 50;
        private const int MaxOrderByColumns = 10;
        private const int MaxGroupByColumns = 20;
        private const int MaxParameterCount = 2000;
        private const int MaxEstimatedCost = 10000;

        internal sealed class QueryComplexityInfo
        {
            public int JoinCount { get; set; }
            public int WhereConditionCount { get; set; }
            public int ParameterCount { get; set; }
            public int EstimatedCost { get; set; }
            public bool HasCartesianProduct { get; set; }
            public List<string> WarningMessages { get; } = new();
        }

        public static QueryComplexityInfo AnalyzeQuery(Expression query)
        {
            var analyzer = new ComplexityVisitor();
            analyzer.Visit(query);
            return analyzer.GetComplexityInfo();
        }

        private sealed class ComplexityVisitor : ExpressionVisitor
        {
            private readonly QueryComplexityInfo _complexity = new();
            private readonly HashSet<Type> _joinedTypes = new();
            private int _nestedSelectDepth = 0;

            public QueryComplexityInfo GetComplexityInfo()
            {
                _complexity.EstimatedCost = CalculateEstimatedCost();
                ValidateComplexity();
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
                    var count = 0;
                    foreach (var _ in enumerable)
                        count++;
                    _complexity.ParameterCount += count;
                    if (_complexity.ParameterCount > MaxParameterCount)
                        throw new NormQueryTranslationException($"Query exceeds maximum parameter count of {MaxParameterCount}");
                }
                return base.VisitConstant(node);
            }

            private void AnalyzeJoin(MethodCallExpression node)
            {
                _complexity.JoinCount++;
                if (_complexity.JoinCount > MaxJoinDepth)
                    throw new NormQueryTranslationException($"Query exceeds maximum join depth of {MaxJoinDepth}");

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
                    if (_complexity.WhereConditionCount > MaxWhereConditions)
                        throw new NormQueryTranslationException($"Query exceeds maximum WHERE conditions of {MaxWhereConditions}");
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

            private void ValidateComplexity()
            {
                if (_complexity.EstimatedCost > MaxEstimatedCost)
                    throw new NormQueryTranslationException(
                        $"Query complexity too high (cost: {_complexity.EstimatedCost}, max: {MaxEstimatedCost}). " +
                        "Consider simplifying the query or breaking it into smaller operations.");

                if (_complexity.HasCartesianProduct)
                    _complexity.WarningMessages.Add("Query may produce cartesian product - verify join conditions");
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
