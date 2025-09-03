using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Collections.Generic;
using nORM.Core;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private static readonly Dictionary<string, IMethodCallTranslator> _methodTranslators = new()
        {
            { "Where", new WhereTranslator() },
            { "Select", new SelectTranslator() },
            { "OrderBy", new OrderByTranslator() },
            { "OrderByDescending", new OrderByTranslator() },
            { "ThenBy", new OrderByTranslator() },
            { "ThenByDescending", new OrderByTranslator() },
            { "Take", new TakeTranslator() },
            { "Skip", new SkipTranslator() },
            { "Join", new JoinTranslator(false) },
            { "GroupJoin", new JoinTranslator(true) },
            { "SelectMany", new SelectManyTranslator() },
            { "Distinct", new DistinctTranslator() },
            { "Reverse", new ReverseTranslator() },
            { "Union", new SetOperationTranslator() },
            { "Intersect", new SetOperationTranslator() },
            { "Except", new SetOperationTranslator() },
            { "Any", new SetPredicateTranslator() },
            { "Contains", new SetPredicateTranslator() },
            { "ElementAt", new ElementAtTranslator() },
            { "ElementAtOrDefault", new ElementAtTranslator() },
            { "First", new FirstSingleTranslator() },
            { "FirstOrDefault", new FirstSingleTranslator() },
            { "Single", new FirstSingleTranslator() },
            { "SingleOrDefault", new FirstSingleTranslator() },
            { "Last", new LastTranslator() },
            { "LastOrDefault", new LastTranslator() },
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
            { "Include", new IncludeTranslator() },
            { "ThenInclude", new ThenIncludeTranslator() },
            { "AsNoTracking", new AsNoTrackingTranslator() },
            { "AsSplitQuery", new AsSplitQueryTranslator() }
        };

        private sealed class WhereTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression lambda)
                {
                    var body = lambda.Body;
                    var param = lambda.Parameters[0];
                    var alias = "T" + t._joinCounter;
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var visitor = ExpressionVisitorPool.Get(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap);
                    var sql = visitor.Translate(body);
                    var isGrouping = node.Arguments[0] is MethodCallExpression mc && mc.Method.Name == "GroupBy";
                    var target = isGrouping ? t._having : t._where;
                    if (target.Length > 0) target.Append(" AND ");
                    target.Append($"({sql})");
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    ExpressionVisitorPool.Return(visitor);
                }
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class SelectTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._projection = QueryTranslator.StripQuotes(node.Arguments[1]) as LambdaExpression;
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class OrderByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = t.Visit(node.Arguments[0]);
                if (QueryTranslator.StripQuotes(node.Arguments[1]) is LambdaExpression keySelector)
                {
                    var param = keySelector.Parameters[0];
                    var alias = "T" + t._joinCounter;
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var visitor = ExpressionVisitorPool.Get(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap);
                    var sql = visitor.Translate(keySelector.Body);
                    t._orderBy.Add((sql, !t._methodName.Contains("Descending")));
                    ExpressionVisitorPool.Return(visitor);
                }
                return source;
            }
        }

        private sealed class TakeTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments[1] is ParameterExpression tp)
                {
                    if (!t._paramMap.TryGetValue(tp, out var tName))
                    {
                        tName = t._ctx.Provider.ParamPrefix + "p" + t._paramIndex++;
                        t._params[tName] = DBNull.Value;
                        t._compiledParams.Add(tName);
                        t._paramMap[tp] = tName;
                    }
                    t._takeParam = tName;
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int take))
                {
                    t._take = take;
                }
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class SkipTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments[1] is ParameterExpression sp)
                {
                    if (!t._paramMap.TryGetValue(sp, out var sName))
                    {
                        sName = t._ctx.Provider.ParamPrefix + "p" + t._paramIndex++;
                        t._params[sName] = DBNull.Value;
                        t._compiledParams.Add(sName);
                        t._paramMap[sp] = sName;
                    }
                    t._skipParam = sName;
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int skip))
                {
                    t._skip = skip;
                }
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class JoinTranslator : IMethodCallTranslator
        {
            private readonly bool _isGroupJoin;
            public JoinTranslator(bool isGroupJoin) => _isGroupJoin = isGroupJoin;
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return _isGroupJoin ? t.HandleGroupJoin(node) : t.HandleJoin(node, false);
            }
        }

        private sealed class SelectManyTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleSelectMany(node);
            }
        }

        private sealed class DistinctTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._isDistinct = true;
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class ReverseTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var revSource = t.Visit(node.Arguments[0]);
                if (t._orderBy.Count > 0)
                {
                    for (int i = 0; i < t._orderBy.Count; i++)
                    {
                        var (col, asc) = t._orderBy[i];
                        t._orderBy[i] = (col, !asc);
                    }
                }
                else
                {
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, false));
                }
                return revSource;
            }
        }

        private sealed class SetOperationTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var leftSql = t.TranslateSubExpression(node.Arguments[0]);
                var rightSql = t.TranslateSubExpression(node.Arguments[1]);
                var setOp = t._methodName switch
                {
                    "Union" => "UNION",
                    "Intersect" => "INTERSECT",
                    "Except" => "EXCEPT",
                    _ => throw new NormUnsupportedFeatureException("Set operation not supported")
                };
                t._sql.Clear();
                t._sql.Append($"({leftSql}) {setOp} ({rightSql})");
                return node;
            }
        }

        private sealed class SetPredicateTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleSetOperation(node);
            }
        }

        private sealed class ElementAtTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var elementSource = t.Visit(node.Arguments[0]);
                if (node.Arguments[1] is ParameterExpression eaParam)
                {
                    if (!t._paramMap.TryGetValue(eaParam, out var eName))
                    {
                        eName = t._ctx.Provider.ParamPrefix + "p" + t._paramIndex++;
                        t._params[eName] = DBNull.Value;
                        t._compiledParams.Add(eName);
                        t._paramMap[eaParam] = eName;
                    }
                    if (t._skipParam != null)
                        t._skipParam = $"({t._skipParam} + {eName})";
                    else if (t._skip != null)
                    {
                        t._skipParam = $"({t._skip} + {eName})";
                        t._skip = null;
                    }
                    else
                        t._skipParam = eName;
                }
                else if (QueryTranslator.TryGetIntValue(node.Arguments[1], out int index))
                {
                    if (t._skipParam != null)
                        t._skipParam = $"({t._skipParam} + {index})";
                    else
                        t._skip = (t._skip ?? 0) + index;
                }
                else
                {
                    throw new NormUnsupportedFeatureException("ElementAt requires constant integer index or parameter.");
                }

                t._take = 1;
                t._singleResult = t._methodName == "ElementAt";
                if (t._orderBy.Count == 0)
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, true));
                return elementSource;
            }
        }

        private sealed class FirstSingleTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression predicate)
                {
                    var param = predicate.Parameters[0];
                    var alias = "T" + t._joinCounter;
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var visitor = ExpressionVisitorPool.Get(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap);
                    var sql = visitor.Translate(predicate.Body);
                    if (t._where.Length > 0) t._where.Append(" AND ");
                    t._where.Append($"({sql})");
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    ExpressionVisitorPool.Return(visitor);
                }
                t._take = 1;
                t._singleResult = t._methodName == "First" || t._methodName == "Single";
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class LastTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression lastPredicate)
                {
                    var param = lastPredicate.Parameters[0];
                    var alias = "T" + t._joinCounter;
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var visitor = ExpressionVisitorPool.Get(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap);
                    var sql = visitor.Translate(lastPredicate.Body);
                    if (t._where.Length > 0) t._where.Append(" AND ");
                    t._where.Append($"({sql})");
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    ExpressionVisitorPool.Return(visitor);
                }
                var lastSrc = t.Visit(node.Arguments[0]);
                if (t._orderBy.Count > 0)
                {
                    for (int i = 0; i < t._orderBy.Count; i++)
                    {
                        var (col, asc) = t._orderBy[i];
                        t._orderBy[i] = (col, !asc);
                    }
                }
                else
                {
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, false));
                }
                t._take = 1;
                t._singleResult = t._methodName == "Last";
                return lastSrc;
            }
        }

        private sealed class CountTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._isAggregate = true;
                t._sql.Clear();
                t._sql.Append("SELECT COUNT(*)");
                if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression countPredicate)
                {
                    var param = countPredicate.Parameters[0];
                    var alias = "T" + t._joinCounter;
                    if (!t._correlatedParams.ContainsKey(param))
                        t._correlatedParams[param] = (t._mapping, alias);
                    var visitor = ExpressionVisitorPool.Get(t._ctx, t._mapping, t._provider, param, alias, t._correlatedParams, t._compiledParams, t._paramMap);
                    var sql = visitor.Translate(countPredicate.Body);
                    if (t._where.Length > 0) t._where.Append(" AND ");
                    t._where.Append($"({sql})");
                    foreach (var kvp in visitor.GetParameters())
                        t._params[kvp.Key] = kvp.Value;
                    ExpressionVisitorPool.Return(visitor);
                }
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class AggregateExpressionTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleAggregateExpression(node);
            }
        }

        private sealed class GroupByTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleGroupBy(node);
            }
        }

        private sealed class DirectAggregateTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleDirectAggregate(node);
            }
        }

        private sealed class AllTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleAllOperation(node);
            }
        }

        private sealed class RowNumberTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleRowNumberOperation(node);
            }
        }

        private sealed class RankTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleRankOperation(node);
            }
        }

        private sealed class DenseRankTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleDenseRankOperation(node);
            }
        }

        private sealed class IncludeTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments.Count > 1)
                {
                    var includeExpr = node.Arguments[1];
                    if (includeExpr is LambdaExpression includeLambda)
                    {
                        var member = includeLambda.Body is UnaryExpression unary ?
                                     (MemberExpression)unary.Operand :
                                     (MemberExpression)includeLambda.Body;
                        var propName = member.Member.Name;
                        if (t._mapping.Relations.TryGetValue(propName, out var relation))
                        {
                            t._includes.Add(new IncludePlan(new List<TableMapping.Relation> { relation }));
                            t.TrackMapping(relation.DependentType);
                        }
                    }
                }
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class ThenIncludeTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var parentExpression = t.Visit(node.Arguments[0]);
                if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression thenLambda)
                {
                    var member = thenLambda.Body is UnaryExpression unary2 ?
                                 (MemberExpression)unary2.Operand :
                                 (MemberExpression)thenLambda.Body;
                    var propName = member.Member.Name;
                    if (t._includes.Count > 0)
                    {
                        var lastInclude = t._includes[^1];
                        var lastRelation = lastInclude.Path.Last();
                        var parentMap = t.TrackMapping(lastRelation.DependentType);
                        if (parentMap.Relations.TryGetValue(propName, out var relation))
                        {
                            lastInclude.Path.Add(relation);
                            t.TrackMapping(relation.DependentType);
                        }
                    }
                }
                return parentExpression;
            }
        }

        private sealed class AsNoTrackingTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._noTracking = true;
                return t.Visit(node.Arguments[0]);
            }
        }

        private sealed class AsSplitQueryTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._splitQuery = true;
                return t.Visit(node.Arguments[0]);
            }
        }
    }
}
