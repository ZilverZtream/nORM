using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed class ExpressionToSqlVisitor : ExpressionVisitor
    {
        private readonly DbContext _ctx;
        private readonly TableMapping _mapping;
        private readonly DatabaseProvider _provider;
        private readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _parameterMappings;
        private readonly ParameterExpression _parameter;
        private readonly string _tableAlias;
        private readonly StringBuilder _sql = new();
        private readonly Dictionary<string, object> _params = new();
        private int _paramIndex = 0;
        private readonly List<string> _compiledParams;
        private readonly Dictionary<ParameterExpression, string> _paramMap;

        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias,
                                      Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
                                      List<string>? compiledParams = null,
                                      Dictionary<ParameterExpression, string>? paramMap = null)
        {
            _ctx = ctx;
            _mapping = mapping;
            _provider = provider;
            _parameter = parameter;
            _tableAlias = tableAlias;
            _parameterMappings = correlated != null
                ? new(correlated)
                : new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            _parameterMappings[parameter] = (mapping, tableAlias);
            _compiledParams = compiledParams ?? new List<string>();
            _paramMap = paramMap ?? new Dictionary<ParameterExpression, string>();
        }

        public string Translate(Expression expression)
        {
            Visit(expression);
            return _sql.ToString();
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _sql.Append("(");
            Visit(node.Left);
            _sql.Append(node.NodeType switch
            {
                ExpressionType.Equal => " = ",
                ExpressionType.NotEqual => " <> ",
                ExpressionType.GreaterThan => " > ",
                ExpressionType.GreaterThanOrEqual => " >= ",
                ExpressionType.LessThan => " < ",
                ExpressionType.LessThanOrEqual => " <= ",
                ExpressionType.AndAlso => " AND ",
                ExpressionType.OrElse => " OR ",
                _ => throw new NotSupportedException($"Binary operator '{node.NodeType}' not supported.")
            });
            Visit(node.Right);
            _sql.Append(")");
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ParameterExpression pe && _parameterMappings.TryGetValue(pe, out var info))
            {
                var column = info.Mapping.Columns.FirstOrDefault(c => c.Prop.Name == node.Member.Name);
                if (column != null)
                {
                    _sql.Append($"{info.Alias}.{column.EscCol}");
                    return node;
                }
            }

            if (TryGetConstantValue(node, out var value))
            {
                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                _params[paramName] = value ?? DBNull.Value;
                _sql.Append(paramName);
                return node;
            }

            if (node.Expression != null)
            {
                var exprSql = GetSql(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType!, exprSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
            }

            throw new NotSupportedException($"Member '{node.Member.Name}' is not supported in this context.");
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = node.Value ?? DBNull.Value;
            _sql.Append(paramName);
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (_parameterMappings.ContainsKey(node))
                return base.VisitParameter(node);

            if (_paramMap.TryGetValue(node, out var existing))
            {
                _sql.Append(existing);
                return node;
            }

            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                _sql.Append("(NOT(");
                Visit(node.Operand);
                _sql.Append("))");
                return node;
            }

            return base.VisitUnary(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (TryGetConstantValue(node, out var constVal))
            {
                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                _params[paramName] = constVal ?? DBNull.Value;
                _sql.Append(paramName);
                return node;
            }

            if (node.Method.DeclaringType == typeof(string))
            {
                switch (node.Method.Name)
                {
                    case "Contains":
                        Visit(node.Object);
                        _sql.Append(" LIKE ");
                        if (TryGetConstantValue(node.Arguments[0], out var contains) && contains is string cs)
                        {
                            var containsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _params[containsParam] = $"%{_provider.EscapeLikePattern(cs)}%";
                            _sql.Append(containsParam).Append($" ESCAPE '{_provider.LikeEscapeChar}'");
                        }
                        else
                        {
                            throw new NotSupportedException("Only constant values are supported in Contains().");
                        }
                        return node;
                    case "StartsWith":
                        Visit(node.Object);
                        _sql.Append(" LIKE ");
                        if (TryGetConstantValue(node.Arguments[0], out var starts) && starts is string ss)
                        {
                            var startsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _params[startsParam] = $"{_provider.EscapeLikePattern(ss)}%";
                            _sql.Append(startsParam).Append($" ESCAPE '{_provider.LikeEscapeChar}'");
                        }
                        else
                        {
                            throw new NotSupportedException("Only constant values are supported in StartsWith().");
                        }
                        return node;
                    case "EndsWith":
                        Visit(node.Object);
                        _sql.Append(" LIKE ");
                        if (TryGetConstantValue(node.Arguments[0], out var ends) && ends is string es)
                        {
                            var endsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _params[endsParam] = $"%{_provider.EscapeLikePattern(es)}";
                            _sql.Append(endsParam).Append($" ESCAPE '{_provider.LikeEscapeChar}'");
                        }
                        else
                        {
                            throw new NotSupportedException("Only constant values are supported in EndsWith().");
                        }
                        return node;
                }

                var strArgs = new List<string>();
                if (node.Object != null)
                    strArgs.Add(GetSql(node.Object));
                foreach (var a in node.Arguments)
                    strArgs.Add(GetSql(a));
                var fn = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, strArgs.ToArray());
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }

                throw new NotSupportedException($"String method '{node.Method.Name}' not supported.");
            }
            if (node.Method.DeclaringType == typeof(Enumerable) || node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case "Count":
                    case "LongCount":
                        if (node.Arguments.Count >= 1 && node.Arguments[0] is ParameterExpression cp && _parameterMappings.ContainsKey(cp))
                        {
                            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression countSelector)
                            {
                                var info = _parameterMappings[cp];
                                var visitor = new ExpressionToSqlVisitor(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap);
                                var predSql = visitor.Translate(countSelector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                _sql.Append($"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)");
                            }
                            else
                            {
                                _sql.Append("COUNT(*)");
                            }
                            return node;
                        }
                        break;
                    case "Sum":
                    case "Average":
                    case "Min":
                    case "Max":
                        if (node.Arguments.Count >= 2 && node.Arguments[0] is ParameterExpression gp && _parameterMappings.ContainsKey(gp))
                        {
                            var selector = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (selector != null)
                            {
                                var info = _parameterMappings[gp];
                                var visitor = new ExpressionToSqlVisitor(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap);
                                var colSql = visitor.Translate(selector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                var fn = node.Method.Name switch
                                {
                                    "Sum" => "SUM",
                                    "Average" => "AVG",
                                    "Min" => "MIN",
                                    "Max" => "MAX",
                                    _ => ""
                                };
                                _sql.Append($"{fn}({colSql})");
                                return node;
                            }
                        }
                        break;
                }
            }
            if (node.Method.Name == nameof(List<int>.Contains))
            {
                Expression? collectionExpr = null;
                Expression? valueExpr = null;

                if (node.Method.DeclaringType == typeof(Enumerable))
                {
                    if (node.Arguments.Count == 2)
                    {
                        collectionExpr = node.Arguments[0];
                        valueExpr = node.Arguments[1];
                    }
                }
                else if (node.Object != null && node.Arguments.Count == 1)
                {
                    collectionExpr = node.Object;
                    valueExpr = node.Arguments[0];
                }

                if (collectionExpr != null && valueExpr != null && TryGetConstantValue(collectionExpr, out var colVal) && colVal is IEnumerable en && colVal is not string)
                {
                    var items = new List<object?>();
                    foreach (var item in en)
                        items.Add(item);

                    if (items.Count == 0)
                    {
                        _sql.Append("(1=0)");
                        return node;
                    }

                    Visit(valueExpr);
                    _sql.Append(" IN (");
                    for (int i = 0; i < items.Count; i++)
                    {
                        if (i > 0) _sql.Append(", ");
                        var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                        _params[paramName] = items[i] ?? DBNull.Value;
                        _sql.Append(paramName);
                    }
                    _sql.Append(")");
                    return node;
                }
            }

            if (node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.Any):
                        BuildExists(node.Arguments[0], node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null, negate: false);
                        return node;
                    case nameof(Queryable.All):
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new ArgumentException("All requires a predicate");
                        var param = pred.Parameters[0];
                        var notBody = Expression.Not(pred.Body);
                        var lambda = Expression.Lambda(notBody, param);
                        BuildExists(node.Arguments[0], lambda, negate: true);
                        return node;
                    case nameof(Queryable.Contains):
                        BuildIn(node.Arguments[0], node.Arguments[1]);
                        return node;
                    default:
                        throw new NotSupportedException($"Queryable method '{node.Method.Name}' not supported.");
                }
            }

            var args = new List<string>();
            if (node.Object != null)
                args.Add(GetSql(node.Object));
            foreach (var a in node.Arguments)
                args.Add(GetSql(a));
            var fnSql = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, args.ToArray());
            if (fnSql != null)
            {
                _sql.Append(fnSql);
                return node;
            }

            var custom = node.Method.GetCustomAttribute<SqlFunctionAttribute>();
            if (custom != null)
            {
                var formatted = string.Format(custom.Format, args.ToArray());
                _sql.Append(formatted);
                return node;
            }

            throw new NotSupportedException($"Method '{node.Method.Name}' not supported.");
        }

        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }

            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);

            var subTranslator = new QueryTranslator(_ctx, mapping, _params, ref _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count);
            var subPlan = subTranslator.Translate(source);

            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }

        private void BuildIn(Expression source, Expression value)
        {
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);

            var subTranslator = new QueryTranslator(_ctx, mapping, _params, ref _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count);
            var subPlan = subTranslator.Translate(source);

            Visit(value);
            _sql.Append(" IN (");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }

        private static Type GetRootElementType(Expression source)
        {
            while (source is MethodCallExpression mce)
            {
                if (mce.Method.Name == "Query" && mce.Arguments.Count == 0)
                    return GetElementType(mce);
                source = mce.Arguments[0];
            }
            return GetElementType(source);
        }


        public Dictionary<string, object> GetParameters() => _params;

        private string GetSql(Expression expression)
        {
            var start = _sql.Length;
            Visit(expression);
            var segment = _sql.ToString(start, _sql.Length - start);
            _sql.Remove(start, _sql.Length - start);
            return segment;
        }

        private static bool TryGetConstantValue(Expression e, out object? value)
        {
            switch (e)
            {
                case ConstantExpression ce:
                    value = ce.Value;
                    return true;
                case MemberExpression me:
                    if (me.Expression != null && TryGetConstantValue(me.Expression, out var obj))
                    {
                        value = me.Member switch
                        {
                            FieldInfo fi => fi.GetValue(obj),
                            PropertyInfo pi => pi.GetValue(obj),
                            _ => null
                        };
                        return true;
                    }
                    break;
                case MethodCallExpression mce:
                    object? instance = null;
                    if (mce.Object != null && !TryGetConstantValue(mce.Object, out instance))
                    {
                        value = null;
                        return false;
                    }

                    var args = new object?[mce.Arguments.Count];
                    for (int i = 0; i < mce.Arguments.Count; i++)
                    {
                        if (!TryGetConstantValue(mce.Arguments[i], out var argVal))
                        {
                            value = null;
                            return false;
                        }
                        args[i] = argVal;
                    }

                    value = mce.Method.Invoke(instance, args);
                    return true;
            }

            value = null;
            return false;
        }

        private static Expression StripQuotes(Expression e)
            => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }

            var iface = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
            if (iface != null) return iface.GetGenericArguments()[0];

            throw new ArgumentException($"Cannot determine element type from expression of type {type}");
        }

    }
}