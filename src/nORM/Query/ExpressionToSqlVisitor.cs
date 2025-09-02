using System;
using System.Collections.Generic;
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

        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias,
                                      Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null)
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

            throw new NotSupportedException($"Member '{node.Member.Name}' is not supported in this context.");
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = node.Value ?? DBNull.Value;
            _sql.Append(paramName);
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(string))
            {
                Visit(node.Object);
                switch (node.Method.Name)
                {
                    case "Contains":
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
                        break;
                    case "StartsWith":
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
                        break;
                    case "EndsWith":
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
                        break;
                    default:
                        throw new NotSupportedException($"String method '{node.Method.Name}' not supported.");
                }
                return node;
            }
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.Any):
                        BuildExists(node.Arguments[0], node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null, negate:false);
                        return node;
                    case nameof(Queryable.All):
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new ArgumentException("All requires a predicate");
                        var param = pred.Parameters[0];
                        var notBody = Expression.Not(pred.Body);
                        var lambda = Expression.Lambda(notBody, param);
                        BuildExists(node.Arguments[0], lambda, negate:true);
                        return node;
                    case nameof(Queryable.Contains):
                        var source = node.Arguments[0];
                        var elementType = GetElementType(source);
                        var p = Expression.Parameter(elementType, "x");
                        var eq = Expression.Equal(p, Expression.Convert(node.Arguments[1], elementType));
                        var l = Expression.Lambda(eq, p);
                        BuildExists(source, l, negate:false);
                        return node;
                    default:
                        throw new NotSupportedException($"Queryable method '{node.Method.Name}' not supported.");
                }
            }
            return base.VisitMethodCall(node);
        }

        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            var elementType = GetElementType(source);
            var mapping = _ctx.GetMapping(elementType);
            var alias = "T" + _parameterMappings.Count;
            var builder = new StringBuilder();
            builder.Append($"SELECT 1 FROM {mapping.EscTable} {alias} ");
            if (predicate != null)
            {
                var visitor = new ExpressionToSqlVisitor(_ctx, mapping, _provider, predicate.Parameters[0], alias, _parameterMappings);
                var predSql = visitor.Translate(predicate.Body);
                foreach (var kvp in visitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                builder.Append($"WHERE ({predSql})");
            }
            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(builder);
            _sql.Append(")");
        }

        public Dictionary<string, object> GetParameters() => _params;

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