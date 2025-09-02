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
        private readonly ParameterExpression _parameter;
        private readonly string _tableAlias;
        private readonly StringBuilder _sql = new();
        private readonly Dictionary<string, object> _params = new();
        private int _paramIndex = 0;

        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias)
        {
            _ctx = ctx;
            _mapping = mapping;
            _provider = provider;
            _parameter = parameter;
            _tableAlias = tableAlias;
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
            if (node.Expression == _parameter)
            {
                var column = _mapping.Columns.FirstOrDefault(c => c.Prop.Name == node.Member.Name);
                if (column != null)
                {
                    _sql.Append($"{_tableAlias}.{column.EscCol}");
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
            }
            else
            {
                return base.VisitMethodCall(node);
            }
            return node;
        }

        public Dictionary<string, object> GetParameters() => _params;

        private static bool TryGetConstantValue(Expression e, out object? value)
        {
            var stack = new Stack<MemberExpression>();
            while (e is MemberExpression me)
            {
                stack.Push(me);
                e = me.Expression!;
            }

            if (e is not ConstantExpression ce)
            {
                value = null;
                return false;
            }

            object? current = ce.Value;
            while (stack.Count > 0)
            {
                var m = stack.Pop();
                current = m.Member switch
                {
                    FieldInfo fi => fi.GetValue(current),
                    PropertyInfo pi => pi.GetValue(current),
                    _ => current
                };
            }

            value = current;
            return true;
        }

    }
}