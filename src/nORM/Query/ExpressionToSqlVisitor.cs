using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
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

            var value = Expression.Lambda(node).Compile().DynamicInvoke();
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = value ?? DBNull.Value;
            _sql.Append(paramName);
            return node;
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
                        var containsValue = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke();
                        var containsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                        _params[containsParam] = $"%{containsValue}%";
                        _sql.Append(containsParam);
                        break;
                    case "StartsWith":
                        _sql.Append(" LIKE ");
                        var startsValue = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke();
                        var startsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                        _params[startsParam] = $"{startsValue}%";
                        _sql.Append(startsParam);
                        break;
                    case "EndsWith":
                        _sql.Append(" LIKE ");
                        var endsValue = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke();
                        var endsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                        _params[endsParam] = $"%{endsValue}";
                        _sql.Append(endsParam);
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
    }
}