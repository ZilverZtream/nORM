using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed class SelectClauseVisitor : ExpressionVisitor
    {
        private readonly TableMapping _mapping;
        private readonly List<string> _groupBy;
        private readonly DatabaseProvider _provider;
        private readonly StringBuilder _sb = new();

        public SelectClauseVisitor(TableMapping mapping, List<string> groupBy, DatabaseProvider provider)
        {
            _mapping = mapping;
            _groupBy = groupBy;
            _provider = provider;
        }

        public string Translate(Expression e)
        {
            Visit(e);
            return _sb.ToString();
        }

        protected override Expression VisitNew(NewExpression node)
        {
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                if (i > 0) _sb.Append(", ");
                Visit(node.Arguments[i]);
                _sb.Append($" AS {_provider.Escape(node.Members![i].Name)}");
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ParameterExpression p && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() == typeof(IGrouping<,>) && node.Member.Name == "Key")
            {
                _sb.Append(string.Join(", ", _groupBy));
            }
            else
            {
                _sb.Append(_mapping.Columns.First(c => c.Prop.Name == node.Member.Name).EscCol);
            }
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            _sb.Append($"{node.Method.Name.ToUpper()}(");
            if (node.Arguments.Count > 1)
            {
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                if (lambda.Body is MemberExpression me)
                {
                    _sb.Append(_mapping.Columns.First(c => c.Prop.Name == me.Member.Name).EscCol);
                }
            }
            else if (node.Method.Name.ToUpper() != "COUNT")
            {
                _sb.Append(_groupBy.FirstOrDefault() ?? "*");
            }
            else
            {
                _sb.Append("*");
            }
            _sb.Append(")");
            return node;
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            for (int i = 0; i < node.Bindings.Count; i++)
            {
                if (i > 0) _sb.Append(", ");
                if (node.Bindings[i] is MemberAssignment assignment)
                {
                    Visit(assignment.Expression);
                    _sb.Append($" AS {_provider.Escape(assignment.Member.Name)}");
                }
            }
            return node;
        }

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
    }
}