using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Microsoft.Extensions.ObjectPool;
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
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());
        private StringBuilder _sb = null!;

        public SelectClauseVisitor(TableMapping mapping, List<string> groupBy, DatabaseProvider provider)
        {
            _mapping = mapping;
            _groupBy = groupBy;
            _provider = provider;
        }

        public string Translate(Expression e)
        {
            _sb = _stringBuilderPool.Get();
            try
            {
                Visit(e);
                return _sb.ToString();
            }
            finally
            {
                _sb.Clear();
                _stringBuilderPool.Return(_sb);
            }
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
                for (int i = 0; i < _groupBy.Count; i++)
                {
                    if (i > 0) _sb.Append(", ");
                    _sb.Append(_groupBy[i]);
                }
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