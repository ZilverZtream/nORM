using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
        private readonly List<PropertyInfo> _detectedCollections = new();

        public SelectClauseVisitor(TableMapping mapping, List<string> groupBy, DatabaseProvider provider)
        {
            _mapping = mapping;
            _groupBy = groupBy;
            _provider = provider;
        }

        /// <summary>
        /// Gets the list of navigation properties detected during translation that should be
        /// fetched via separate queries to avoid Cartesian explosion.
        /// </summary>
        public IReadOnlyList<PropertyInfo> DetectedCollections => _detectedCollections;

        /// <summary>
        /// Translates a projection expression into a comma-separated SQL <c>SELECT</c> clause.
        /// </summary>
        /// <param name="e">The projection expression to translate.</param>
        /// <returns>SQL representing the projection.</returns>
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
            bool firstColumn = true;
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                var memberName = node.Members![i].Name;

                // Check if this is a navigation property (collection)
                if (IsNavigationCollection(arg, out var navProperty))
                {
                    // Track it for later split query processing
                    _detectedCollections.Add(navProperty);
                    // Skip adding to SQL SELECT - it will be fetched separately
                    continue;
                }

                if (!firstColumn) _sb.Append(", ");
                Visit(arg);
                _sb.Append($" AS {_provider.Escape(memberName)}");
                firstColumn = false;
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
                _sb.Append(_mapping.ColumnsByName[node.Member.Name].EscCol);
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
                    _sb.Append(_mapping.ColumnsByName[me.Member.Name].EscCol);
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
            bool firstColumn = true;
            for (int i = 0; i < node.Bindings.Count; i++)
            {
                if (node.Bindings[i] is MemberAssignment assignment)
                {
                    // Check if this is a navigation property (collection)
                    if (IsNavigationCollection(assignment.Expression, out var navProperty))
                    {
                        // Track it for later split query processing
                        _detectedCollections.Add(navProperty);
                        // Skip adding to SQL SELECT - it will be fetched separately
                        continue;
                    }

                    if (!firstColumn) _sb.Append(", ");
                    Visit(assignment.Expression);
                    _sb.Append($" AS {_provider.Escape(assignment.Member.Name)}");
                    firstColumn = false;
                }
            }
            return node;
        }

        /// <summary>
        /// Determines if the expression represents a navigation property that is a collection.
        /// </summary>
        private bool IsNavigationCollection(Expression expr, out PropertyInfo property)
        {
            property = null!;

            // Look for member access like "b.Posts"
            if (expr is MemberExpression memberExpr &&
                memberExpr.Member is PropertyInfo propInfo &&
                memberExpr.Expression is ParameterExpression)
            {
                var propType = propInfo.PropertyType;

                // Check if it's a collection type (IEnumerable<T> but not string)
                if (propType != typeof(string) &&
                    typeof(IEnumerable).IsAssignableFrom(propType) &&
                    propType.IsGenericType)
                {
                    // Verify it's actually a navigation property (not a column)
                    if (!_mapping.ColumnsByName.ContainsKey(propInfo.Name))
                    {
                        property = propInfo;
                        return true;
                    }
                }
            }

            return false;
        }

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
    }
}