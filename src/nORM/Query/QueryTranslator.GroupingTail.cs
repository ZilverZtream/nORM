using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private bool TryCreateProjectedTailSelector(LambdaExpression selector, out LambdaExpression projectedSelector)
        {
            projectedSelector = selector;
            if (_projection == null || selector.Parameters.Count != 1)
                return false;

            var projectedType = _projection.Body.Type;
            var projectedParam = Expression.Parameter(projectedType, "__projected");
            var rewriter = new ProjectedTailSelectorRewriter(selector.Parameters[0], projectedParam, projectedType);
            var body = rewriter.Visit(selector.Body)!;
            if (!rewriter.Replaced)
                return false;

            projectedSelector = Expression.Lambda(body, projectedParam);
            return true;
        }

        private static bool SourceContainsGroupBy(Expression source)
        {
            if (source is MethodCallExpression call)
            {
                if (call.Method.Name == nameof(Queryable.GroupBy))
                    return true;
                return call.Arguments.Count > 0 && SourceContainsGroupBy(call.Arguments[0]);
            }

            return false;
        }

        private static bool IsGroupingSequenceType(Type type)
        {
            if (!type.IsGenericType)
                return false;

            var def = type.GetGenericTypeDefinition();
            return def == typeof(IGrouping<,>) || def == typeof(IEnumerable<>);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ProjectedTailSelectorRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _source;
            private readonly ParameterExpression _projected;
            private readonly Type _projectedType;

            public ProjectedTailSelectorRewriter(ParameterExpression source, ParameterExpression projected, Type projectedType)
            {
                _source = source;
                _projected = projected;
                _projectedType = projectedType;
            }

            public bool Replaced { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression == _source && node.Type == _projectedType)
                {
                    Replaced = true;
                    return _projected;
                }

                return base.VisitMember(node);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ProjectionMemberReplacer : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                var visitedExpression = node.Expression == null ? null : Visit(node.Expression);
                if (visitedExpression is NewExpression newExpr)
                {
                    if (newExpr.Members != null)
                    {
                        for (int i = 0; i < newExpr.Members.Count; i++)
                        {
                            if (newExpr.Members[i].Name == node.Member.Name)
                                return Visit(newExpr.Arguments[i]);
                        }
                    }

                    var parameters = newExpr.Constructor?.GetParameters();
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length && i < newExpr.Arguments.Count; i++)
                        {
                            if (string.Equals(parameters[i].Name, node.Member.Name, StringComparison.OrdinalIgnoreCase))
                                return Visit(newExpr.Arguments[i]);
                        }
                    }
                }
                if (visitedExpression is MemberInitExpression memberInit)
                {
                    foreach (var binding in memberInit.Bindings)
                    {
                        if (binding is MemberAssignment ma && ma.Member.Name == node.Member.Name)
                            return Visit(ma.Expression);
                    }
                }
                if (visitedExpression != node.Expression)
                    return node.Update(visitedExpression);

                return base.VisitMember(node);
            }
        }
    }

    /// <summary>
    /// Concrete IGrouping implementation returned by the streaming GroupBy transform.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal sealed class ClientGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private readonly List<TElement> _elements;

        public ClientGrouping(TKey key, IEnumerable<TElement> elements)
        {
            Key = key;
            _elements = new List<TElement>(elements);
        }

        public TKey Key { get; }

        public IEnumerator<TElement> GetEnumerator() => _elements.GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _elements.GetEnumerator();
    }
}
