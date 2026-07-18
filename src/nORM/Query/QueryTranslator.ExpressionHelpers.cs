using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;
using System.Text;
using nORM.Core;
using nORM.Configuration;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private string EscapeAlias(string alias)
        {
            if (string.IsNullOrWhiteSpace(alias))
                throw new NormQueryException($"Invalid table alias: {alias}");
            return _provider.Escape(alias);
        }
        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

        /// <summary>
        /// Strips an optional <c>.Where(pred)</c> wrap so the nav-aggregate detection paths
        /// (<see cref="QueryTranslator.OrderByTranslator"/> family) can match
        /// <c>parent.Items.Sum(...)</c> and <c>parent.Items.Where(p).Sum(...)</c> uniformly.
        /// Returns the unwrapped <c>MemberExpression</c> for <c>parent.Items</c>, or the
        /// original input if it doesn't fit the shape.
        /// </summary>
        internal static Expression UnwrapNavMember(Expression e)
        {
            if (e is MethodCallExpression mc
                && mc.Method.Name == nameof(Queryable.Where)
                && mc.Arguments.Count == 2)
            {
                return mc.Arguments[0];
            }
            return e;
        }
        private LambdaExpression ComposeGroupJoinExpansionSelector(LambdaExpression resultSelector)
        {
            if (_groupJoinExpansionSelector != null
                && resultSelector.Parameters.Count >= 1
                && resultSelector.Parameters[0].Type == _groupJoinExpansionSelector.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[0], _groupJoinExpansionSelector.Body).Visit(resultSelector.Body)!;
                body = new ProjectionMemberReplacer().Visit(body)!;
                var parameters = _groupJoinExpansionSelector.Parameters
                    .Concat(resultSelector.Parameters.Skip(1))
                    .ToArray();
                return Expression.Lambda(body, parameters);
            }

            return resultSelector;
        }
        private LambdaExpression ComposeTransparentIdentifierSelector(LambdaExpression resultSelector)
        {
            var previous = _transparentIdentifier ?? _projection;
            if (previous != null
                && resultSelector.Parameters.Count >= 1
                && resultSelector.Parameters[0].Type == previous.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[0], previous.Body).Visit(resultSelector.Body)!;
                body = new ProjectionMemberReplacer().Visit(body)!;
                var parameters = previous.Parameters
                    .Concat(resultSelector.Parameters.Skip(1))
                    .ToArray();
                return Expression.Lambda(body, parameters);
            }

            return resultSelector;
        }

        private void RegisterTransparentIdentifierTail(LambdaExpression selector, TableMapping mapping, string alias)
        {
            if (selector.Parameters.Count == 0)
                return;

            var tail = selector.Parameters[^1];
            if (!_correlatedParams.ContainsKey(tail))
                _correlatedParams[tail] = (mapping, alias);
        }

        private LambdaExpression ExpandProjection(LambdaExpression lambda)
        {
            // Prefer the transparent-identifier lambda when present - that's the one that
            // unpacks `t.l` / `t.r` back into the join's outer/inner parameters. Fall back
            // to a normal _projection inline for non-join shapes.
            if (_transparentIdentifier != null &&
                lambda.Parameters.Count == 1 &&
                lambda.Parameters[0].Type == _transparentIdentifier.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(lambda.Parameters[0], _transparentIdentifier.Body).Visit(lambda.Body)!;
                body = new ProjectionMemberReplacer().Visit(body);
                return Expression.Lambda(body, _transparentIdentifier.Parameters);
            }
            if (_projection != null &&
                lambda.Parameters.Count == 1 &&
                lambda.Parameters[0].Type == _projection.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(lambda.Parameters[0], _projection.Body).Visit(lambda.Body)!;
                body = new ProjectionMemberReplacer().Visit(body);
                return Expression.Lambda(body, _projection.Parameters);
            }
            // GroupJoin's result selector isn't stored in _projection (materialiser
            // limitation - see HandleGroupJoin), but downstream Where/OrderBy still
            // need to expand `r.Member` back to the outer/group expressions inside
            // the selector. Fall through to it here when no regular projection is set.
            if (_groupJoinExpansionSelector != null &&
                lambda.Parameters.Count == 1 &&
                lambda.Parameters[0].Type == _groupJoinExpansionSelector.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(lambda.Parameters[0], _groupJoinExpansionSelector.Body).Visit(lambda.Body)!;
                body = new ProjectionMemberReplacer().Visit(body);
                return Expression.Lambda(body, _groupJoinExpansionSelector.Parameters);
            }
            if (_groupJoinResultSelector != null &&
                lambda.Parameters.Count == 1 &&
                lambda.Parameters[0].Type == _groupJoinResultSelector.Body.Type)
            {
                var body = new nORM.Internal.ParameterReplacer(lambda.Parameters[0], _groupJoinResultSelector.Body).Visit(lambda.Body)!;
                body = new ProjectionMemberReplacer().Visit(body);
                return Expression.Lambda(body, _groupJoinResultSelector.Parameters);
            }
            return lambda;
        }
        private bool ProjectionContainsMember(string memberName)
        {
            return _projection?.Body switch
            {
                NewExpression ne when ne.Members != null => ne.Members.Any(m => m.Name == memberName),
                MemberInitExpression mi => mi.Bindings.OfType<MemberAssignment>().Any(b => b.Member.Name == memberName),
                _ => false
            };
        }
        private static Expression UnwrapQueryExpression(Expression expression) =>
            expression is MethodCallExpression mc &&
            !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
            mc.Arguments.Count > 0
                ? mc.Arguments[0]
                : expression;

        /// <summary>
        /// Walks to the deepest source expression and returns the <see cref="INormRawSqlSource"/> at the root
        /// of the query, if any — i.e. when the query started from <c>FromSqlRaw</c>/<c>FromSqlInterpolated</c>.
        /// Descends through the operator chain (each operator's first argument) and unwraps the
        /// <c>Convert</c>/<c>Quote</c> nodes nORM inserts for its interface-typed operators.
        /// </summary>
        internal static INormRawSqlSource? FindRootRawSource(Expression expression)
        {
            var expr = expression;
            while (true)
            {
                switch (expr)
                {
                    // Static extension operators (Queryable.Where/OrderBy/Count…) carry the source as Arguments[0];
                    // nORM's instance operators (AsNoTracking/AsSplitQuery/Include, built as Convert(src).Op(...))
                    // carry it as the receiver Object with the predicate/path in Arguments. Follow whichever holds.
                    case MethodCallExpression { Object: { } receiver }:
                        expr = receiver;
                        break;
                    case MethodCallExpression mc when mc.Arguments.Count > 0:
                        expr = mc.Arguments[0];
                        break;
                    case UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.Quote } u:
                        expr = u.Operand;
                        break;
                    case ConstantExpression c:
                        return c.Value as INormRawSqlSource;
                    default:
                        return null;
                }
            }
        }

        // Operators that compose cleanly on top of a FromSqlRaw derived table: they leave the raw SQL as the
        // sole FROM source (only Where/OrderBy/paging/projection/scalar-terminals + query-config markers).
        private static readonly System.Collections.Generic.HashSet<string> RawSqlComposableOperators =
            new(System.StringComparer.Ordinal)
            {
                "Where", "Select", "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending",
                "Skip", "Take", "Distinct", "Count", "LongCount", "Any", "All",
                "First", "FirstOrDefault", "Single", "SingleOrDefault", "Last", "LastOrDefault",
                "ElementAt", "ElementAtOrDefault", "Sum", "Min", "Max", "Average",
                "AsNoTracking", "AsTracking", "AsSplitQuery", "IgnoreQueryFilters", "TagWith", "Cast",
                // Cacheable wraps the result set; it doesn't rewrite the FROM. The result-cache key includes the
                // rendered SQL (so distinct raw SQL never collides) and the entity's mapped table drives
                // invalidation — sound whenever the raw SQL reads that table, the standard caller-owns-SQL caveat.
                "Cacheable",
            };

        /// <summary>
        /// Rejects operators that can't yet compose onto a <c>FromSqlRaw</c> query — joins, <c>GroupBy</c>,
        /// <c>Include</c>, <c>SelectMany</c>, set operators — because they build the <c>FROM</c> clause from a
        /// different source than the raw SQL and would silently query the mapped table instead. Everything on
        /// the whitelist keeps the raw SQL as the sole table source. Called only when a raw root is present.
        /// </summary>
        private static void EnsureRawSqlComposableShape(Expression expression)
        {
            var expr = expression;
            while (true)
            {
                switch (expr)
                {
                    case MethodCallExpression mc when mc.Object != null || mc.Arguments.Count > 0:
                        if (!RawSqlComposableOperators.Contains(mc.Method.Name))
                            throw new nORM.Core.NormUnsupportedFeatureException(
                                $"'{mc.Method.Name}' can't be composed onto a FromSqlRaw query yet. Supported: Where, " +
                                "Select, OrderBy/ThenBy, Skip/Take, Distinct, and the scalar terminals (Count, Any, " +
                                "First, Sum, …). For joins, GroupBy, Include, or set operators, materialise the raw " +
                                "query first (ToList) and continue with LINQ-to-Objects, or express them in the SQL.");
                        // Instance operators keep the source as the receiver; static extensions keep it at Arguments[0].
                        expr = mc.Object ?? mc.Arguments[0];
                        break;
                    case UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.Quote } u:
                        expr = u.Operand;
                        break;
                    default:
                        return;
                }
            }
        }
        private static string? ExtractPropertyName(Expression expression)
        {
            return expression switch
            {
                MemberExpression member => member.Member.Name,
                UnaryExpression { Operand: MemberExpression member } => member.Member.Name,
                _ => null
            };
        }
    }
}
