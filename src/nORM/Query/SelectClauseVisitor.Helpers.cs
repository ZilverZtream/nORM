using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        /// <summary>
        /// Determines if the expression represents a navigation property that is a collection
        /// (e.g., <c>ICollection&lt;T&gt;</c>, <c>List&lt;T&gt;</c>) rather than a mapped scalar column.
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

                // Check if it's a generic collection type (IEnumerable<T> but not string,
                // which implements IEnumerable but is a scalar column type).
                if (propType != typeof(string) &&
                    typeof(IEnumerable).IsAssignableFrom(propType) &&
                    propType.IsGenericType)
                {
                    // Verify it's actually a navigation property (not a mapped column)
                    if (!_mapping.ColumnsByName.ContainsKey(propInfo.Name))
                    {
                        property = propInfo;
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Matches a projection binding that resolves to a navigation collection, optionally
        /// filtered: a bare <c>o.Lines</c>, <c>o.Lines.ToList()/ToArray()</c>, or
        /// <c>o.Lines.Where(pred).ToList()</c>. The captured predicate (if any) is applied to the
        /// split-query child fetch so only matching children populate the collection. A projection
        /// (<c>Select</c>) into a different element type is deliberately NOT matched here — that
        /// falls through to the general translatability path (a later stage handles it).
        /// </summary>
        private bool TryMatchDetectedCollection(Expression expr, out PropertyInfo navProperty, out LambdaExpression? filter, out LambdaExpression? projection)
        {
            navProperty = null!;
            filter = null;
            projection = null;
            var current = expr;

            // Peel a terminal ToList()/ToArray()/AsEnumerable().
            if (current is MethodCallExpression term
                && term.Arguments.Count == 1
                && (term.Method.DeclaringType == typeof(Enumerable) || term.Method.DeclaringType == typeof(Queryable))
                && term.Method.Name is "ToList" or "ToArray" or "AsEnumerable")
            {
                current = term.Arguments[0];
            }

            // A single Select(source, elementProjection) → capture the element projection so the child
            // materializer shapes each child into the projected element type. Peeled BEFORE Where because
            // the LINQ order is `o.Lines.Where(pred).Select(proj)`. Only a projection that reads solely its
            // own element (no closure captures, no outer-row references) is admitted — the child materializer
            // has only the child entity, and a closure would bake stale into the cached materializer; anything
            // else falls through to the existing fail-loud (client-eval) path.
            if (current is MethodCallExpression selectCall
                && selectCall.Arguments.Count == 2
                && (selectCall.Method.DeclaringType == typeof(Enumerable) || selectCall.Method.DeclaringType == typeof(Queryable))
                && selectCall.Method.Name == nameof(Enumerable.Select)
                && UnwrapLambda(selectCall.Arguments[1]) is { Parameters.Count: 1 } projLambda)
            {
                if (!IsSafeChildProjection(projLambda))
                    return false;
                projection = projLambda;
                current = selectCall.Arguments[0];
            }

            // A single Where(source, predicate) → capture the predicate.
            if (current is MethodCallExpression whereCall
                && whereCall.Arguments.Count == 2
                && (whereCall.Method.DeclaringType == typeof(Enumerable) || whereCall.Method.DeclaringType == typeof(Queryable))
                && whereCall.Method.Name == nameof(Enumerable.Where)
                && UnwrapLambda(whereCall.Arguments[1]) is { Parameters.Count: 1 } predLambda)
            {
                filter = predLambda;
                current = whereCall.Arguments[0];
            }

            return IsNavigationCollection(current, out navProperty);
        }

        /// <summary>
        /// True when a shaped-collection element projection reads ONLY its own element parameter — no
        /// references to the outer projection row or any other parameter, which the child materializer
        /// cannot supply. Closure captures ARE admitted: when present the owning plan is marked
        /// non-cacheable (see <see cref="ProjectionCapturesClosures"/>) so the projection re-translates with
        /// the current captured value each execution instead of freezing the first run's into a cached
        /// delegate. The projection is applied client-side over each fetched child entity.
        /// </summary>
        internal static bool IsSafeChildProjection(LambdaExpression projection)
            => AnalyzeChildProjection(projection).ReferencesOnlyElement;

        /// <summary>
        /// True when a shaped-collection element projection captures a closure variable. The owning plan
        /// must then be non-cacheable so the captured value is re-read per execution rather than frozen
        /// into the cached client-side projection delegate.
        /// </summary>
        internal static bool ProjectionCapturesClosures(LambdaExpression projection)
            => AnalyzeChildProjection(projection).CapturesClosures;

        private static ChildProjectionAnalysis AnalyzeChildProjection(LambdaExpression projection)
        {
            var analysis = new ChildProjectionAnalysis(projection.Parameters[0]);
            analysis.Visit(projection.Body);
            return analysis;
        }

        private sealed class ChildProjectionAnalysis : ExpressionVisitor
        {
            private readonly ParameterExpression _elementParam;
            public bool ReferencesOnlyElement { get; private set; } = true;
            public bool CapturesClosures { get; private set; }
            public ChildProjectionAnalysis(ParameterExpression elementParam) => _elementParam = elementParam;

            protected override Expression VisitMember(MemberExpression node)
            {
                // A member whose root folds to a constant is a closure capture.
                if (QueryTranslator.TryGetConstantValue(node, out _))
                {
                    CapturesClosures = true;
                    return node; // do not descend into the captured constant
                }
                return base.VisitMember(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                // Any parameter other than the element (the outer row, or a nested lambda's parameter)
                // is not available to the child materializer.
                if (node != _elementParam)
                    ReferencesOnlyElement = false;
                return node;
            }
        }

        /// <summary>
        /// Renders a shaped collection binding's predicate to SQL against the child table, capturing the
        /// compiled-parameter names it minted for closure captures. Rendering happens here at translation
        /// time — while the ambient closure-ordinal scope is active and the shared compiled-parameter
        /// channel is open — so a captured variable becomes a <c>@cpN</c> slot that the extractor re-binds
        /// per execution rather than a literal baked into the cached plan. Returns null when the collection
        /// has no resolvable relation (its children can't be fetched at all, so no filter is meaningful).
        /// </summary>
        private RenderedCollectionFilter? RenderShapedCollectionFilter(PropertyInfo navProperty, LambdaExpression filter)
        {
            if (_ctx == null || !_mapping.Relations.TryGetValue(navProperty.Name, out var relation))
                return null;

            var childAlias = _ctx.GetMapping(relation.DependentType).EscTable;
            var before = SharedCompiledParams?.Count ?? 0;
            var sql = RenderNavigationFilter(filter, childAlias);

            IReadOnlyList<string> parameters = Array.Empty<string>();
            if (SharedCompiledParams != null && SharedCompiledParams.Count > before)
            {
                var minted = new List<string>(SharedCompiledParams.Count - before);
                for (var i = before; i < SharedCompiledParams.Count; i++)
                    minted.Add(SharedCompiledParams[i]);
                parameters = minted;
            }
            return new RenderedCollectionFilter(sql, parameters);
        }

        /// <summary>
        /// Renders a filtered-Include predicate to SQL against an explicit alias (the eager-load level's
        /// alias), closure-safely through the shared compiled-parameter channel — the same rendering the
        /// shaped-collection filter uses, but qualified by a caller-supplied alias rather than the child
        /// table. The eager-load child fetch ANDs the returned SQL on and rebinds its @cp parameters.
        /// </summary>
        internal RenderedCollectionFilter RenderFilterAgainstAlias(LambdaExpression filter, string alias)
        {
            var before = SharedCompiledParams?.Count ?? 0;
            var sql = RenderNavigationFilter(filter, alias);

            IReadOnlyList<string> parameters = Array.Empty<string>();
            if (SharedCompiledParams != null && SharedCompiledParams.Count > before)
            {
                var minted = new List<string>(SharedCompiledParams.Count - before);
                for (var i = before; i < SharedCompiledParams.Count; i++)
                    minted.Add(SharedCompiledParams[i]);
                parameters = minted;
            }
            return new RenderedCollectionFilter(sql, parameters);
        }

        /// <summary>A lambda argument is a bare LambdaExpression (Enumerable overloads) or a Quote-wrapped one (Queryable overloads).</summary>
        private static LambdaExpression? UnwrapLambda(Expression arg)
            => arg as LambdaExpression
               ?? (arg is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression q } ? q : null);

        /// <summary>
        /// Returns the active <see cref="StringBuilder"/>, throwing if called outside
        /// a <see cref="Translate"/> invocation.
        /// </summary>
        private StringBuilder EnsureBuilder() =>
            _sb ?? throw new InvalidOperationException("Cannot visit expressions outside of a Translate() call.");

        /// <summary>
        /// Folds a <see cref="TimeSpan"/> expression to its runtime value when it
        /// is either a constant, a static/closure member, or a call to one of the
        /// side-effect-free <c>TimeSpan.From*</c> factories with a constant arg.
        /// Used by the 7-arg <c>new DateTimeOffset(...)</c> handler whose offset
        /// arg must be a compile-time constant. ExpressionValueExtractor refuses
        /// MethodCallExpression by design (RCE prevention); the TimeSpan factories
        /// have a fixed, audited surface so we admit them here explicitly.
        /// </summary>
        private static bool TryGetTimeSpanConstant(Expression e, out TimeSpan value)
        {
            value = default;
            if (ExpressionValueExtractor.TryGetConstantValue(e, out var box) && box is TimeSpan ts)
            {
                value = ts;
                return true;
            }
            if (e is MethodCallExpression mc
                && mc.Object == null
                && mc.Method.DeclaringType == typeof(TimeSpan)
                && mc.Arguments.Count == 1
                && ExpressionValueExtractor.TryGetConstantValue(mc.Arguments[0], out var argBox)
                && argBox != null)
            {
                try
                {
                    double d = Convert.ToDouble(argBox, System.Globalization.CultureInfo.InvariantCulture);
                    switch (mc.Method.Name)
                    {
                        case nameof(TimeSpan.FromDays):         value = TimeSpan.FromDays(d);         return true;
                        case nameof(TimeSpan.FromHours):        value = TimeSpan.FromHours(d);        return true;
                        case nameof(TimeSpan.FromMinutes):      value = TimeSpan.FromMinutes(d);      return true;
                        case nameof(TimeSpan.FromSeconds):      value = TimeSpan.FromSeconds(d);      return true;
                        case nameof(TimeSpan.FromMilliseconds): value = TimeSpan.FromMilliseconds(d); return true;
                        case nameof(TimeSpan.FromTicks):        value = new TimeSpan((long)d);        return true;
                    }
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }

        private static string GetTableName(Type type)
        {
            var tableAttribute = type
                .GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.TableAttribute), inherit: false)
                .Cast<System.ComponentModel.DataAnnotations.Schema.TableAttribute>()
                .FirstOrDefault();

            if (tableAttribute is null)
                return type.Name;

            return string.IsNullOrWhiteSpace(tableAttribute.Schema)
                ? tableAttribute.Name
                : tableAttribute.Schema + "." + tableAttribute.Name;
        }

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
    }
}
