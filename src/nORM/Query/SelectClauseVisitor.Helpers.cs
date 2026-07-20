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
        private static bool IsSequenceOp(MethodCallExpression call)
            => call.Method.DeclaringType == typeof(Enumerable) || call.Method.DeclaringType == typeof(Queryable);

        /// <summary>Peels a terminal <c>ToList/ToArray/AsEnumerable()</c> over a single source, advancing
        /// <paramref name="current"/> to the source.</summary>
        private static void PeelTerminal(ref Expression current)
        {
            if (current is MethodCallExpression term
                && term.Arguments.Count == 1
                && IsSequenceOp(term)
                && term.Method.Name is "ToList" or "ToArray" or "AsEnumerable")
                current = term.Arguments[0];
        }

        /// <summary>Peels a single-lambda sequence op <c>source.Method(lambda)</c> named <paramref name="method"/>,
        /// returning the unwrapped one-parameter lambda and advancing <paramref name="current"/>.</summary>
        private static bool TryPeelLambdaOp(ref Expression current, string method, out LambdaExpression lambda)
        {
            lambda = null!;
            if (current is MethodCallExpression call
                && call.Arguments.Count == 2
                && IsSequenceOp(call)
                && call.Method.Name == method
                && UnwrapLambda(call.Arguments[1]) is { Parameters.Count: 1 } l)
            {
                lambda = l;
                current = call.Arguments[0];
                return true;
            }
            return false;
        }

        /// <summary>Peels a keyed ordering op (OrderBy/ThenBy and their Descending variants), returning the key
        /// lambda + direction and advancing <paramref name="current"/>.</summary>
        private static bool TryPeelKeyedOp(ref Expression current, string ascending, string descending, out LambdaExpression key, out bool desc)
        {
            key = null!;
            desc = false;
            if (current is MethodCallExpression call
                && call.Arguments.Count == 2
                && IsSequenceOp(call)
                && (call.Method.Name == ascending || call.Method.Name == descending)
                && UnwrapLambda(call.Arguments[1]) is { Parameters.Count: 1 } l)
            {
                key = l;
                desc = call.Method.Name == descending;
                current = call.Arguments[0];
                return true;
            }
            return false;
        }

        /// <summary>Peels a CONSTANT <c>Take(n)</c>/<c>Skip(n)</c> named <paramref name="method"/>. Non-constant
        /// counts are left in place so the binding falls through to the fail-loud path.</summary>
        private static bool TryPeelConstantCount(ref Expression current, string method, out int count)
        {
            count = 0;
            if (current is MethodCallExpression call
                && call.Arguments.Count == 2
                && IsSequenceOp(call)
                && call.Method.Name == method
                && QueryTranslator.TryGetConstantValue(call.Arguments[1], out var v) && v is int n)
            {
                count = n;
                current = call.Arguments[0];
                return true;
            }
            return false;
        }

        /// <summary>Peels the ordering ops of a shaped collection projection (Take → Skip → ThenBy* → OrderBy,
        /// outer→inner) into a spec. The final key list is [OrderBy, ThenBy1, ThenBy2, ...]. Returns null when
        /// there is no ordering. Shared with the eager-load Include ordering path (Include(b => b.Posts
        /// .OrderByDescending(p => p.Date).Take(3))) so the two never diverge in what they peel.</summary>
        internal static CollectionOrderingSpec? PeelCollectionOrdering(ref Expression current)
        {
            CollectionOrderingSpec? spec = null;
            if (TryPeelConstantCount(ref current, nameof(Enumerable.Take), out var take))
                (spec ??= new CollectionOrderingSpec()).Take = take;
            if (TryPeelConstantCount(ref current, nameof(Enumerable.Skip), out var skip))
                (spec ??= new CollectionOrderingSpec()).Skip = skip;

            List<OrderingKey>? thenBys = null;
            while (TryPeelKeyedOp(ref current, nameof(Enumerable.ThenBy), nameof(Enumerable.ThenByDescending), out var tk, out var td))
                (thenBys ??= new List<OrderingKey>()).Add(new OrderingKey(tk, td));

            if (TryPeelKeyedOp(ref current, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending), out var ok, out var od))
            {
                (spec ??= new CollectionOrderingSpec()).Keys.Add(new OrderingKey(ok, od));
                if (thenBys != null)
                {
                    thenBys.Reverse();   // peeled outermost-first; application order is OrderBy then these
                    spec.Keys.AddRange(thenBys);
                }
            }
            return spec;
        }

        private bool TryMatchDetectedCollection(Expression expr, out PropertyInfo navProperty, out LambdaExpression? filter, out LambdaExpression? projection, out CollectionOrderingSpec? ordering)
        {
            navProperty = null!;
            filter = null;
            projection = null;
            ordering = null;
            var current = expr;

            PeelTerminal(ref current);

            // Select(elementProjection) — peeled before ordering (LINQ order is nav.Where.OrderBy.Take.Select).
            // Only a projection reading solely its own element is admitted (no closure/outer-row references the
            // child materializer can't supply); anything else falls through to the fail-loud client-eval path.
            if (TryPeelLambdaOp(ref current, nameof(Enumerable.Select), out var projLambda))
            {
                if (!IsSafeChildProjection(projLambda))
                    return false;
                projection = projLambda;
            }

            var spec = PeelCollectionOrdering(ref current);

            // Where(predicate) — the innermost op.
            if (TryPeelLambdaOp(ref current, nameof(Enumerable.Where), out var predLambda))
                filter = predLambda;

            if (!IsNavigationCollection(current, out navProperty))
                return false;

            // Skip/Take without an OrderBy makes the top-N nondeterministic (undefined row order) — fail loud.
            if (spec != null && spec.Keys.Count == 0)
                throw new NormUnsupportedFeatureException(
                    "Skip/Take on a projected collection needs an OrderBy to make the result deterministic, " +
                    "e.g. 'o.Lines.OrderBy(l => l.Id).Take(3).ToList()'.");

            ordering = spec;
            return true;
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

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
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
            if (_ctx == null)
                return null;

            // The element filter renders against the CHILD element's table: the dependent table for a relation,
            // the related (right) table for a many-to-many, or the owned table for an owned collection. Without
            // resolving the owned/m2m child table the filter would be silently dropped (a correctness bug).
            string childAlias;
            if (_mapping.Relations.TryGetValue(navProperty.Name, out var relation))
                childAlias = _ctx.GetMapping(relation.DependentType).EscTable;
            else if (_mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == navProperty.Name) is { } jtm)
                childAlias = _ctx.GetMapping(jtm.RightType).EscTable;
            else if (_mapping.OwnedCollections.FirstOrDefault(o => o.NavigationProperty.Name == navProperty.Name) is { } ownedMap)
                childAlias = _ctx.GetMapping(ownedMap.OwnedType).EscTable;
            else
                return null;
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
        /// Renders a shaped collection binding's ORDER BY keys to SQL against the child table (an ordered /
        /// top-N projection: <c>o.Lines.OrderByDescending(l =&gt; l.Date).Take(3).ToList()</c>) plus the row cap.
        /// Keys resolve through <see cref="RenderFilterSide"/> so fluent <c>HasColumnName</c> is honoured, with
        /// order-preserving coercions for decimal/TimeSpan/DateTimeOffset TEXT storage. Returns null when the
        /// collection has no resolvable child table. Computed keys and value-converter columns fail loud —
        /// ordering by the stored representation of a non-order-preserving converter would sort wrong.
        /// </summary>
        private RenderedCollectionOrdering? RenderShapedCollectionOrdering(PropertyInfo navProperty, CollectionOrderingSpec spec)
        {
            if (_ctx == null)
                return null;

            string childAlias;
            if (_mapping.Relations.TryGetValue(navProperty.Name, out var relation))
                childAlias = _ctx.GetMapping(relation.DependentType).EscTable;
            else if (_mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == navProperty.Name) is { } jtm)
                childAlias = _ctx.GetMapping(jtm.RightType).EscTable;
            else if (_mapping.OwnedCollections.FirstOrDefault(o => o.NavigationProperty.Name == navProperty.Name) is { } ownedMap)
                childAlias = _ctx.GetMapping(ownedMap.OwnedType).EscTable;
            else
                return null;

            return new RenderedCollectionOrdering(RenderOrderingKeys(spec, childAlias), spec.Take, spec.Skip);
        }

        /// <summary>
        /// Renders a collection ordering spec's keys to a comma-separated <c>ORDER BY</c> body (no keyword) against
        /// <paramref name="alias"/>. Shared by the shaped-projection split-query path and the eager-load Include
        /// path so both apply the same simple-key / value-converter / order-preserving-coercion rules. Only a
        /// simple property key on the element parameter is supported; computed/composite keys and value-converter
        /// columns fail loud (the stored form may not preserve the model order).
        /// </summary>
        internal string RenderOrderingKeys(CollectionOrderingSpec spec, string alias)
        {
            var parts = new List<string>(spec.Keys.Count);
            foreach (var key in spec.Keys)
            {
                var body = key.KeySelector.Body;
                while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                    body = u.Operand;
                if (body is not MemberExpression me || me.Expression != key.KeySelector.Parameters[0])
                    throw new NormUnsupportedFeatureException(
                        "Only a simple property ordering key is supported on an ordered collection, e.g. " +
                        "'o.Lines.OrderBy(l => l.Date)'. Computed or composite ordering keys are not supported.");
                if (me.Member.DeclaringType != null && _ctx != null
                    && _ctx.GetMapping(me.Member.DeclaringType).ColumnsByName.TryGetValue(me.Member.Name, out var col)
                    && col.Converter != null)
                    throw new NormUnsupportedFeatureException(
                        "Ordering an included collection by a value-converter column is not supported — the stored " +
                        "representation may not preserve the model order. Order after materialization instead.");

                var keySql = RenderFilterSide(body, key.KeySelector.Parameters[0], alias);
                keySql = CoerceCollectionOrderKeySql(keySql, body.Type);
                parts.Add(keySql + (key.Descending ? " DESC" : " ASC"));
            }
            return string.Join(", ", parts);
        }

        /// <summary>Applies the provider's order-preserving coercion for types stored as TEXT on SQLite
        /// (decimal/TimeSpan/DateTimeOffset), so an ORDER BY key sorts numerically/by-instant, not lexically.
        /// Plain DateTime is deliberately not coerced (its stored form already sorts chronologically).</summary>
        private string CoerceCollectionOrderKeySql(string sql, Type type)
        {
            var t = Nullable.GetUnderlyingType(type) ?? type;
            if (t == typeof(decimal)) return _provider.OrderByDecimalKeySql(sql);
            if (t == typeof(TimeSpan)) return _provider.NormalizeTimeSpanForCompare(sql);
            if (t == typeof(DateTimeOffset)) return _provider.NormalizeDateTimeOffsetForCompare(sql);
            return sql;
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

        /// <summary>
        /// Emits the SQL for a string <c>StartsWith</c>/<c>EndsWith</c>/<c>Contains</c> match against a
        /// pre-rendered receiver with a CONSTANT pattern, applying the provider's case-sensitivity handling
        /// (the ordinal byte-exact bypass, or LIKE with wildcard escaping). Shared by projection rendering
        /// (<see cref="VisitMethodCall"/>) and the navigation-filter grammar so both produce identical
        /// string-match SQL — the case-sensitivity rules here are the ones the Where/projection paths were
        /// fixed to (Ordinal by default; SQLite bypasses LIKE which folds ASCII case).
        /// </summary>
        internal string EmitStringMatch(string receiverSql, string patternStr, string methodName, bool ignoreCase)
        {
            if (!ignoreCase)
            {
                if (_provider.UsesOrdinalStringMatchBypass)
                {
                    var kind = methodName switch
                    {
                        nameof(string.StartsWith) => nORM.Providers.OrdinalStringMatch.StartsWith,
                        nameof(string.EndsWith) => nORM.Providers.OrdinalStringMatch.EndsWith,
                        _ => nORM.Providers.OrdinalStringMatch.Contains,
                    };
                    var patternLiteral = $"'{patternStr.Replace("'", "''")}'";
                    return _provider.GetOrdinalStringMatchSql(receiverSql, patternLiteral, kind);
                }
                receiverSql = _provider.ForceCaseSensitiveStringComparison(receiverSql);
            }
            var escapeChar = NormValidator.ValidateLikeEscapeChar(_provider.LikeEscapeChar);
            var effectivePattern = ignoreCase ? patternStr.ToLowerInvariant() : patternStr;
            var escaped = _provider.EscapeLikePattern(effectivePattern);
            var wrapped = methodName switch
            {
                nameof(string.StartsWith) => $"{escaped}%",
                nameof(string.EndsWith) => $"%{escaped}",
                _ => $"%{escaped}%",
            };
            var lhs = ignoreCase ? $"LOWER({receiverSql})" : receiverSql;
            return $"({lhs} LIKE '{wrapped.Replace("'", "''")}' ESCAPE '{escapeChar}')";
        }
    }
}
