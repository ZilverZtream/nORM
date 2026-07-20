using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// Builds the eager-load relation for a REFERENCE navigation (dependent → principal,
        /// e.g. <c>Order.Customer</c>). The <see cref="IncludeProcessor"/> operations are
        /// direction-symmetric, so loading principals reuses them with the roles inverted:
        /// the loaded level's "foreign key" is the principal's primary key (driving the
        /// WHERE/EXISTS predicates and the child-grouping getter) while the "principal key"
        /// read from the parent entities is the dependent's FK column. Returns null when the
        /// property is not a resolvable single-column reference navigation.
        /// </summary>
        private static TableMapping.Relation? TryBuildReferenceIncludeRelation(
            QueryTranslator t, TableMapping ownerMap, MemberInfo navMember)
        {
            if (navMember is not PropertyInfo prop)
                return null;
            var principalType = prop.PropertyType;
            if (principalType == typeof(string) || principalType == typeof(object) || !principalType.IsClass
                || typeof(System.Collections.IEnumerable).IsAssignableFrom(principalType))
                return null;

            TableMapping principalMap;
            try
            {
                principalMap = t.TrackMapping(principalType);
            }
            catch
            {
                return null;
            }
            if (principalMap.KeyColumns.Length != 1)
                return null;

            var fk = ExpressionToSqlVisitor.FindReferenceNavForeignKey(ownerMap, prop.Name, principalType, principalMap);
            if (fk == null)
                return null;

            return new TableMapping.Relation(prop, principalType, fk, principalMap.KeyColumns[0], false);
        }

        /// <summary>
        /// Extracts the navigation member from an Include/ThenInclude body, peeling an ordered / top-N
        /// <c>nav.OrderBy(k).Take(n)</c> (captured via <paramref name="ordering"/>) and a filtered
        /// <c>nav.Where(pred)</c> (captured via <paramref name="filter"/>). The ordering ops sit OUTSIDE the
        /// filter in the idiomatic form (<c>nav.Where(pred).OrderBy(k).Skip(s).Take(t)</c>), so they peel first.
        /// A Skip/Take with no OrderBy is rejected — it would make the top-N nondeterministic.
        /// </summary>
        private static MemberExpression PeelIncludeMember(Expression body, out LambdaExpression? filter, out SelectClauseVisitor.CollectionOrderingSpec? ordering)
        {
            filter = null;
            if (body is UnaryExpression convert)
                body = convert.Operand;
            // Peel an optional trailing materialization (nav.OrderBy(...).Take(n).ToList()) so both the bare and
            // the ToList/ToArray/AsEnumerable forms are accepted, matching the shaped-projection binding.
            if (body is MethodCallExpression materialize
                && (materialize.Method.Name == nameof(Enumerable.ToList)
                    || materialize.Method.Name == nameof(Enumerable.ToArray)
                    || materialize.Method.Name == nameof(Enumerable.AsEnumerable))
                && materialize.Arguments.Count == 1
                && (materialize.Method.DeclaringType == typeof(Enumerable) || materialize.Method.DeclaringType == typeof(Queryable)))
            {
                body = materialize.Arguments[0];
                if (body is UnaryExpression materializeConvert)
                    body = materializeConvert.Operand;
            }
            // Peel ordering ops (Take → Skip → ThenBy* → OrderBy, outer→inner) using the SAME helper the
            // shaped-projection path uses, so the two can never diverge in what they accept.
            ordering = SelectClauseVisitor.PeelCollectionOrdering(ref body);
            if (ordering != null && ordering.Keys.Count == 0)
                throw new NormUnsupportedFeatureException(
                    "Skip/Take on an included collection needs an OrderBy to make the result deterministic, e.g. " +
                    "Include(b => b.Posts.OrderBy(p => p.Id).Take(3)). A non-constant Take/Skip is also unsupported.");
            if (body is UnaryExpression orderConvert)
                body = orderConvert.Operand;
            if (body is MethodCallExpression whereCall
                && whereCall.Method.Name == nameof(Enumerable.Where)
                && whereCall.Arguments.Count == 2
                && (whereCall.Method.DeclaringType == typeof(Enumerable) || whereCall.Method.DeclaringType == typeof(Queryable))
                && StripQuotes(whereCall.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } predicate)
            {
                filter = predicate;
                body = whereCall.Arguments[0];
                if (body is UnaryExpression innerConvert)
                    body = innerConvert.Operand;
            }
            if (body is not MemberExpression member)
                throw new NormUnsupportedFeatureException(
                    "Include/ThenInclude supports a plain navigation, a filtered navigation ('nav.Where(pred)'), " +
                    "and an ordered / top-N navigation ('nav.OrderBy(k).Take(n)'). This shape is not supported.");
            return member;
        }

        /// <summary>
        /// Renders a filtered-Include predicate to SQL against the eager-load alias for
        /// <paramref name="levelIndex"/> (the IncludeProcessor uses <c>__inc{level}</c>), closure-safely
        /// through the shared compiled-parameter channel so captured variables re-bind per execution.
        /// Returns null when there is no filter.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private static IncludeFilter? RenderIncludeFilter(QueryTranslator t, TableMapping.Relation relation, LambdaExpression? filter, int levelIndex)
        {
            if (filter == null || t._ctx == null)
                return null;

            var childMapping = t._ctx.GetMapping(relation.DependentType);
            var scv = new SelectClauseVisitor(childMapping, new List<string>(), t._provider, outerAlias: null, ctx: t._ctx)
            {
                SharedParams = t._params,
                SharedCompiledParams = t._compiledParams,
                SharedParamConverters = t._paramConverters,
            };
            var alias = t._provider.Escape("__inc" + levelIndex.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var rendered = scv.RenderFilterAgainstAlias(filter, alias);
            return new IncludeFilter(rendered.Sql, rendered.Parameters);
        }

        /// <summary>
        /// Renders an ordered / top-N Include (<c>Include(b => b.Posts.OrderByDescending(p => p.Date).Take(3))</c>)
        /// to an <see cref="IncludeOrdering"/>: the ORDER BY keys rendered against this level's <c>__inc{level}</c>
        /// alias plus the Take/Skip caps. Returns null when there is no ordering. Ordering keys reuse the same
        /// simple-key / value-converter / order-preserving-coercion rules as shaped-collection projections.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private static IncludeOrdering? RenderIncludeOrdering(QueryTranslator t, TableMapping.Relation relation, SelectClauseVisitor.CollectionOrderingSpec? ordering, int levelIndex)
        {
            if (ordering == null || ordering.Keys.Count == 0 || t._ctx == null)
                return null;

            var childMapping = t._ctx.GetMapping(relation.DependentType);
            var scv = new SelectClauseVisitor(childMapping, new List<string>(), t._provider, outerAlias: null, ctx: t._ctx)
            {
                SharedParams = t._params,
                SharedCompiledParams = t._compiledParams,
                SharedParamConverters = t._paramConverters,
            };
            var alias = t._provider.Escape("__inc" + levelIndex.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var orderingSql = scv.RenderOrderingKeys(ordering, alias);
            return new IncludeOrdering(orderingSql, ordering.Take, ordering.Skip);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class IncludeTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Processes an <c>Include</c> call, registering the requested navigation path for eager loading.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>Include</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Support both instance calls (source.Include(lambda)) and
                // static/extension calls (Include(source, lambda)).
                Expression source;
                Expression? rawLambda;
                if (node.Object != null)
                {
                    source = node.Object;
                    rawLambda = node.Arguments.Count > 0 ? node.Arguments[0] : null;
                }
                else
                {
                    source = node.Arguments[0];
                    rawLambda = node.Arguments.Count > 1 ? node.Arguments[1] : null;
                }

                // Visit source FIRST to establish _mapping before the Relations lookup.
                var visited = t.Visit(source);

                if (rawLambda != null)
                {
                    var includeLambda = rawLambda is UnaryExpression qu ? qu.Operand as LambdaExpression : rawLambda as LambdaExpression;
                    if (includeLambda != null)
                    {
                        var member = PeelIncludeMember(includeLambda.Body, out var includeFilter, out var includeOrdering);
                        var propName = member.Member.Name;
                        if (t._mapping != null && t._mapping.Relations.TryGetValue(propName, out var relation))
                        {
                            var plan = new IncludePlan(new List<TableMapping.Relation> { relation });
                            plan.Filters.Add(RenderIncludeFilter(t, relation, includeFilter, 0));
                            plan.Orderings.Add(RenderIncludeOrdering(t, relation, includeOrdering, 0));
                            t._includes.Add(plan);
                            t.TrackMapping(relation.DependentType);
                        }
                        else if (t._mapping != null)
                        {
                            // Check if this is a many-to-many navigation property
                            var jtm = t._mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == propName);
                            if (jtm != null)
                            {
                                if (includeFilter != null)
                                    throw new NormUnsupportedFeatureException(
                                        $"A filtered Include on the many-to-many navigation '{propName}' is not supported. " +
                                        "Filter after materialization, or model the relationship as an explicit join entity.");
                                if (includeOrdering != null)
                                    throw new NormUnsupportedFeatureException(
                                        $"An ordered / top-N Include on the many-to-many navigation '{propName}' is not supported. " +
                                        "Order after materialization instead.");
                                t._m2mIncludes.Add(new M2MIncludePlan(jtm));
                                t.TrackMapping(jtm.RightType);
                            }
                            else if (TryBuildReferenceIncludeRelation(t, t._mapping, member.Member) is { } refRelation)
                            {
                                if (includeOrdering != null)
                                    throw new NormUnsupportedFeatureException(
                                        $"OrderBy/Take/Skip on the reference navigation '{propName}' is meaningless (it loads a single " +
                                        "related entity) and is not supported.");
                                var plan = new IncludePlan(new List<TableMapping.Relation> { refRelation });
                                plan.Filters.Add(RenderIncludeFilter(t, refRelation, includeFilter, 0));
                                plan.Orderings.Add(null);
                                t._includes.Add(plan);
                            }
                        }
                    }
                }
                return visited;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ThenIncludeTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Extends the most recently registered include path with an additional navigation property.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>ThenInclude</c>.</param>
            /// <returns>The translated expression representing the parent include.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var parentExpression = t.Visit(node.Arguments[0]);
                if (node.Arguments.Count > 1)
                {
                    // Arguments[1] is typically a quoted lambda (UnaryExpression{Quote}); strip quotes.
                    var thenLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
                    if (thenLambda != null)
                    {
                        var member = PeelIncludeMember(thenLambda.Body, out var includeFilter, out var includeOrdering);
                        var propName = member.Member.Name;
                        if (t._includes.Count > 0)
                        {
                            var lastInclude = t._includes[^1];
                            var lastRelation = lastInclude.Path.Last();
                            var parentMap = t.TrackMapping(lastRelation.DependentType);
                            var levelIndex = lastInclude.Path.Count;
                            if (parentMap.Relations.TryGetValue(propName, out var relation))
                            {
                                lastInclude.Path.Add(relation);
                                lastInclude.Filters.Add(RenderIncludeFilter(t, relation, includeFilter, levelIndex));
                                lastInclude.Orderings.Add(RenderIncludeOrdering(t, relation, includeOrdering, levelIndex));
                                t.TrackMapping(relation.DependentType);
                            }
                            else if (TryBuildReferenceIncludeRelation(t, parentMap, member.Member) is { } refRelation)
                            {
                                if (includeOrdering != null)
                                    throw new NormUnsupportedFeatureException(
                                        $"OrderBy/Take/Skip on the reference navigation '{propName}' is meaningless (it loads a single " +
                                        "related entity) and is not supported.");
                                lastInclude.Path.Add(refRelation);
                                lastInclude.Filters.Add(RenderIncludeFilter(t, refRelation, includeFilter, levelIndex));
                                lastInclude.Orderings.Add(null);
                            }
                        }
                    }
                }
                return parentExpression;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AsNoTrackingTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Marks the query so that returned entities are not tracked by the context.
            /// </summary>
            /// <param name="t">The translator applying the option.</param>
            /// <param name="node">The method call expression for <c>AsNoTracking</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Last-wins with AsTracking: the outermost (last-written, first-visited) operator locks the
                // decision. Guard is a no-op for the common single-AsNoTracking query.
                if (!t._trackingDecided)
                {
                    t._noTracking = true;
                    t._trackingDecided = true;
                }
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AsNoTrackingWithIdentityResolutionTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Marks the query untracked (as <c>AsNoTracking</c> does, last-wins with <c>AsTracking</c>) and,
            /// additionally, flags identity resolution so a root key repeated in one result set collapses to a
            /// single instance in the materialize loop.
            /// </summary>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (!t._trackingDecided)
                {
                    t._noTracking = true;
                    t._identityResolution = true;
                    t._trackingDecided = true;
                }
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AsTrackingTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Forces change-tracking ON for this query, overriding a <c>DefaultTrackingBehavior.NoTracking</c>
            /// context default (EF Core's <c>AsTracking</c>). Passes through otherwise. Composes last-wins with
            /// <c>AsNoTracking</c> via the shared tracking-decision guard.
            /// </summary>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Only raise the force-tracking flag (which overrides the context NoTracking default); never
                // clear _noTracking. AsOf/Include set _noTracking=true as a hard safety invariant, and leaving
                // it untouched lets it win via trackable=false regardless of AsTracking's composition order.
                if (!t._trackingDecided)
                {
                    t._forceTracking = true;
                    t._trackingDecided = true;
                }
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class TagWithTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Captures the tag from <c>TagWith(source, tag)</c> and prepends it to the generated SQL as a
            /// line comment (see <see cref="ApplyQueryTags"/>). Pass-through otherwise.
            /// </summary>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments.Count >= 2 && TryGetConstantValue(node.Arguments[1], out var tagValue) && tagValue is string tag && tag.Length > 0)
                    (t._queryTags ??= new List<string>()).Add(tag);
                return t.Visit(node.Arguments[0]);
            }
        }

        /// <summary>
        /// Prepends any TagWith(...) comments to <paramref name="sql"/> as SQL line comments. Line comments
        /// consume to end of line, so each line of every tag is prefixed with <c>-- </c> (and embedded
        /// carriage returns/newlines are normalized) — a tag can never break out of the comment into SQL.
        /// </summary>
        private string ApplyQueryTags(string sql)
        {
            if (_queryTags == null || _queryTags.Count == 0)
                return sql;
            var sb = new System.Text.StringBuilder();
            foreach (var tag in _queryTags)
            {
                foreach (var line in tag.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n'))
                    sb.Append("-- ").Append(line).Append('\n');
            }
            sb.Append('\n').Append(sql);
            return sb.ToString();
        }

        private sealed class IgnoreQueryFiltersTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Strips the <c>IgnoreQueryFilters</c> marker. Whether user global filters are
            /// suppressed was already decided pre-translation in
            /// <c>NormQueryProvider.ApplyGlobalFilters</c>, so nothing to do here but pass through.
            /// </summary>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class CastOrOfTypeTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var source = node.Arguments[0];
                var sourceElement = GetElementType(source);
                var targetElement = node.Method.GetGenericArguments().FirstOrDefault();
                if (targetElement == null)
                {
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} requires a generic type argument.");
                }
                // Cast / OfType collapse to an identity pass-through at the SQL layer when
                // the target type matches the source element type (or is a reference-type
                // base that the runtime cast will satisfy on materialization).
                if (targetElement == sourceElement ||
                    (!targetElement.IsValueType && targetElement.IsAssignableFrom(sourceElement)))
                {
                    return t.Visit(source);
                }
                // OfType<DerivedType>() on a TPH hierarchy. When the derived type is the
                // query's element type (no downstream projection), TranslationBuilder.Setup()
                // already injected the discriminator WHERE predicate — only _rootType needs
                // restoring after VisitConstant resets it to the base. With a downstream
                // Select, Setup saw the PROJECTION's element type instead and injected
                // nothing — without the predicate here, OfType<Dog>().Select(...) would
                // silently return every subtype's rows.
                if (targetElement.IsSubclassOf(sourceElement)
                    && targetElement.GetCustomAttribute<DiscriminatorValueAttribute>() is { } discriminatorValue)
                {
                    var baseMapping = t.TrackMapping(sourceElement);
                    if (baseMapping.DiscriminatorColumn != null)
                    {
                        var setupHandledDiscriminator = t._rootType == targetElement;
                        t.Visit(source);
                        t._rootType = targetElement;
                        if (!setupHandledDiscriminator)
                        {
                            t._mapping = baseMapping;
                            var paramName = t._ctx!.RawProvider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
                            t._params[paramName] = discriminatorValue.Value;
                            if (t._where.Length > 0)
                                t._where.Append(" AND ");
                            t._where.Append('(').Append(baseMapping.DiscriminatorColumn!.EscCol)
                                .Append(" = ").Append(paramName).Append(')');
                        }
                        return source;
                    }
                }
                throw new NormUnsupportedFeatureException(
                    $"{node.Method.Name}<{targetElement.Name}>() on IQueryable<{sourceElement.Name}> cannot be translated to SQL: " +
                    $"{targetElement.Name} is not a subtype of {sourceElement.Name} with a [DiscriminatorValue] attribute, " +
                    "or the base type has no [DiscriminatorColumn]. Project explicitly with Select(...) instead.");
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AsSplitQueryTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Indicates that related data should be loaded using multiple queries instead of a single join.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>AsSplitQuery</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                t._splitQuery = true;
                var source = node.Object ?? node.Arguments[0];
                return t.Visit(source);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AsOfTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Applies temporal querying by translating the <c>AsOf</c> operation into a timestamp filter.
            /// </summary>
            /// <param name="t">The translator managing the temporal context.</param>
            /// <param name="node">The method call expression for <c>AsOf</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Temporal snapshots represent historical state, not the current
                // live row. Tracking them by primary key can alias the snapshot to
                // an already-tracked current entity and silently return current state.
                t._noTracking = true;
                var timeTravelArg = node.Arguments[1];
                if (QueryTranslator.TryGetConstantValue(timeTravelArg, out var value))
                {
                    if (value is DateTime dt)
                    {
                        t._asOfTimestamp = NormalizeAsOfTimestamp(dt);
                    }
                    else if (value is string tagName)
                    {
                        // Reuse the pre-scan's resolution: resolving the tag twice could
                        // observe a concurrent tag rewrite and produce two timestamps for
                        // one statement.
                        t._asOfTimestamp = t._asOfTagResolution is { } cached && cached.Tag == tagName
                            ? cached.Ts
                            : t.GetTimestampForTagAsync(tagName).GetAwaiter().GetResult();
                    }
                }
                else
                {
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, ".AsOf() requires a constant DateTime or string tag."));
                }
                t.BeginTemporalTableSourceScope(t._asOfTimestamp!.Value);
                return t.Visit(node.Arguments[0]);
            }
        }
    }
}
