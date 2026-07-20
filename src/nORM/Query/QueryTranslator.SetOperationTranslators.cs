using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class DistinctTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Marks the query results as distinct by setting the <c>DISTINCT</c> flag.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>Distinct</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Distinct after a reshape or group-join result runs in memory over the
                // assembled rows with LINQ-to-Objects equality (an appended duplicate
                // must dedup too; group-join results only exist after grouping), so SQL
                // DISTINCT — which sees only flat server rows — must not be set.
                if (SourceHasClientTailReshape(node.Arguments[0])
                    || SourceHasGroupJoinResultTail(node.Arguments[0])
                    || SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    var reshapedSource = t.Visit(node.Arguments[0]);
                    if (t.TryAppendClientSequenceOperator(node))
                        return reshapedSource;
                    ThrowIfClientTailReshapePending(t, node.Method.Name);
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} after a client-materialized sequence operator has no in-memory equivalent overload.");
                }
                // Distinct after a Take/Skip-windowed source — wrap as derived table so
                // DISTINCT runs over only the windowed rows. Sister of the post-Take/Skip
                // family fixes. Includes the common `Take(N).Select(proj).Distinct()`
                // shape (Select sits between Take and Distinct) — for those we need to
                // translate the full source chain (including the projection) as the
                // sub-plan and apply DISTINCT on the outer wrap.
                if (QueryTranslator.SourceHasTakeOrSkip(node.Arguments[0]))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }

                // Distinct after a set operation (Concat -> UNION ALL keeps duplicates): the compound fills
                // _sql, and the DISTINCT keyword is only rendered when _sql is empty, so a bare `t._isDistinct`
                // never reaches the SQL and duplicates survive. Wrap the compound as a derived table with
                // DISTINCT applied on the outer SELECT (mirror of the Where-after-set-op wrap).
                if (IsSetOperationCall(node.Arguments[0]))
                {
                    t.Visit(node.Arguments[0]);
                    if (t._sql.Length > 0)
                    {
                        var innerSetSql = t._sql.ToString();
                        t._sql.Clear();
                        var setWrapAlias = t.EscapeAlias("__dset" + t._joinCounter++);
                        t._sql.AppendFragment("SELECT DISTINCT * FROM (").Append(innerSetSql)
                              .AppendFragment(") AS ").Append(setWrapAlias);
                        t._isDistinct = true;
                        t._orderBy.Clear();
                        return node.Arguments[0];
                    }
                }

                // Set _isDistinct BEFORE visiting the source. JoinBuilder.BuildJoinClauseInto
                // captures `distinct: _isDistinct` at join-emit time (e97b814), which runs
                // INSIDE the Visit below — if we set the flag afterward, the join SQL is
                // built without DISTINCT and the test (Join.Distinct) silently returns
                // duplicates.
                t._isDistinct = true;
                var source = t.Visit(node.Arguments[0]);
                // LINQ Distinct does not preserve the source ordering, and a surviving
                // ORDER BY over non-projected columns is invalid SQL under DISTINCT on
                // MySQL/Postgres ("ORDER BY not in SELECT list"). Any OrderBy applied
                // AFTER Distinct is visited later and re-populates the list.
                t._orderBy.Clear();
                return source;
            }

            /// <summary>
            /// Windowed-source branch — wraps the windowed source as a derived
            /// table and applies DISTINCT on the outer SELECT. Lives in its own
            /// method so the common-path Translate stays stack-frame lean.
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanD = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapD);
                t._mapping = subMapD;
                t.MergeSubPlanParameters(subPlanD);
                var winAliasD = t.EscapeAlias("__wdis" + t._joinCounter++);

                // C# Distinct() on strings is ordinal, but DISTINCT on CI-collation
                // providers (MySQL, SQL Server) folds case and silently collapses
                // "abc" and "ABC". Mirror of the flat-path rewrite in
                // TranslationBuilder: for a windowed scalar string projection whose
                // sub-select emits one plain column, group the wrap by
                // (column, binary column) instead of emitting DISTINCT — grouping by
                // the composite is byte-wise distinctness while the SELECT keeps
                // returning the plain string.
                // The sub-context consumed the projection that defines the element
                // shape; without re-installing it the outer plan materializes the
                // entity mapping's columns into the projected type's constructor and
                // fails ("no suitable constructor" for any anonymous shape). The
                // parameter binds to the wrap alias so any later projection-driven
                // SELECT rewrite emits wrap-scope references.
                var shapeSelect = FindShapeDefiningSelect(node.Arguments[0]);
                LambdaExpression? shapeLambda = null;
                if (shapeSelect != null
                    && StripQuotes(shapeSelect.Arguments[1]) is LambdaExpression shapeProj)
                {
                    shapeLambda = shapeProj;
                    if (!t._correlatedParams.ContainsKey(shapeProj.Parameters[0]))
                        t._correlatedParams[shapeProj.Parameters[0]] = (subMapD, winAliasD);
                    t._projection = shapeProj;
                }

                var elementArgs = node.Arguments[0].Type.IsGenericType
                    ? node.Arguments[0].Type.GetGenericArguments()
                    : Type.EmptyTypes;
                var elementType = elementArgs.Length > 0
                    ? Nullable.GetUnderlyingType(elementArgs[0]) ?? elementArgs[0]
                    : null;
                if (elementType == typeof(string)
                    && t._provider.DefaultStringEqualityIsCaseInsensitive
                    && TryGetSingleColumnSelectIdentifier(subPlanD.Sql, out var distinctCol))
                {
                    t._sql.AppendFragment("SELECT ").Append(distinctCol)
                          .AppendFragment(" FROM (").Append(subPlanD.Sql).AppendFragment(") AS ").Append(winAliasD);
                    t._groupBy.Add(distinctCol);
                    t._groupByOrdinalExtras.Add(t._provider.ForceCaseSensitiveStringComparison(distinctCol));
                    t._isDistinct = true;
                    return node;
                }

                // Scalar DateTimeOffset: .NET dedups by INSTANT; offset-suffixed TEXT
                // storage dedups by representation. Mirror of the flat-path rewrite —
                // select and group the canonical form (it still materializes as a
                // DateTimeOffset). Identity-hook providers skip this branch.
                if (elementType == typeof(DateTimeOffset)
                    && TryGetSingleColumnSelectIdentifier(subPlanD.Sql, out var dtoCol)
                    && t._provider.CanonicalizeDateTimeOffsetGroupKey(dtoCol) is { } canonicalDto)
                {
                    t._sql.AppendFragment("SELECT ").Append(canonicalDto)
                          .AppendFragment(" FROM (").Append(subPlanD.Sql).AppendFragment(") AS ").Append(winAliasD);
                    t._groupBy.Add(canonicalDto);
                    t._isDistinct = true;
                    return node;
                }

                // Anonymous / record shapes: DISTINCT dedups string members by the CI
                // collation and DateTimeOffset members by representation. The
                // sub-select aliases each output by the projection MEMBER name
                // (computed members included), so the wrap selects and groups those
                // aliases with type-appropriate extras per member.
                if (shapeLambda?.Body is NewExpression shapeNew
                    && shapeNew.Members != null
                    && shapeNew.Members.Count == shapeNew.Arguments.Count
                    && shapeNew.Arguments.Count > 0
                    && shapeNew.Arguments.Any(a =>
                        NeedsDistinctKeyTreatment(t, Nullable.GetUnderlyingType(a.Type) ?? a.Type)))
                {
                    t._sql.AppendFragment("SELECT ");
                    for (var ci = 0; ci < shapeNew.Members.Count; ci++)
                    {
                        var outName = t._provider.Escape(shapeNew.Members[ci].Name);
                        var memberType = Nullable.GetUnderlyingType(shapeNew.Arguments[ci].Type) ?? shapeNew.Arguments[ci].Type;
                        if (ci > 0) t._sql.AppendFragment(", ");
                        if (memberType == typeof(DateTimeOffset)
                            && t._provider.CanonicalizeDateTimeOffsetGroupKey(outName) is { } canonicalMember)
                        {
                            t._sql.Append(canonicalMember).AppendFragment(" AS ").Append(outName);
                        }
                        else
                        {
                            t._sql.Append(outName);
                        }
                    }
                    t._sql.AppendFragment(" FROM (").Append(subPlanD.Sql).AppendFragment(") AS ").Append(winAliasD);
                    for (var ci = 0; ci < shapeNew.Members.Count; ci++)
                    {
                        var outName = t._provider.Escape(shapeNew.Members[ci].Name);
                        var memberType = Nullable.GetUnderlyingType(shapeNew.Arguments[ci].Type) ?? shapeNew.Arguments[ci].Type;
                        if (memberType == typeof(DateTimeOffset)
                            && t._provider.CanonicalizeDateTimeOffsetGroupKey(outName) is { } canonicalMember)
                        {
                            t._groupBy.Add(canonicalMember);
                            continue;
                        }
                        t._groupBy.Add(outName);
                        if (memberType == typeof(string) && t._provider.DefaultStringEqualityIsCaseInsensitive)
                            t._groupByOrdinalExtras.Add(t._provider.ForceCaseSensitiveStringComparison(outName));
                    }
                    t._isDistinct = true;
                    return node;
                }

                t._sql.AppendFragment("SELECT DISTINCT * FROM (").Append(subPlanD.Sql).AppendFragment(") AS ").Append(winAliasD);
                t._isDistinct = true;
                return node;
            }

            /// <summary>
            /// True when a projected member of this type dedups incorrectly under a
            /// plain DISTINCT on the current provider (CI-collated strings; text-stored
            /// DateTimeOffset instants).
            /// </summary>
            private static bool NeedsDistinctKeyTreatment(QueryTranslator t, Type memberType)
                => (memberType == typeof(string) && t._provider.DefaultStringEqualityIsCaseInsensitive)
                   || (memberType == typeof(DateTimeOffset) && t._provider.CanonicalizeDateTimeOffsetGroupKey("x") != null);

            /// <summary>
            /// Walks down through element-shape-preserving operators (Where, OrderBy,
            /// ThenBy, Skip, Take) to the Select call whose lambda produced the
            /// windowed source's element shape. Returns null when the chain bottoms
            /// out at the entity root (no reshaping Select).
            /// </summary>
            private static MethodCallExpression? FindShapeDefiningSelect(Expression source)
            {
                var current = source;
                while (current is MethodCallExpression call && call.Arguments.Count > 0)
                {
                    if (call.Method.Name == nameof(Queryable.Select))
                        return call;
                    if (call.Method.Name is nameof(Queryable.Where)
                        or nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                        or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending)
                        or nameof(Queryable.Skip) or nameof(Queryable.Take))
                    {
                        current = call.Arguments[0];
                        continue;
                    }
                    return null;
                }
                return null;
            }


            /// <summary>
            /// Matches a sub-select whose output list is exactly one plain
            /// (optionally table-qualified) escaped column identifier and returns
            /// the bare identifier for use in the wrapping scope. Expression
            /// selects, aliases, and multi-column lists return false — those fall
            /// back to the DISTINCT keyword.
            /// </summary>
            private static bool TryGetSingleColumnSelectIdentifier(string sql, out string column)
            {
                column = string.Empty;
                if (!sql.StartsWith("SELECT ", StringComparison.Ordinal)) return false;
                var fromIdx = sql.IndexOf(" FROM ", StringComparison.Ordinal);
                if (fromIdx < 0) return false;
                var list = sql.Substring(7, fromIdx - 7).Trim();
                if (list.Length is 0 or > 128) return false;
                if (list.Contains(',') || list.Contains('(') || list.Contains(' ')) return false;
                var lastDot = list.LastIndexOf('.');
                if (lastDot >= 0) list = list[(lastDot + 1)..];
                if (list.Length < 3) return false;
                var first = list[0];
                var last = list[^1];
                var escaped = (first == '"' && last == '"')
                    || (first == '[' && last == ']')
                    || (first == '`' && last == '`');
                if (!escaped) return false;
                column = list;
                return true;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ReverseTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Reverses the current ordering or applies a descending order on key columns if none exists.
            /// </summary>
            /// <param name="t">The translator working on the query.</param>
            /// <param name="node">The method call expression for <c>Reverse</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Reverse after a Take/Skip-windowed source must reverse only the
                // windowed rows. The default path flips the existing _orderBy
                // direction and sends a single `ORDER BY … DESC LIMIT N` to the
                // server — which picks the BOTTOM rows of the full table, not the
                // reverse of the top-N window. Sister of the post-Take/Skip fixes
                // (3040f49 / e0f1397 / 99a02ce / a1eb69e / bfc8180 / d6de693).
                if (node.Arguments[0] is MethodCallExpression revWinSrc
                    && revWinSrc.Method.Name is nameof(Queryable.Take) or nameof(Queryable.Skip))
                {
                    return TranslateAfterTakeSkipWindow(t, node);
                }
                var revSource = t.Visit(node.Arguments[0]);
                // Reverse after a reshape must reverse the reshaped sequence (a prepended
                // element becomes the last one), not flip the server ORDER BY — the
                // transform runs after materialization and would keep its position. The
                // Try method self-gates on tail mode and installs a pending raw-GroupBy
                // grouping transform first.
                if (t._postMaterializeTransform != null || t._streamingGroupByKeySelector != null)
                {
                    if (t.TryAppendClientSequenceOperator(node))
                        return revSource;
                }
                if (t._orderBy.Count > 0)
                {
                    for (int i = 0; i < t._orderBy.Count; i++)
                    {
                        var (col, asc) = t._orderBy[i];
                        t._orderBy[i] = (col, !asc);
                    }
                }
                else
                {
                    foreach (var key in t._mapping.KeyColumns)
                        t._orderBy.Add((key.EscCol, false));
                }
                return revSource;
            }

            /// <summary>
            /// Windowed-source branch — wraps the source as a derived table and
            /// emits the reversed OrderBy keys on the outer SELECT. Kept in its
            /// own method so the common-path Translate stays stack-frame lean.
            /// </summary>
            private static Expression TranslateAfterTakeSkipWindow(QueryTranslator t, MethodCallExpression node)
            {
                var subPlanR = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var subMapR);
                t._mapping = subMapR;
                t.MergeSubPlanParameters(subPlanR);
                var winAliasR = t.EscapeAlias("__wrev" + t._joinCounter++);
                t._sql.AppendFragment("SELECT * FROM (").Append(subPlanR.Sql).AppendFragment(") AS ").Append(winAliasR);
                // Lift the source's OrderBy keys and flip them for the outer SELECT.
                // If no explicit OrderBy is present in the source chain, fall back
                // to the mapping's key columns ordered descending (matching the
                // unwindowed-Reverse default).
                var orderKeysR = ExtractOrderByKeys(node.Arguments[0]);
                t._orderBy.Clear();
                if (orderKeysR.Count > 0)
                {
                    foreach (var (keyLambda, asc) in orderKeysR)
                    {
                        var okParam = keyLambda.Parameters[0];
                        if (!t._correlatedParams.ContainsKey(okParam))
                            t._correlatedParams[okParam] = (subMapR, winAliasR);
                        var vctxOk = new VisitorContext(t._ctx, subMapR, t._provider, okParam, winAliasR, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                        var okVisitor = FastExpressionVisitorPool.Get(in vctxOk);
                        var okSql = okVisitor.Translate(keyLambda.Body);
                        FastExpressionVisitorPool.Return(okVisitor);
                        // Re-derived keys need the same treatment the forward OrderBy
                        // applied: null rank (flip-safe — rank and key flip together)
                        // and the TEXT-storage value coercions.
                        var okType = keyLambda.Body.Type;
                        if (t._provider.RequiresExplicitNullOrderingForNullableKeys
                            && (!okType.IsValueType || Nullable.GetUnderlyingType(okType) != null))
                            t._orderBy.Add(($"({okSql} IS NOT NULL)", !asc));
                        t._orderBy.Add((t.CoerceOrderKeySql(okSql, okType), !asc));
                    }
                }
                else
                {
                    foreach (var key in subMapR.KeyColumns)
                        t._orderBy.Add(($"{winAliasR}.{key.EscCol}", false));
                }
                return node;
            }
        }

        /// <summary>
        /// Concat with a client-tail reshape in either arm: the left arm translates and
        /// executes normally (carrying its reshape transform); the right arm's query
        /// executes through its own provider when the transform runs, and its rows are
        /// attached after the left arm's — exact LINQ Concat semantics, one round trip
        /// per arm.
        /// </summary>
        private static Expression TranslateClientConcat(QueryTranslator t, MethodCallExpression node)
        {
            IQueryable secondQuery;
            if (TryGetConstantValue(node.Arguments[1], out var secondConst) && secondConst is IQueryable direct)
            {
                secondQuery = direct;
            }
            else if (!HasFreeParameterReference(node.Arguments[1]))
            {
                secondQuery = (IQueryable)Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke()!;
            }
            else
            {
                throw new NormUnsupportedFeatureException(
                    "Concat requires the second sequence to be built from captured state; a row-derived query has no translation.");
            }

            var source = t.Visit(node.Arguments[0]);
            var elementType = node.Type.GetGenericArguments()[0];
            var capturedSecond = secondQuery;

            t.AppendPostMaterializeTransform((ctx, rows) =>
            {
                var output = CreateRuntimeList(elementType, rows.Count);
                foreach (var row in rows)
                    output.Add(row);
                foreach (var item in (System.Collections.IEnumerable)capturedSecond)
                    output.Add(item);
                return output;
            }, elementType);
            return source;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SetOperationTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Handles set operations such as <c>Union</c>, <c>Intersect</c>, and <c>Except</c> by combining
            /// the SQL generated for the left and right sequences.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression representing the set operation.</param>
            /// <returns>The original method call expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A client-tail reshape in either arm never reaches the set-op SQL — the
                // reshaped element exists only after materialization, so the SQL set-op
                // would silently drop it. Concat has exact client semantics (no dedup):
                // translate the left arm as the row plan and attach the right arm's rows
                // in the transform. Union/Intersect/Except fail closed: their dedup
                // compares column VALUES in SQL but object references in CLR, so client
                // evaluation would silently change semantics for untracked results.
                bool leftReshaped = SourceHasClientTailReshape(node.Arguments[0]);
                bool rightReshaped = SourceHasClientTailReshape(node.Arguments[1]);
                if (leftReshaped || rightReshaped)
                {
                    if (node.Method.Name != nameof(Queryable.Concat))
                    {
                        throw new NormUnsupportedFeatureException(
                            $"{node.Method.Name} with a client-materialized sequence operator (Append, Prepend, Chunk, Zip, " +
                            "DefaultIfEmpty with a default value) in either arm is not supported: SQL set semantics dedup by " +
                            "column values while in-memory dedup compares references. Materialize both sequences first " +
                            $"(e.g. ToListAsync) and use LINQ-to-Objects {node.Method.Name}.");
                    }
                    return TranslateClientConcat(t, node);
                }
                // SQL parsers reject ORDER BY / LIMIT / OFFSET on a bare set-op arm.
                // SQLite is strictest ("ORDER BY clause should come after UNION ALL
                // not before"); SqlServer and Postgres tolerate some forms but the
                // semantics are dialect-specific. Wrap any arm that carries an
                // OrderBy/Take/Skip as a derived table so the clause binds to the
                // arm only — every dialect accepts `SELECT * FROM (subq) AS alias`.
                bool leftNeedsWrap  = SourceHasOrderTakeOrSkip(node.Arguments[0]);
                bool rightNeedsWrap = SourceHasOrderTakeOrSkip(node.Arguments[1]);
                // UNION / INTERSECT / EXCEPT all use set semantics that dedup by string
                // equality on SQLite, so '10.5' vs '10.50' register as distinct rows even
                // though they're the same decimal. Concat (UNION ALL) doesn't dedup, but
                // we coerce uniformly so the materialized values match across arms (without
                // coercion one arm could yield decimal 10.5 from '10.5' while the other
                // yields 10.50 from '10.50', producing inconsistent row shapes for the same
                // logical value). The flag is scoped per-arm via try/finally.
                var savedCoerce = t._exactDecimalProjectionKeys;
                t._exactDecimalProjectionKeys = true;
                // LINQ set operations compare strings ordinally, but UNION / INTERSECT / EXCEPT
                // on CI-collation providers (MySQL, SQL Server) dedup and match by the column
                // collation — merging "abc"/"ABC" in Union, cross-matching them in Intersect/
                // Except. Wrap each arm's string projections in the provider's value-preserving
                // ordinal collation so the set semantics match LINQ. Concat (UNION ALL) never
                // dedups, so it needs no wrap.
                var savedOrdinal = t._forceOrdinalStringProjections;
                if (node.Method.Name != nameof(Queryable.Concat)
                    && t._provider.DefaultStringEqualityIsCaseInsensitive
                    && node.Method.GetGenericArguments() is { Length: 1 } setElem
                    && (setElem[0] == typeof(string) || ElementCarriesStringMember(setElem[0])))
                {
                    t._forceOrdinalStringProjections = true;
                }
                string leftSql, rightSql;
                try
                {
                    leftSql = t.TranslateSubExpression(node.Arguments[0]);
                    rightSql = t.TranslateSubExpression(node.Arguments[1]);
                }
                finally
                {
                    t._exactDecimalProjectionKeys = savedCoerce;
                    t._forceOrdinalStringProjections = savedOrdinal;
                }
                var setOp = node.Method.Name switch
                {
                    "Union" => "UNION",
                    // Concat preserves duplicates (LINQ-to-Objects semantics) -> UNION ALL.
                    "Concat" => "UNION ALL",
                    "Intersect" => "INTERSECT",
                    "Except" => "EXCEPT",
                    _ => throw new NormUnsupportedFeatureException(string.Format(ErrorMessages.UnsupportedOperation, "Set operation"))
                };
                // A wrapped arm places the arm SELECT inside a derived table, where
                // an unaliased expression column — the ordinal-collation wrap on a
                // scalar string arm — has no name and SQL Server rejects the derived
                // table outright. Name the expression before wrapping; set ops match
                // columns positionally, so the alias never changes semantics.
                if (leftNeedsWrap) leftSql = AliasNamelessScalarSelect(t, leftSql);
                if (rightNeedsWrap) rightSql = AliasNamelessScalarSelect(t, rightSql);
                // SQLite rejects bare-parenthesised compound SELECTs in set ops;
                // every dialect accepts `SELECT * FROM (subq) AS alias` though,
                // so wrap each LIMIT/OFFSET arm as a derived table. Unwrapped arms
                // are emitted as-is to keep the SQL minimal.
                t._sql.Clear();
                if (leftNeedsWrap) t._sql.Append("SELECT * FROM (").Append(leftSql).Append(") AS ").Append(t._provider.Escape("__lset0"));
                else t._sql.Append(leftSql);
                t._sql.Append(' ').Append(setOp).Append(' ');
                if (rightNeedsWrap) t._sql.Append("SELECT * FROM (").Append(rightSql).Append(") AS ").Append(t._provider.Escape("__rset0"));
                else t._sql.Append(rightSql);
                // TranslateSubExpression isolates each side in its own context, so any
                // Select-projection inside the arguments never propagates to the outer
                // translator. Without it, Generate() builds the materializer against
                // _mapping.Columns — for an anonymous-typed Union (`Select(p => new {…})
                // .Union(Select(c => new {…}))`) that's the left-source entity's columns,
                // not the projected anonymous type's ctor params, and GetCachedConstructor
                // throws "No suitable constructor for <>f__AnonymousType…" because the
                // arities don't match. Lift the projection from the left source (compiler
                // forces both sides to share the same shape for typed Union) so the
                // materializer reconstructs the anonymous type correctly.
                if (t._projection == null)
                {
                    var lifted = ExtractTrailingProjection(node.Arguments[0]);
                    if (lifted != null)
                        t._projection = lifted;
                }
                return node;
            }

            /// <summary>
            /// Returns true when the arm-source chain contains <c>OrderBy</c>,
            /// <c>OrderByDescending</c>, <c>ThenBy</c>, <c>ThenByDescending</c>,
            /// <c>Take</c>, or <c>Skip</c> — clauses that emit ORDER BY / LIMIT /
            /// OFFSET in the arm SQL and must be wrapped as a derived table so the
            /// outer set op parses.
            /// </summary>
            /// <summary>
            /// Names the output of a single-expression SELECT list (no alias, not a
            /// bare column identifier) so the arm can serve as a derived table.
            /// Multi-column lists, aliased items, and plain identifiers pass
            /// through unchanged.
            /// </summary>
            private static string AliasNamelessScalarSelect(QueryTranslator t, string sql)
            {
                if (!sql.StartsWith("SELECT ", StringComparison.Ordinal)) return sql;
                var depth = 0;
                var fromIdx = -1;
                for (var i = 7; i < sql.Length - 6; i++)
                {
                    var ch = sql[i];
                    if (ch == '(') depth++;
                    else if (ch == ')') depth--;
                    else if (depth == 0 && ch == ' ' && string.CompareOrdinal(sql, i, " FROM ", 0, 6) == 0)
                    {
                        fromIdx = i;
                        break;
                    }
                }
                if (fromIdx < 0) return sql;

                var list = sql[7..fromIdx];
                var listDepth = 0;
                foreach (var ch in list)
                {
                    if (ch == '(') listDepth++;
                    else if (ch == ')') listDepth--;
                    else if (ch == ',' && listDepth == 0) return sql; // multi-column
                }
                if (list.Contains(" AS ", StringComparison.OrdinalIgnoreCase)) return sql;
                if (!list.Contains('(') && !list.Contains(' ')) return sql; // bare identifier is already named

                return string.Concat(sql.AsSpan(0, fromIdx), " AS ", t._provider.Escape("__set_val"), sql.AsSpan(fromIdx));
            }

            private static bool SourceHasOrderTakeOrSkip(Expression source)
            {
                var current = source;
                while (current is MethodCallExpression mce)
                {
                    if (mce.Method.Name is nameof(Queryable.Take)
                        or nameof(Queryable.Skip)
                        or nameof(Queryable.OrderBy)
                        or nameof(Queryable.OrderByDescending)
                        or nameof(Queryable.ThenBy)
                        or nameof(Queryable.ThenByDescending))
                        return true;
                    if (mce.Arguments.Count == 0) break;
                    current = mce.Arguments[0];
                }
                return false;
            }

            /// <summary>
            /// Walks back through the source expression chain looking for the most-recent
            /// projection-defining call (Select / SelectMany) and returns its lambda.
            /// Skips over Where / OrderBy / Take / Skip / Distinct / Reverse / AsNoTracking
            /// / AsSplitQuery, which preserve the projection shape. Returns null if no
            /// projecting call is found (e.g. raw `Query&lt;T&gt;()` on both sides — the
            /// entity-Columns path the outer Generate() will fall back to is already correct).
            /// </summary>
            private static LambdaExpression? ExtractTrailingProjection(Expression source)
            {
                var current = source;
                while (current is MethodCallExpression mce)
                {
                    if ((mce.Method.Name == nameof(Queryable.Select)
                         || mce.Method.Name == nameof(Queryable.SelectMany))
                        && mce.Arguments.Count >= 2
                        && QueryTranslator.StripQuotes(mce.Arguments[mce.Arguments.Count - 1]) is LambdaExpression lambda)
                    {
                        return lambda;
                    }
                    if (mce.Arguments.Count == 0) break;
                    current = mce.Arguments[0];
                }
                return null;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SetPredicateTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates set-based predicates like <c>Any</c> or <c>Contains</c>.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for the predicate.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // Any() directly over a raw streaming GroupBy: a group exists exactly
                // when a row exists, so the quantifier collapses to Any() over the
                // ungrouped rows and stays fully server-side.
                if (node.Method.Name == nameof(Queryable.Any)
                    && node.Arguments.Count == 1
                    && node.Arguments[0] is MethodCallExpression gbAny
                    && IsRawStreamingGroupByShape(gbAny))
                {
                    var rowElementType = gbAny.Arguments[0].Type.GetGenericArguments()[0];
                    var rewritten = Expression.Call(typeof(Queryable), nameof(Queryable.Any),
                        new[] { rowElementType }, gbAny.Arguments[0]);
                    return t.Visit(rewritten);
                }
                // A client-tail reshaped, group-join-result, or raw-GroupBy source must
                // divert before any EXISTS/IN SQL is built: translate the source as the
                // row plan and evaluate the quantifier in memory. The source has been
                // visited at this point, so falling through to SQL generation is never
                // valid — fail closed if client evaluation is impossible.
                if (SourceHasClientTailReshape(node.Arguments[0])
                    || SourceHasGroupJoinResultTail(node.Arguments[0])
                    || SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    var source = t.Visit(node.Arguments[0]);
                    if (t.TryAppendClientScalarAggregate(node))
                        return source;
                    ThrowIfClientTailReshapePending(t, node.Method.Name);
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} after a client-materialized sequence operator has no in-memory equivalent overload.");
                }
                var result = t.HandleSetOperation(node);
                // EXISTS/IN SQL evaluates against server rows and would ignore a pending
                // client-tail reshape hidden deeper than the operator spine (e.g. inside
                // a joined sub-source).
                ThrowIfClientTailReshapePending(t, node.Method.Name);
                return result;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SequenceEqualTranslator : IMethodCallTranslator
        {
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                if (node.Arguments.Count != 2)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual comparer overloads are not provider-mobile; compare materialized sequences in CLR when a custom comparer is required.");
                }

                if (ExtractOrderByKeys(node.Arguments[0]).Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual requires the queryable source to have an explicit OrderBy/ThenBy chain for provider-mobile sequence comparison.");
                }

                if (TryTranslateLocalSequenceEqual(t, node, out var translated))
                    return translated;

                if (ExtractOrderByKeys(node.Arguments[1]).Count == 0)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual requires the second queryable source to have an explicit OrderBy/ThenBy chain for provider-mobile sequence comparison.");
                }

                var leftPlan = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var leftMapping);
                t.MergeSubPlanParameters(leftPlan);
                var rightPlan = t.TranslateInSubContext(node.Arguments[1], leftMapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var rightMapping);
                t.MergeSubPlanParameters(rightPlan);

                if (leftMapping.Columns.Length != rightMapping.Columns.Length)
                {
                    throw new NormUnsupportedFeatureException(
                        "SequenceEqual requires both sources to project the same provider-mobile row shape.");
                }

                var leftAlias = t.EscapeAlias("__seql" + t._joinCounter++);
                var rightAlias = t.EscapeAlias("__seqr" + t._joinCounter++);
                var rn = t._provider.Escape("__norm_seq_rn");
                var leftNumbered = BuildNumberedSequence(t, node.Arguments[0], leftPlan.Sql, leftMapping, leftAlias, rn);
                var rightNumbered = BuildNumberedSequence(t, node.Arguments[1], rightPlan.Sql, rightMapping, rightAlias, rn);
                var d1 = t._provider.Escape("__seqd1");
                var d2 = t._provider.Escape("__seqd2");
                var diff = t._provider.Escape("__seqdiff");

                t._sql.Append("SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM (SELECT * FROM (")
                    .Append(leftNumbered).Append(" EXCEPT ").Append(rightNumbered).Append(") AS ").Append(d1)
                    .Append(" UNION ALL SELECT * FROM (")
                    .Append(rightNumbered).Append(" EXCEPT ").Append(leftNumbered).Append(") AS ").Append(d2)
                    .Append(") AS ").Append(diff).Append(") THEN 1 ELSE 0 END");
                t._isAggregate = true;
                t._singleResult = true;
                return node;
            }

            private static bool TryTranslateLocalSequenceEqual(QueryTranslator t, MethodCallExpression node, out Expression translated)
            {
                translated = node;
                if (!TryGetConstantValue(node.Arguments[1], out var rightValue)
                    || rightValue is not System.Collections.IEnumerable rightEnumerable
                    || rightValue is IQueryable)
                {
                    return false;
                }

                var rightRows = new List<object>();
                foreach (var item in rightEnumerable)
                {
                    if (item is null)
                    {
                        throw new NormUnsupportedFeatureException(
                            "SequenceEqual against a local sequence containing null rows is not provider-mobile; compare materialized sequences in CLR.");
                    }
                    rightRows.Add(item);
                }

                var leftPlan = t.TranslateInSubContext(node.Arguments[0], t._mapping, t._parameterManager.Index, t._joinCounter, t._recursionDepth + 1, out var leftMapping);
                t.MergeSubPlanParameters(leftPlan);

                if (rightRows.Count == 0)
                {
                    var emptyAlias = t.EscapeAlias("__seqempty" + t._joinCounter++);
                    t._sql.Append("SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM (")
                        .Append(RemoveTrailingOrderBy(node.Arguments[0], leftPlan.Sql))
                        .Append(") AS ").Append(emptyAlias)
                        .Append(") THEN 1 ELSE 0 END");
                    t._isAggregate = true;
                    t._singleResult = true;
                    return true;
                }

                var leftAlias = t.EscapeAlias("__seql" + t._joinCounter++);
                var rn = t._provider.Escape("__norm_seq_rn");
                var leftNumbered = BuildNumberedSequence(t, node.Arguments[0], leftPlan.Sql, leftMapping, leftAlias, rn);
                var rightNumbered = BuildLocalNumberedSequence(t, leftMapping, rightRows, rn);
                var rightWrappedAlias1 = t._provider.Escape("__seqlocal1");
                var rightWrappedAlias2 = t._provider.Escape("__seqlocal2");
                var rightWrapped1 = "SELECT * FROM (" + rightNumbered + ") AS " + rightWrappedAlias1;
                var rightWrapped2 = "SELECT * FROM (" + rightNumbered + ") AS " + rightWrappedAlias2;
                var d1 = t._provider.Escape("__seqd1");
                var d2 = t._provider.Escape("__seqd2");
                var diff = t._provider.Escape("__seqdiff");

                t._sql.Append("SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM (SELECT * FROM (")
                    .Append(leftNumbered).Append(" EXCEPT ").Append(rightWrapped1).Append(") AS ").Append(d1)
                    .Append(" UNION ALL SELECT * FROM (")
                    .Append(rightWrapped2).Append(" EXCEPT ").Append(leftNumbered).Append(") AS ").Append(d2)
                    .Append(") AS ").Append(diff).Append(") THEN 1 ELSE 0 END");
                t._isAggregate = true;
                t._singleResult = true;
                return true;
            }

            private static string BuildLocalNumberedSequence(
                QueryTranslator t,
                TableMapping mapping,
                IReadOnlyList<object> rows,
                string rowNumberAlias)
            {
                var sb = PooledStringBuilder.Rent();
                try
                {
                    for (var rowIndex = 0; rowIndex < rows.Count; rowIndex++)
                    {
                        if (rowIndex > 0)
                            sb.Append(" UNION ALL ");
                        sb.Append("SELECT ").Append((rowIndex + 1).ToString(System.Globalization.CultureInfo.InvariantCulture))
                            .Append(" AS ").Append(rowNumberAlias);
                        foreach (var column in mapping.Columns)
                        {
                            var pName = t._ctx.RawProvider.ParamPrefix + "p" + t._parameterManager.GetNextIndex();
                            t.AddLiteralParameter(pName, column.Prop.GetValue(rows[rowIndex]));
                            sb.Append(", ").Append(pName).Append(" AS ").Append(column.EscCol);
                        }
                    }
                    return sb.ToString();
                }
                finally
                {
                    PooledStringBuilder.Return(sb);
                }
            }

            private static string BuildNumberedSequence(
                QueryTranslator t,
                Expression source,
                string sourceSql,
                TableMapping mapping,
                string alias,
                string rowNumberAlias)
            {
                var orderBy = BuildSequenceOrderBy(t, source, mapping, alias);
                var selectCols = string.Join(", ", mapping.Columns.Select(c => $"{alias}.{c.EscCol} AS {c.EscCol}"));
                var sql = RemoveTrailingOrderBy(source, sourceSql);
                return "SELECT ROW_NUMBER() OVER (ORDER BY " + PooledStringBuilder.JoinOrderBy(orderBy) + ") AS " + rowNumberAlias +
                       ", " + selectCols + " FROM (" + sql + ") AS " + alias;
            }

            private static List<(string col, bool asc)> BuildSequenceOrderBy(
                QueryTranslator t,
                Expression source,
                TableMapping mapping,
                string alias)
            {
                var result = new List<(string col, bool asc)>();
                foreach (var (keyLambda, ascending) in ExtractOrderByKeys(source))
                    result.Add((BuildSql(t, keyLambda.Parameters[0], keyLambda.Body, mapping, alias), ascending));
                return result;
            }

            private static string BuildSql(
                QueryTranslator t,
                ParameterExpression parameter,
                Expression expression,
                TableMapping mapping,
                string alias)
            {
                t._correlatedParams[parameter] = (mapping, alias);
                var vctx = new VisitorContext(t._ctx, mapping, t._provider, parameter, alias, t._correlatedParams, t._compiledParams, t._paramConverters, t._paramMap, t._recursionDepth + 1, t._params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var sql = visitor.Translate(expression);
                foreach (var kvp in visitor.GetParameters())
                    t._params[kvp.Key] = kvp.Value;
                if (t._params.Count > t._parameterManager.Index)
                    t._parameterManager.Index = t._params.Count;
                FastExpressionVisitorPool.Return(visitor);
                var type = Nullable.GetUnderlyingType(expression.Type) ?? expression.Type;
                return type == typeof(decimal) ? t._provider.ExactDecimalKeySql(sql) : sql;
            }

            private static string RemoveTrailingOrderBy(Expression source, string sql)
            {
                if (SourceHasTakeOrSkip(source))
                    return sql;
                return RemoveTrailingOrderByUnlessPaged(sql);
            }
        }


        /// <summary>
        /// True when <paramref name="e"/> is a set-operation call (Union / Concat / Intersect / Except).
        /// A set-op fills <c>_sql</c> with the bare compound; a projection or DISTINCT applied after it must
        /// wrap that compound as a derived table (as Where already does) or its effect is silently lost.
        /// </summary>
        private static bool IsSetOperationCall(Expression e)
            => e is MethodCallExpression m
               && m.Method.Name is "Union" or "Concat" or "Intersect" or "Except";

        /// <summary>
        /// True when a set-operation element type (an anonymous or DTO shape) exposes
        /// a string member: its dedup then needs the same value-preserving ordinal
        /// projection wrap the scalar-string arms get, or UNION / INTERSECT / EXCEPT
        /// on CI-collation providers would merge and cross-match case variants.
        /// </summary>
        private static bool ElementCarriesStringMember(Type elementType)
        {
            if (elementType.IsPrimitive || elementType == typeof(string) || elementType.IsEnum)
                return false;
            foreach (var prop in elementType.GetProperties())
                if (prop.PropertyType == typeof(string)) return true;
            return false;
        }

    }
}
