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
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal sealed partial class SelectClauseVisitor : ExpressionVisitor
    {
        private readonly TableMapping _mapping;
        private readonly List<string> _groupBy;
        private readonly DatabaseProvider _provider;
        // Outer-row alias used to reference parent columns from correlated subqueries
        // emitted inside the projection (e.g. `parent.Children.Count()` →
        // `(SELECT COUNT(*) FROM Child WHERE Child.FK = {outerAlias}.PK)`). Defaults to
        // the principal table's escaped name when no JOIN alias is in scope — SQLite
        // accepts table-qualified outer-scope references even without an explicit AS alias.
        private readonly string _outerAlias;
        // Used to look up intermediate/dependent-type mappings for multi-hop nav aggregates.
        private readonly DbContext? _ctx;
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());
        private StringBuilder? _sb;
        private List<PropertyInfo> _detectedCollections = new();

        /// <summary>SQL aggregate function name for COUNT operations.</summary>
        private const string CountFunctionName = "COUNT";

        // When set, decimal column references in projection are emitted as
        // CAST(col AS REAL). Used by the DISTINCT projection path because
        // SQLite stores decimals as TEXT and DISTINCT dedups lexically
        // ('10.5' vs '10.50' compare unequal as strings but equal numerically).
        // Same precision tradeoff as the rest of the decimal-cluster: REAL is
        // IEEE-754 binary, so results are approximate; SqlServer/Postgres/MySQL
        // use native DECIMAL and don't need this coercion.
        public bool ExactDecimalProjectionKeys { get; set; }

        // When set, string column references in the projection are wrapped with the provider's
        // value-preserving ordinal collation (OrdinalComparableStringProjection) so set-operation
        // dedup/matching (UNION / INTERSECT / EXCEPT) compares byte-wise like LINQ instead of by
        // the CI column collation. Identity on providers whose set ops are already ordinal.
        public bool ForceOrdinalStringProjections { get; set; }

        // Shared with the owning QueryTranslator so PROJECTION closures emit compiled parameters
        // (@cpN) instead of baked literals. Plans are cached by expression fingerprint, so a baked
        // closure literal poisons the cache: a second run with a different captured value silently
        // computes against the first run's value. The ParameterValueExtractor re-reads every
        // closure in document order on each execution and binds these slots positionally — the
        // same contract ExpressionToSqlVisitor's closure parameters use. Null in fragment-only
        // uses, which keep the literal fallback.
        public System.Collections.Generic.IDictionary<string, object>? SharedParams { get; set; }
        public System.Collections.Generic.List<string>? SharedCompiledParams { get; set; }
        // Converter registrations for the compiled slots above: plan binders apply
        // ConvertToProvider to the extracted value so a closure compared against a converter
        // column binds the PROVIDER representation (same contract as Where-side closures).
        public System.Collections.Generic.Dictionary<string, nORM.Mapping.IValueConverter>? SharedParamConverters { get; set; }
        // The projection (or order-key) lambda's own parameters — the outer ROW references.
        // Correlated subqueries emitted inside the projection resolve exactly these against
        // the outer alias; any OTHER free parameter (a compiled query's value parameter) is
        // NOT a row reference and must bind as a compiled parameter slot instead.
        public System.Collections.Generic.IReadOnlyList<ParameterExpression>? OuterRowParameters { get; set; }

        public SelectClauseVisitor(TableMapping mapping, List<string> groupBy, DatabaseProvider provider, string? outerAlias = null, DbContext? ctx = null)
        {
            _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
            _groupBy = groupBy ?? throw new ArgumentNullException(nameof(groupBy));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _outerAlias = outerAlias ?? mapping.EscTable;
            _ctx = ctx;
        }

        /// <summary>
        /// Gets the list of navigation properties detected during translation that should be
        /// fetched via separate queries to avoid Cartesian explosion.
        /// </summary>
        public IReadOnlyList<PropertyInfo> DetectedCollections => _detectedCollections;

        /// <summary>
        /// Translates a projection expression into a SQL <c>SELECT</c> column list.
        /// Each call resets detected collections, so the result reflects only the
        /// most recent translation.
        /// </summary>
        /// <param name="e">The projection expression to translate.</param>
        /// <returns>SQL representing the projection.</returns>
        public string Translate(Expression e)
        {
            if (e == null) throw new ArgumentNullException(nameof(e));

            // Reset per-translation state so consecutive calls don't accumulate stale entries.
            _detectedCollections = new List<PropertyInfo>();

            var sb = _stringBuilderPool.Get();
            _sb = sb;
            try
            {
                Visit(e);
                return sb.ToString();
            }
            finally
            {
                _sb = null;
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var sb = EnsureBuilder();
            // Embedded pure-value-type constructor (e.g. `new DateTime(2020,1,1)` inside
            // a ternary or comparison). Constant-fold via ExpressionValueExtractor and
            // emit as a SQL literal — without this the anonymous-type projection logic
            // below kicks in and emits `2020 AS Year, 1 AS Month, 1 AS Day` producing
            // "near AS: syntax error" inside the CASE WHEN.
            if (ExpressionValueExtractor.TryGetConstantValue(node, out var ctorValue))
            {
                sb.Append(FormatLiteral(ctorValue));
                return node;
            }

            // 7-arg new DateTimeOffset(y, m, d, h, mi, s, TimeSpan offset) with at least one
            // column date/time part and a compile-time constant offset. Emit canonical text;
            // the materialiser routes the string through DateTimeOffset.Parse.
            if (node.Type == typeof(DateTimeOffset)
                && node.Arguments.Count == 7
                && node.Constructor is { } dto7Ctor
                && dto7Ctor.GetParameters() is { Length: 7 } dto7Params
                && dto7Params[0].ParameterType == typeof(int)
                && dto7Params[1].ParameterType == typeof(int)
                && dto7Params[2].ParameterType == typeof(int)
                && dto7Params[3].ParameterType == typeof(int)
                && dto7Params[4].ParameterType == typeof(int)
                && dto7Params[5].ParameterType == typeof(int)
                && dto7Params[6].ParameterType == typeof(TimeSpan)
                && TryGetTimeSpanConstant(node.Arguments[6], out var tsOff))
            {
                string PartSql(Expression e)
                {
                    if (ExpressionValueExtractor.TryGetConstantValue(e, out var cv) && cv != null)
                        return FormatLiteral(cv);
                    var partBuilder = new System.Text.StringBuilder();
                    var saved = _sb;
                    _sb = partBuilder;
                    try { Visit(e); }
                    finally { _sb = saved; }
                    return partBuilder.ToString();
                }
                var ySql = PartSql(node.Arguments[0]);
                var mSql = PartSql(node.Arguments[1]);
                var dSql = PartSql(node.Arguments[2]);
                var hSql = PartSql(node.Arguments[3]);
                var miSql = PartSql(node.Arguments[4]);
                var sSql = PartSql(node.Arguments[5]);
                sb.Append(_provider.GetDateTimeOffsetFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql, tsOff));
                return node;
            }

            bool firstColumn = true;
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                // Members may be null for ValueTuple or similar parameterized constructors;
                // fall back to positional Item name (Item1, Item2, ...).
                var memberName = node.Members?[i]?.Name ?? $"Item{i + 1}";

                // Check if this is a navigation property (collection)
                if (IsNavigationCollection(arg, out var navProperty))
                {
                    // Track it for later split query processing
                    _detectedCollections.Add(navProperty);
                    // Skip adding to SQL SELECT - it will be fetched separately
                    continue;
                }

                if (!firstColumn) sb.Append(", ");
                Visit(arg);
                sb.Append(" AS ").Append(_provider.Escape(memberName));
                firstColumn = false;
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var sb = EnsureBuilder();
            // `entity.GetType().Name/FullName/Namespace/AssemblyQualifiedName` --
            // fold to the receiver's declared compile-time type. nORM doesn't
            // emit a runtime discriminator at projection-time, so the value is
            // always the declared entity type. Mirror Type.<member> on
            // typeof(T) which the existing constant-fold already handles.
            // Note: Type.Name etc. are declared on MemberInfo (its base), so we
            // check the receiver type rather than node.Member.DeclaringType.
            if (node.Expression is MethodCallExpression gtCall
                && gtCall.Method.Name == "GetType"
                && gtCall.Arguments.Count == 0
                && gtCall.Object != null
                && typeof(Type).IsAssignableFrom(gtCall.Type))
            {
                var declaredType = gtCall.Object.Type;
                string? value = node.Member.Name switch
                {
                    nameof(Type.Name) => declaredType.Name,
                    nameof(Type.FullName) => declaredType.FullName,
                    nameof(Type.Namespace) => declaredType.Namespace,
                    nameof(Type.AssemblyQualifiedName) => declaredType.AssemblyQualifiedName,
                    _ => null
                };
                if (value != null)
                {
                    sb.Append('\'').Append(value.Replace("'", "''")).Append('\'');
                    return node;
                }
            }
            if (node.Expression is ParameterExpression p && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() == typeof(IGrouping<,>) && node.Member.Name == "Key")
            {
                for (int i = 0; i < _groupBy.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append(_groupBy[i]);
                }
                return node;
            }

            // dtoCol.LocalDateTime — wall clock at the local machine's offset.
            // The local offset isn't known to the SQL engine, so capture it at
            // query-build time via TimeZoneInfo.Local and bake it into the SQL
            // shift. Matches .NET's behaviour where DTO.LocalDateTime captures
            // the local TZ at API-call time (with the same DST caveat).
            if (node.Member.Name == nameof(DateTimeOffset.LocalDateTime)
                && node.Member.DeclaringType == typeof(DateTimeOffset)
                && node.Expression != null
                && (Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type) == typeof(DateTimeOffset))
            {
                var dtoStart = sb.Length;
                Visit(node.Expression);
                var dtoSql = sb.ToString(dtoStart, sb.Length - dtoStart);
                sb.Length = dtoStart;
                sb.Append(DateTimeOffsetLocalTimeSql.Build(_provider, dtoSql));
                return node;
            }

            // (DateTime - DateTime).<TimeSpan-member> and (TimeOnly - TimeOnly).
            // <TimeSpan-member> -- mirror ETSV's TryEmitTimeSpanMember. The
            // binary path produces fractional seconds (DateTime: julianday
            // delta; TimeOnly: wrapped diff in [0, 24h)); member access
            // extracts a specific component matching System.TimeSpan's
            // semantics.
            if (node.Expression is BinaryExpression tsBin
                && tsBin.NodeType == ExpressionType.Subtract
                && node.Expression.Type == typeof(TimeSpan))
            {
                var leftUnderlying = Nullable.GetUnderlyingType(tsBin.Left.Type) ?? tsBin.Left.Type;
                var rightUnderlying = Nullable.GetUnderlyingType(tsBin.Right.Type) ?? tsBin.Right.Type;
                var endSql = TranslateProjectionArg(tsBin.Left);
                var startSql = TranslateProjectionArg(tsBin.Right);
                var secondsSql = leftUnderlying == typeof(TimeOnly) && rightUnderlying == typeof(TimeOnly)
                    ? $"({_provider.GetTimeOnlyDifferenceSecondsSql(endSql, startSql)})"
                    : $"({_provider.GetDateTimeDifferenceSecondsSql(endSql, startSql)})";
                string? emit = node.Member.Name switch
                {
                    nameof(TimeSpan.TotalSeconds) => secondsSql,
                    nameof(TimeSpan.TotalMinutes) => $"({secondsSql} / 60.0)",
                    nameof(TimeSpan.TotalHours) => $"({secondsSql} / 3600.0)",
                    nameof(TimeSpan.TotalDays) => $"({secondsSql} / 86400.0)",
                    nameof(TimeSpan.TotalMilliseconds) => $"({secondsSql} * 1000.0)",
                    nameof(TimeSpan.Days) => _provider.GetTruncateToIntSql($"({secondsSql} / 86400)"),
                    nameof(TimeSpan.Hours) => $"({_provider.GetTruncateToIntSql($"({secondsSql} / 3600)")} % 24)",
                    nameof(TimeSpan.Minutes) => $"({_provider.GetTruncateToIntSql($"({secondsSql} / 60)")} % 60)",
                    nameof(TimeSpan.Seconds) => $"({_provider.GetTruncateToIntSql(secondsSql)} % 60)",
                    _ => null
                };
                if (emit != null)
                {
                    sb.Append(emit);
                    return node;
                }
            }

            // Stored TimeSpan column members: r.Duration.TotalSeconds, .Hours,
            // .Minutes, etc. The WHERE visitor already routes these through the
            // provider seconds hook; projection needs the same handling before
            // the member name is mistaken for an entity column.
            if (node.Expression != null
                && (Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type) == typeof(TimeSpan))
            {
                var spanSql = TranslateProjectionArg(node.Expression);
                var secondsSql = _provider.GetTimeSpanColumnSecondsSql(spanSql);
                string? emit = node.Member.Name switch
                {
                    nameof(TimeSpan.TotalSeconds) => secondsSql,
                    nameof(TimeSpan.TotalMinutes) => $"({secondsSql} / 60.0)",
                    nameof(TimeSpan.TotalHours) => $"({secondsSql} / 3600.0)",
                    nameof(TimeSpan.TotalDays) => $"({secondsSql} / 86400.0)",
                    nameof(TimeSpan.TotalMilliseconds) => $"({secondsSql} * 1000.0)",
                    nameof(TimeSpan.Days) => _provider.GetTruncateToIntSql($"({secondsSql} / 86400)"),
                    nameof(TimeSpan.Hours) => $"({_provider.GetTruncateToIntSql($"({secondsSql} / 3600)")} % 24)",
                    nameof(TimeSpan.Minutes) => $"({_provider.GetTruncateToIntSql($"({secondsSql} / 60)")} % 60)",
                    nameof(TimeSpan.Seconds) => $"({_provider.GetTruncateToIntSql(secondsSql)} % 60)",
                    _ => null
                };
                if (emit != null)
                {
                    sb.Append(emit);
                    return node;
                }
            }

            // Nullable<T>.HasValue / .Value -- structural members. HasValue
            // lowers to IS NOT NULL (boolean column); Value passes through to
            // the underlying expression. Mirror of ETSV's VisitMember branch.
            if (node.Expression != null
                && node.Expression.Type.IsGenericType
                && node.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                if (node.Member.Name == "HasValue")
                {
                    sb.Append('(').Append(TranslateProjectionArg(node.Expression)).Append(" IS NOT NULL)");
                    return node;
                }
                if (node.Member.Name == "Value")
                {
                    sb.Append(TranslateProjectionArg(node.Expression));
                    return node;
                }
            }

            if (_mapping.TryGetColumnForMemberAccess(node, out var col))
            {
                var memberType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (ExactDecimalProjectionKeys && memberType == typeof(decimal))
                {
                    // Provider hook: SqliteProvider emits the canonical decimal
                    // text, others identity. Decimal projections from set ops /
                    // DISTINCT need scale-insensitive EXACT dedup on SQLite
                    // (TEXT storage; REAL would merge values differing beyond
                    // double precision), and the canonical text still
                    // materializes as the decimal value.
                    sb.Append(_provider.ExactDecimalKeySql(col.EscCol));
                }
                else if (ForceOrdinalStringProjections && memberType == typeof(string)
                         && _provider.OrdinalComparableStringProjection(col.EscCol) is { } ordinalSql)
                {
                    // Set-operation arm on a CI-collation provider: emit the value-preserving
                    // binary-collated form so UNION/INTERSECT/EXCEPT compare byte-wise like LINQ.
                    sb.Append(ordinalSql);
                }
                else
                {
                    sb.Append(col.EscCol);
                }
                return node;
            }

            // Single-reference navigation traversal in a projection: e.Dept.Title
            // emits a correlated scalar subquery (NULL when the parent is missing),
            // mirroring the predicate translator.
            if (node.Expression is MemberExpression scvNavExpr
                && BuildScvReferenceNavigationScalarSql(scvNavExpr, node.Member.Name, depth: 0) is { } navScalarSql)
            {
                sb.Append(navScalarSql);
                return node;
            }

            // Member on a non-entity type (DateTime.Year, string.Length, TimeSpan.TotalHours,
            // etc.) — route through the provider's function map the same way
            // ExpressionToSqlVisitor does on the WHERE side. Without this, projection emits
            // "Member 'Year' on type 'DateTime' is not mapped to a column" even though the
            // WHERE side happily accepts the same expression.
            if (node.Expression != null && node.Member.DeclaringType != null)
            {
                var exprSql = TranslateProjectionArg(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType, exprSql);
                if (fn != null)
                {
                    sb.Append(fn);
                    return node;
                }
            }

            // Closure-captured local (compiler-generated DisplayClass member) -- evaluate
            // to a constant and emit as a literal. ExpressionToSqlVisitor binds these as
            // parameters via CreateSafeParameter, but SCV writes SQL fragments without
            // parameter-manager access, so inline as a literal. Mirrors the constant-fold
            // ETSV path so projection accepts the same captured-local shapes Where does
            // -- without this, `var prefix = "x"; .Select(p => p.Name.StartsWith(prefix))`
            // throws the misleading "not mapped to a column" error pointing at the
            // DisplayClass field.
            if (QueryTranslator.TryGetConstantValue(node, out var capturedValue))
            {
                // Emit a compiled parameter when the owning translator shared its channel: the
                // live value is re-extracted on every plan-cache hit. Baking the literal would
                // freeze the FIRST run's captured value into the cached SQL.
                if (SharedParams != null && SharedCompiledParams != null)
                {
                    // Reuse the slot already minted for this exact node (an ORDER BY key
                    // expansion may render the projection fragment before Build does) —
                    // the extractor produces ONE value per tree occurrence.
                    var reused = QueryTranslator.TryReuseClosureSlot(node);
                    if (reused != null)
                    {
                        sb.Append(reused);
                        return node;
                    }
                    var paramName = $"{_provider.ParamPrefix}cp{SharedCompiledParams.Count}";
                    SharedParams[paramName] = DBNull.Value;
                    SharedCompiledParams.Add(paramName);
                    QueryTranslator.RecordClosureSlot(node, paramName);
                    sb.Append(paramName);
                    return node;
                }
                sb.Append(FormatLiteral(capturedValue));
                return node;
            }

            throw new InvalidOperationException(
                $"Member '{node.Member.Name}' on type '{node.Member.DeclaringType?.Name}' is not mapped to a column in table '{_mapping.TableName}'. " +
                $"Ensure the property is read/write and not a navigation collection.");
        }

        /// <summary>
        /// Correlated scalar subquery for a reference-navigation member in a
        /// projection; the chain roots at the projection parameter (this visitor's
        /// mapping/alias) and nests one subquery per additional hop. Null when the
        /// receiver is not a mapped single-key reference navigation.
        /// </summary>
        /// <summary>
        /// SQL for a whole-entity navigation receiver's FK VALUE (projection side):
        /// the dependent's FK column for a root receiver, or the nested subquery
        /// fetching the FK through a deeper chain. Used by null tests in projections.
        /// </summary>
        private bool TryResolveScvNavigationFkValueSql(Expression expr, out string fkValueSql)
        {
            fkValueSql = string.Empty;
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            if (expr is not MemberExpression navExpr || _ctx == null)
                return false;
            var navType = System.Nullable.GetUnderlyingType(navExpr.Type) ?? navExpr.Type;
            if (!navType.IsClass || navType == typeof(string))
                return false;
            TableMapping principalMap;
            try { principalMap = _ctx.GetMapping(navType); }
            catch { return false; }
            if (principalMap.KeyColumns.Length != 1)
                return false;
            if (navExpr.Expression is ParameterExpression)
            {
                var fkCol = ExpressionToSqlVisitor.FindReferenceNavForeignKey(_mapping, navExpr.Member.Name, navType, principalMap);
                if (fkCol == null) return false;
                fkValueSql = $"{_outerAlias}.{fkCol.EscCol}";
                return true;
            }
            if (navExpr.Expression is MemberExpression parentNav)
            {
                var ownerType = System.Nullable.GetUnderlyingType(parentNav.Type) ?? parentNav.Type;
                if (!ownerType.IsClass || ownerType == typeof(string)) return false;
                TableMapping ownerMap;
                try { ownerMap = _ctx.GetMapping(ownerType); }
                catch { return false; }
                var fkCol = ExpressionToSqlVisitor.FindReferenceNavForeignKey(ownerMap, navExpr.Member.Name, navType, principalMap);
                if (fkCol == null) return false;
                var nested = BuildScvReferenceNavigationScalarSql(parentNav, fkCol.PropName, depth: 0);
                if (nested == null) return false;
                fkValueSql = nested;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Renders a whole-entity navigation null test for a projection: FK [NOT] NULL
        /// normally, or a [NOT] EXISTS probe of the principal row when the principal
        /// type carries global filters — a filtered-out parent must read as missing,
        /// consistent with member reads through the navigation. Emits a bare predicate;
        /// the boolean-value wrapper handles value positions.
        /// </summary>
        private bool TryRenderScvNavigationNullTest(Expression expr, bool testIsNull, out string sql)
        {
            sql = string.Empty;
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            if (expr is not MemberExpression navExpr || _ctx == null)
                return false;
            var navType = System.Nullable.GetUnderlyingType(navExpr.Type) ?? navExpr.Type;
            if (!navType.IsClass || navType == typeof(string))
                return false;
            TableMapping principalMap;
            try { principalMap = _ctx.GetMapping(navType); }
            catch { return false; }
            if (principalMap.KeyColumns.Length != 1)
                return false;
            if (!TryResolveScvNavigationFkValueSql(navExpr, out var fkValueSql))
                return false;

            var combined = GlobalFilterFragment.CombineWithTenant(_ctx, principalMap.Type);
            if (combined == null)
            {
                sql = $"({fkValueSql}{(testIsNull ? " IS NULL" : " IS NOT NULL")})";
                return true;
            }
            var probeAlias = _provider.Escape("NF");
            var filterSql = RenderNavigationFilter(combined, probeAlias);
            sql = $"{(testIsNull ? "NOT EXISTS(" : "EXISTS(")}SELECT 1 FROM {QueryTranslator.TemporalTableSource(principalMap)} {probeAlias} " +
                  $"WHERE {probeAlias}.{principalMap.KeyColumns[0].EscCol} = {fkValueSql} AND {filterSql})";
            return true;
        }

        private string? BuildScvReferenceNavigationScalarSql(MemberExpression navExpr, string targetMemberName, int depth)
        {
            var navType = System.Nullable.GetUnderlyingType(navExpr.Type) ?? navExpr.Type;
            if (!navType.IsClass || navType == typeof(string) || _ctx == null)
                return null;
            TableMapping principalMap;
            try { principalMap = _ctx.GetMapping(navType); }
            catch { return null; }
            if (principalMap.KeyColumns.Length != 1
                || !principalMap.ColumnsByName.TryGetValue(targetMemberName, out var targetCol))
                return null;

            string? fkValueSql;
            if (navExpr.Expression is ParameterExpression)
            {
                var fkCol = ExpressionToSqlVisitor.FindReferenceNavForeignKey(_mapping, navExpr.Member.Name, navType, principalMap);
                if (fkCol == null) return null;
                fkValueSql = $"{_outerAlias}.{fkCol.EscCol}";
            }
            else if (navExpr.Expression is MemberExpression parentNav)
            {
                var ownerType = System.Nullable.GetUnderlyingType(parentNav.Type) ?? parentNav.Type;
                if (!ownerType.IsClass || ownerType == typeof(string)) return null;
                TableMapping ownerMap;
                try { ownerMap = _ctx.GetMapping(ownerType); }
                catch { return null; }
                var fkCol = ExpressionToSqlVisitor.FindReferenceNavForeignKey(ownerMap, navExpr.Member.Name, navType, principalMap);
                if (fkCol == null) return null;
                fkValueSql = BuildScvReferenceNavigationScalarSql(parentNav, fkCol.PropName, depth + 1);
                if (fkValueSql == null) return null;
            }
            else
            {
                return null;
            }

            var alias = _provider.Escape("NR" + depth.ToString(System.Globalization.CultureInfo.InvariantCulture));
            // The principal's global filters (soft-delete, tenant) gate visibility through
            // the navigation too: a filtered-out parent must read as a MISSING parent
            // (NULL member), or its data leaks into projections. Mirrors the ETSV emit.
            string? globalFilterSql = null;
            var combinedFilter = GlobalFilterFragment.CombineWithTenant(_ctx, principalMap.Type);
            if (combinedFilter != null)
                globalFilterSql = RenderNavigationFilter(combinedFilter, alias);
            return $"(SELECT {alias}.{targetCol.EscCol} FROM {QueryTranslator.TemporalTableSource(principalMap)} {alias} " +
                   $"WHERE {alias}.{principalMap.KeyColumns[0].EscCol} = {fkValueSql}" +
                   (globalFilterSql != null ? $" AND {globalFilterSql}" : string.Empty) + ")";
        }
    }
}
