using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using System.Collections.Frozen;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == null && node.Member.DeclaringType == typeof(Guid) && node.Member.Name == nameof(Guid.Empty))
            {
                _sql.Append('\'').Append(Guid.Empty.ToString("D", CultureInfo.InvariantCulture)).Append('\'');
                return node;
            }
            // dtoCol.LocalDateTime — sister of the projection-side handler in
            // SelectClauseVisitor (1f06ac1). Per-instant local timezone semantics are lowered through
            // DateTimeOffsetLocalTimeSql using TimeZoneInfo.Local offset ranges.
            if (node.Member.Name == nameof(DateTimeOffset.LocalDateTime)
                && node.Member.DeclaringType == typeof(DateTimeOffset)
                && node.Expression != null
                && (Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type) == typeof(DateTimeOffset))
            {
                var dtoSql = GetSql(node.Expression);
                _sql.Append(DateTimeOffsetLocalTimeSql.Build(_provider, dtoSql));
                return node;
            }
            // `entity.GetType().Name/FullName/Namespace/AssemblyQualifiedName` --
            // fold to the receiver's declared compile-time type. Mirror of the
            // SCV fold in 149fa9a so the same expression works in WHERE. Type.*
            // members are declared on MemberInfo (Type's base), so check the
            // receiver type rather than node.Member.DeclaringType.
            if (node.Expression is MethodCallExpression gtCall
                && gtCall.Method.Name == "GetType"
                && gtCall.Arguments.Count == 0
                && gtCall.Object != null
                && typeof(Type).IsAssignableFrom(gtCall.Type))
            {
                var declaredType = gtCall.Object.Type;
                string? typeNameLiteral = node.Member.Name switch
                {
                    nameof(Type.Name) => declaredType.Name,
                    nameof(Type.FullName) => declaredType.FullName,
                    nameof(Type.Namespace) => declaredType.Namespace,
                    nameof(Type.AssemblyQualifiedName) => declaredType.AssemblyQualifiedName,
                    _ => null
                };
                if (typeNameLiteral != null)
                {
                    _sql.Append('\'').Append(typeNameLiteral.Replace("'", "''")).Append('\'');
                    return node;
                }
            }
            // TimeSpan member access whose receiver is a DateTime or TimeOnly
            // subtraction lowers to a fractional-seconds scalar via the provider, then
            // a unit-conversion divide. Examples: (end - start).TotalHours,
            // .TotalMinutes, .TotalSeconds, .TotalDays, .Days, .Hours, .Minutes,
            // .Seconds. Both nullable and non-nullable receivers.
            if (node.Expression is BinaryExpression timeSpanBinary
                && timeSpanBinary.NodeType == ExpressionType.Subtract
                && node.Expression.Type == typeof(TimeSpan))
            {
                if (IsDateTimeLike(timeSpanBinary.Left.Type)
                    && IsDateTimeLike(timeSpanBinary.Right.Type)
                    && TryEmitTimeSpanMember(node.Member.Name, GetSql(timeSpanBinary.Left), GetSql(timeSpanBinary.Right), useTimeOnly: false))
                {
                    return node;
                }
                if (IsTimeOnly(timeSpanBinary.Left.Type)
                    && IsTimeOnly(timeSpanBinary.Right.Type)
                    && TryEmitTimeSpanMember(node.Member.Name, GetSql(timeSpanBinary.Left), GetSql(timeSpanBinary.Right), useTimeOnly: true))
                {
                    return node;
                }
            }

            // TimeSpan stored-column member access: r.Duration.TotalSeconds, .Days, etc.
            // The subtraction path above handles (end - start).MemberName. This path handles
            // a mapped column (or any non-subtraction expression) whose CLR type is TimeSpan.
            // GetTimeSpanColumnSecondsSql wraps the column reference in provider-specific SQL
            // that evaluates to total fractional seconds, then TryEmitTimeSpanMemberFromSeconds
            // applies the same unit-conversion math as the subtraction path.
            if (node.Expression != null
                && !(node.Expression is BinaryExpression { NodeType: ExpressionType.Subtract })
                && (Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type) == typeof(TimeSpan))
            {
                var storedColSql = GetSql(node.Expression);
                var secondsSql = _provider.GetTimeSpanColumnSecondsSql(storedColSql);
                if (TryEmitTimeSpanMemberFromSeconds(node.Member.Name, secondsSql))
                    return node;
            }

            // Nullable<T> structural members: HasValue -> IS NOT NULL, Value -> operand itself.
            // GetValueOrDefault is a method, handled in VisitMethodCall.
            if (node.Expression != null
                && node.Expression.Type.IsGenericType
                && node.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                if (node.Member.Name == "HasValue")
                {
                    _sql.Append('(');
                    Visit(node.Expression);
                    _sql.Append(" IS NOT NULL)");
                    return node;
                }
                if (node.Member.Name == "Value")
                {
                    Visit(node.Expression);
                    return node;
                }
            }
            if (node.Expression is ParameterExpression groupParameter &&
                _parameterMappings.TryGetValue(groupParameter, out _) &&
                _groupingKeys.TryGetValue(groupParameter, out var groupKey) &&
                node.Member.Name == "Key")
            {
                _sql.Append(groupKey);
                return node;
            }
            if (TableMapping.TryGetMemberAccessRoot(node, out var mappedRoot) &&
                _parameterMappings.TryGetValue(mappedRoot, out var info) &&
                info.Mapping.TryGetColumnForMemberAccess(node, out var column))
            {
                // Table aliases are generated internally and escaped when created,
                // allowing them to be used safely without additional validation.
                _sql.Append($"{info.Alias}.{column.EscCol}");
                return node;
            }
            // Single-reference navigation traversal: e.Dept.Title (or deeper,
            // e.Dept.Head.Name) emits a correlated scalar subquery against the
            // principal table, keyed by the dependent's FK. A missing parent
            // yields SQL NULL, which drops the row from predicates and projects
            // null — the relational (EF-style) semantics for optional navigations.
            if (TryEmitReferenceNavigationScalar(node))
                return node;
            if (node.Expression is ParameterExpression p && !_parameterMappings.ContainsKey(p))
            {
                var key = (p, node.Member.Name);
                if (!_memberParamMap.TryGetValue(key, out var paramName))
                {
                    // The __qm<Member> marker names the query-parameter member this slot
                    // binds, so the compiled-query pipeline pairs it BY NAME. Positional
                    // document-order pairing breaks for these slots: the projection is
                    // rendered at Build time, after clause-translated slots registered,
                    // while the value-source walk runs in document order.
                    paramName = $"{_provider.ParamPrefix}p{_paramIndex++}__qm{node.Member.Name}";
                    _params[paramName] = DBNull.Value;
                    _compiledParams.Add(paramName);
                    _memberParamMap[key] = paramName;
                }
                _sql.Append(paramName);
                return node;
            }
            if (TryGetConstantValue(node, out var value))
            {
                // Closure-captured variable: emit a compiled parameter so the live value is
                // re-extracted from the expression tree on every plan-cache hit.  Baking the
                // value into _params at translation time causes stale values to be used when
                // the captured variable changes between calls that share the same cached plan.
                //
                // Use the current size of the SHARED _compiledParams list as the index so that
                // parameter names are globally unique across all visitor instances within one
                // query translation.  The "cp" prefix prevents collisions with inline-constant
                // parameters which use the "p" prefix (visitor-local _paramIndex).
                // Reuse the slot already minted for this exact node (clause expansions can
                // render one tree occurrence more than once — one slot per occurrence).
                var reusedName = QueryTranslator.TryReuseClosureSlot(node);
                if (reusedName != null)
                {
                    _sql.Append(reusedName);
                    return node;
                }
                var paramName = $"{_provider.ParamPrefix}cp{_compiledParams.Count}";
                _params[paramName] = DBNull.Value; // placeholder; actual value supplied at execution time
                _compiledParams.Add(paramName);
                QueryTranslator.RecordClosureSlot(node, paramName);
                _sql.Append(paramName);
                return node;
            }
            if (node.Expression != null)
            {
                var exprSql = GetSql(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType!, exprSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
            }
            throw new NormUnsupportedFeatureException($"Member '{node.Member.Name}' is not supported in this context.");
        }
        private bool TryEmitReferenceNavigationScalar(MemberExpression node)
        {
            if (node.Expression is not MemberExpression navExpr)
                return false;
            var sql = BuildReferenceNavigationScalarSql(navExpr, node.Member.Name, depth: 0);
            if (sql == null) return false;
            _sql.Append(sql);
            return true;
        }

        /// <summary>
        /// Correlated scalar subquery for <c>navChain.member</c>:
        /// <c>(SELECT NR0.col FROM Principal NR0 WHERE NR0.pk = fkValue)</c>, where the
        /// FK value is the dependent's column for a root-parameter owner or a nested
        /// subquery for a deeper chain. Returns null when the receiver is not a mapped
        /// single-key reference navigation so the caller falls through to the normal
        /// member handling (closures, provider functions).
        /// </summary>
        private string? BuildReferenceNavigationScalarSql(MemberExpression navExpr, string targetMemberName, int depth)
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

            if (!TryResolveNavigationFkValueSql(navExpr, navType, principalMap, depth, out var fkValueSql))
                return null;

            var alias = _provider.Escape("NR" + depth.ToString(System.Globalization.CultureInfo.InvariantCulture));
            // The principal's global filters (soft-delete, tenant) gate visibility through
            // the navigation too: a filtered-out parent must read as a MISSING parent
            // (NULL member), or its data leaks through predicates and projections.
            var globalFilterSql = RenderPrincipalGlobalFilterSql(principalMap, alias);
            return $"(SELECT {alias}.{targetCol.EscCol} FROM {QueryTranslator.TemporalTableSource(principalMap)} {alias} " +
                   $"WHERE {alias}.{principalMap.KeyColumns[0].EscCol} = {fkValueSql}" +
                   (globalFilterSql != null ? $" AND {globalFilterSql}" : string.Empty) + ")";
        }

        /// <summary>
        /// Emits a whole-entity navigation null test. Without global filters on the
        /// principal this is the dependent's FK being [NOT] NULL. When the principal
        /// type carries global filters, a filtered-out parent must read as MISSING —
        /// consistent with member reads through the navigation — so the test probes
        /// the principal row itself with the filter applied.
        /// </summary>
        internal bool TryEmitNavigationNullTest(Expression expr, bool testIsNull)
        {
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
            if (!TryResolveNavigationFkValueSql(navExpr, navType, principalMap, depth: 0, out var fkValueSql))
                return false;

            var probeAlias = _provider.Escape("NF");
            var filterSql = RenderPrincipalGlobalFilterSql(principalMap, probeAlias);
            if (filterSql == null)
            {
                _sql.Append('(').Append(fkValueSql)
                    .Append(testIsNull ? " IS NULL" : " IS NOT NULL").Append(')');
                return true;
            }
            _sql.Append(testIsNull ? "NOT EXISTS(" : "EXISTS(")
                .Append("SELECT 1 FROM ").Append(QueryTranslator.TemporalTableSource(principalMap)).Append(' ').Append(probeAlias)
                .Append(" WHERE ").Append(probeAlias).Append('.').Append(principalMap.KeyColumns[0].EscCol)
                .Append(" = ").Append(fkValueSql)
                .Append(" AND ").Append(filterSql).Append(')');
            return true;
        }

        /// <summary>
        /// Translates the combined global filter of a navigation principal against the
        /// correlated subquery's alias. Same sub-visitor pattern as the aggregate
        /// selector branches (paramIndexStart offset prevents @pN collisions).
        /// </summary>
        private string? RenderPrincipalGlobalFilterSql(TableMapping principalMap, string alias)
        {
            if (_ctx == null)
                return null;
            var combined = GlobalFilterFragment.Combine(_ctx, principalMap.Type);
            if (combined == null)
                return null;
            var vctx = new VisitorContext(_ctx, principalMap, _provider, combined.Parameters[0], alias, _parameterMappings, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _paramIndex);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            try
            {
                var sql = visitor.Translate(combined.Body);
                foreach (var kvp in visitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                _paramIndex = visitor.ParamIndex;
                return sql;
            }
            finally
            {
                FastExpressionVisitorPool.Return(visitor);
            }
        }

        /// <summary>
        /// SQL producing the FOREIGN KEY VALUE of a reference-navigation receiver:
        /// the dependent's FK column for a root-parameter owner, or a nested scalar
        /// subquery fetching the FK through a deeper chain. Also used by the
        /// whole-entity null test (`e.Dept == null` is FK-IS-NULL).
        /// </summary>
        internal bool TryResolveNavigationFkValueSql(
            MemberExpression navExpr, Type navType, TableMapping principalMap, int depth, out string fkValueSql)
        {
            fkValueSql = string.Empty;
            if (navExpr.Expression is ParameterExpression rootP
                && _parameterMappings.TryGetValue(rootP, out var rootInfo))
            {
                var fkCol = FindReferenceNavForeignKey(rootInfo.Mapping, navExpr.Member.Name, navType, principalMap);
                if (fkCol == null) return false;
                fkValueSql = $"{rootInfo.Alias}.{fkCol.EscCol}";
                return true;
            }
            if (navExpr.Expression is MemberExpression parentNav)
            {
                var ownerType = System.Nullable.GetUnderlyingType(parentNav.Type) ?? parentNav.Type;
                if (!ownerType.IsClass || ownerType == typeof(string) || _ctx == null) return false;
                TableMapping ownerMap;
                try { ownerMap = _ctx.GetMapping(ownerType); }
                catch { return false; }
                var fkCol = FindReferenceNavForeignKey(ownerMap, navExpr.Member.Name, navType, principalMap);
                if (fkCol == null) return false;
                var nested = BuildReferenceNavigationScalarSql(parentNav, fkCol.PropName, depth + 1);
                if (nested == null) return false;
                fkValueSql = nested;
                return true;
            }
            return false;
        }

        /// <summary>
        /// SQL for a whole-entity navigation receiver's FK value, for null tests:
        /// resolves the navigation type/mapping gates then delegates to
        /// <see cref="TryResolveNavigationFkValueSql(MemberExpression, Type, TableMapping, int, out string)"/>.
        /// </summary>
        internal bool TryResolveNavigationFkValueSql(Expression expr, out string fkValueSql)
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
            return TryResolveNavigationFkValueSql(navExpr, navType, principalMap, depth: 0, out fkValueSql);
        }

        /// <summary>
        /// Binds a captured entity INSTANCE by its primary key: the extractor
        /// supplies the entity object at execution time and this converter maps it
        /// to the key value, so `e.Dept == someDept` stays plan-cache-safe across
        /// different captured instances. A null instance passes through (the
        /// binder skips converters for nulls), and the null-safe comparison then
        /// matches orphans exactly like C# `null == null`.
        /// </summary>
        private sealed class EntityKeyValueConverter : nORM.Mapping.IValueConverter
        {
            private readonly Type _entityType;
            private readonly System.Reflection.PropertyInfo _keyProp;
            public EntityKeyValueConverter(Type entityType, System.Reflection.PropertyInfo keyProp)
            {
                _entityType = entityType;
                _keyProp = keyProp;
            }
            public Type ModelType => _entityType;
            public Type ProviderType => _keyProp.PropertyType;
            public object? ConvertToProvider(object? modelValue)
                => modelValue == null ? null : _keyProp.GetValue(modelValue);
            public object? ConvertFromProvider(object? providerValue) => providerValue;
        }

        /// <summary>
        /// Whole-entity navigation vs a captured entity instance: compares the
        /// dependent's FOREIGN KEY to the instance's PRIMARY KEY through the
        /// compiled-parameter converter pipeline. Null-safe both ways so a missing
        /// parent and a null captured instance follow C# equality.
        /// </summary>
        private bool TryEmitNavigationEntityComparison(BinaryExpression node)
        {
            string fkSql;
            Expression valueSide;
            if (TryResolveNavigationFkValueSql(node.Left, out fkSql)) valueSide = node.Right;
            else if (TryResolveNavigationFkValueSql(node.Right, out fkSql)) valueSide = node.Left;
            else return false;

            // Navigation vs navigation (e.Boss == e.Mentor): both sides reduce to
            // their FK values, null-safe so two orphan sides compare like C# nulls.
            if (TryResolveNavigationFkValueSql(valueSide, out var otherFkSql))
            {
                _sql.Append(node.NodeType == ExpressionType.Equal
                    ? _provider.NullSafeEqual(fkSql, otherFkSql)
                    : _provider.NullSafeNotEqual(fkSql, otherFkSql));
                return true;
            }

            var stripped = valueSide;
            while (stripped is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                stripped = u.Operand;
            var navSideType = System.Nullable.GetUnderlyingType(
                (TryResolveNavigationFkValueSql(node.Left, out _) ? node.Left : node.Right).Type)
                ?? (TryResolveNavigationFkValueSql(node.Left, out _) ? node.Left : node.Right).Type;
            if (!navSideType.IsAssignableFrom(stripped.Type) && !stripped.Type.IsAssignableFrom(navSideType))
                return false;
            // Only captured/constant instances: mapped-parameter members fail evaluation.
            if (!TryGetConstantValue(valueSide, out _))
                return false;

            TableMapping principalMap;
            try { principalMap = _ctx.GetMapping(navSideType); }
            catch { return false; }
            if (principalMap.KeyColumns.Length != 1) return false;

            var paramName = $"{_provider.ParamPrefix}cp{_compiledParams.Count}";
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramConverters[paramName] = new EntityKeyValueConverter(navSideType, principalMap.KeyColumns[0].Prop);
            _sql.Append(node.NodeType == ExpressionType.Equal
                ? _provider.NullSafeEqual(fkSql, paramName)
                : _provider.NullSafeNotEqual(fkSql, paramName));
            return true;
        }

        /// <summary>
        /// Resolves the dependent-side FK column backing a reference navigation:
        /// the [ForeignKey]/convention marker (which stores the navigation or
        /// principal-type name), the <c>NavNameId</c> convention, or the fluent
        /// relation registered on the principal.
        /// </summary>
        internal static Column? FindReferenceNavForeignKey(
            TableMapping ownerMap, string navName, Type principalType, TableMapping principalMap)
        {
            // Navigation-NAME matches bind tightest: with two navigations to the
            // same principal (Dept + BackupDept), a type-name match would resolve
            // BOTH to the first Dept-typed FK and silently compare the wrong
            // column. Type-name and relation fallbacks only apply when unambiguous.
            foreach (var c in ownerMap.Columns)
            {
                if (c.ForeignKeyPrincipalTypeName != null
                    && string.Equals(c.ForeignKeyPrincipalTypeName, navName, StringComparison.OrdinalIgnoreCase))
                    return c;
            }
            if (ownerMap.ColumnsByName.TryGetValue(navName + "Id", out var conventional))
                return conventional;
            Column? byTypeName = null;
            foreach (var c in ownerMap.Columns)
            {
                if (c.ForeignKeyPrincipalTypeName != null
                    && string.Equals(c.ForeignKeyPrincipalTypeName, principalType.Name, StringComparison.OrdinalIgnoreCase))
                {
                    if (byTypeName != null) { byTypeName = null; break; }
                    byTypeName = c;
                }
            }
            if (byTypeName != null)
                return byTypeName;
            Column? byRelation = null;
            foreach (var rel in principalMap.Relations.Values)
            {
                if (rel.DependentType == ownerMap.Type && !rel.IsComposite)
                {
                    if (byRelation != null) return null;
                    byRelation = rel.ForeignKey;
                }
            }
            return byRelation;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            AppendConstant(node.Value, node.Type);
            return node;
        }

        // Emit CASE WHEN col = <int> THEN '<name>' ... ELSE CAST(col AS TEXT) END for
        // an enum-typed receiver. The ELSE branch mirrors .NET's Enum.ToString behavior
        // on undefined values (returns the integer as text). Names are quote-escaped
        // by doubling single quotes so values like O'Brien (hypothetical) round-trip.
        // Shared between projection (SelectClauseVisitor) and predicates (this class)
        // via the static helper below.
        internal void EmitEnumToStringCase(string columnSql, Type enumType)
        {
            _sql.Append(BuildEnumToStringCase(_provider, columnSql, enumType));
        }

        internal static string BuildEnumToStringCase(DatabaseProvider provider, string columnSql, Type enumType)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("(CASE");
            // GetValuesAsUnderlyingType avoids boxing enum-typed values (and the runtime
            // enum-array instantiation NativeAOT cannot provide); the returned values are
            // already underlying-typed so no Convert.ChangeType round-trip is needed.
            var values = Enum.GetValuesAsUnderlyingType(enumType);
            foreach (var underlying in values)
            {
                var name = Enum.GetName(enumType, underlying!) ?? underlying!.ToString()!;
                sb.Append(" WHEN ").Append(columnSql).Append(" = ").Append(underlying)
                  .Append(" THEN '").Append(name.Replace("'", "''")).Append('\'');
            }
            sb.Append(" ELSE ").Append(provider.GetToStringSql(columnSql)).Append(" END)");
            return sb.ToString();
        }

        /// <summary>
        /// Sister of <see cref="BuildEnumToStringCase"/>: emits a CASE-WHEN cascade
        /// mapping each enum member's name (case-sensitive, matching
        /// <see cref="Enum.Parse{T}(string)"/>'s default) to its underlying integer
        /// value. Used to translate <c>Enum.Parse&lt;T&gt;(stringColumn)</c> in
        /// WHERE / projection. Result rows yielding NULL from the ELSE branch
        /// would surface as a default(enum) — matches the behaviour callers tend
        /// to expect when the data is dirty; strict Parse callers should sanitize
        /// the column server-side first.
        /// </summary>
        internal static string BuildStringToEnumCase(string columnSql, Type enumType)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("(CASE");
            // Same underlying-typed enumeration rationale as BuildEnumToStringCase.
            var values = Enum.GetValuesAsUnderlyingType(enumType);
            foreach (var underlying in values)
            {
                var name = Enum.GetName(enumType, underlying!) ?? underlying!.ToString()!;
                sb.Append(" WHEN ").Append(columnSql).Append(" = '")
                  .Append(name.Replace("'", "''")).Append("' THEN ").Append(underlying);
            }
            sb.Append(" ELSE NULL END)");
            return sb.ToString();
        }

        // Fold a NewExpression whose arguments are all compile-time constants
        // (or evaluate to constants via Expression.Lambda().Compile()) into a
        // single bound parameter at translation time. Without this, an inline
        // `new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc)` on the RHS of
        // a Where comparison emits the raw constructor argument list as bound
        // parameters into the SQL ("col > @p0, @p1, @p2, ..."), producing
        // SQLite syntax errors. The same applies to user-defined value types
        // with constant args.
        protected override Expression VisitNew(NewExpression node)
        {
            if (node.Type == typeof(string)) return base.VisitNew(node);
            // new DateTime(year, month, day) / new DateOnly(year, month, day) /
            // new TimeOnly(hour, minute, second) with at least one non-constant
            // argument can't be folded but each provider has a native
            // from-parts primitive. Route through the provider hook before
            // the constant-fold loop below.
            if ((node.Type == typeof(DateTime) || node.Type == typeof(DateOnly) || node.Type == typeof(TimeOnly))
                && node.Arguments.Count == 3
                && node.Constructor is { } fromPartsCtor
                && fromPartsCtor.GetParameters().Length == 3
                && fromPartsCtor.GetParameters()[0].ParameterType == typeof(int)
                && fromPartsCtor.GetParameters()[1].ParameterType == typeof(int)
                && fromPartsCtor.GetParameters()[2].ParameterType == typeof(int))
            {
                bool anyNonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        anyNonConst = true;
                        break;
                    }
                if (anyNonConst)
                {
                    var aSql = GetSql(node.Arguments[0]);
                    var bSql = GetSql(node.Arguments[1]);
                    var cSql = GetSql(node.Arguments[2]);
                    _sql.Append(node.Type == typeof(DateTime)
                        ? _provider.GetDateTimeFromPartsSql(aSql, bSql, cSql)
                        : node.Type == typeof(DateOnly)
                            ? _provider.GetDateOnlyFromPartsSql(aSql, bSql, cSql)
                            : _provider.GetTimeOnlyFromPartsSql(aSql, bSql, cSql));
                    return node;
                }
            }

            // 6-arg new DateTime(y, m, d, h, mi, s) with at least one column arg.
            if (node.Type == typeof(DateTime)
                && node.Arguments.Count == 6
                && node.Constructor is { } dt6Ctor
                && dt6Ctor.GetParameters() is { Length: 6 } dt6Params
                && dt6Params[0].ParameterType == typeof(int)
                && dt6Params[1].ParameterType == typeof(int)
                && dt6Params[2].ParameterType == typeof(int)
                && dt6Params[3].ParameterType == typeof(int)
                && dt6Params[4].ParameterType == typeof(int)
                && dt6Params[5].ParameterType == typeof(int))
            {
                bool any6NonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        any6NonConst = true;
                        break;
                    }
                if (any6NonConst)
                {
                    var ySql = GetSql(node.Arguments[0]);
                    var mSql = GetSql(node.Arguments[1]);
                    var dSql = GetSql(node.Arguments[2]);
                    var hSql = GetSql(node.Arguments[3]);
                    var miSql = GetSql(node.Arguments[4]);
                    var sSql = GetSql(node.Arguments[5]);
                    _sql.Append(_provider.GetDateTimeFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql));
                    return node;
                }
            }

            // 4-arg new TimeOnly(h, m, s, ms) with at least one column arg.
            if (node.Type == typeof(TimeOnly)
                && node.Arguments.Count == 4
                && node.Constructor is { } to4Ctor
                && to4Ctor.GetParameters() is { Length: 4 } to4Params
                && to4Params[0].ParameterType == typeof(int)
                && to4Params[1].ParameterType == typeof(int)
                && to4Params[2].ParameterType == typeof(int)
                && to4Params[3].ParameterType == typeof(int))
            {
                bool any4NonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        any4NonConst = true;
                        break;
                    }
                if (any4NonConst)
                {
                    var hSql4 = GetSql(node.Arguments[0]);
                    var miSql4 = GetSql(node.Arguments[1]);
                    var sSql4 = GetSql(node.Arguments[2]);
                    var msSql4 = GetSql(node.Arguments[3]);
                    _sql.Append(_provider.GetTimeOnlyFromPartsSql(hSql4, miSql4, sSql4, msSql4));
                    return node;
                }
            }

            // 7-arg new DateTimeOffset(y, m, d, h, mi, s, TimeSpan offset) with at least one
            // column date/time part and a compile-time constant offset. Lower to canonical
            // ISO-8601 text the materialiser parses via DateTimeOffset.Parse — every provider
            // can emit text without needing a native DTO-from-parts function.
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
                && dto7Params[6].ParameterType == typeof(TimeSpan))
            {
                bool anyDtoPartNonConst = false;
                for (int i = 0; i < 6; i++)
                {
                    var a = node.Arguments[i];
                    if (a is not ConstantExpression && !TryGetConstantValue(a, out _))
                    {
                        anyDtoPartNonConst = true;
                        break;
                    }
                }
                if (anyDtoPartNonConst && TryGetTimeSpanConstantArg(node.Arguments[6], out var tsOff))
                {
                    var ySql = GetSql(node.Arguments[0]);
                    var mSql = GetSql(node.Arguments[1]);
                    var dSql = GetSql(node.Arguments[2]);
                    var hSql = GetSql(node.Arguments[3]);
                    var miSql = GetSql(node.Arguments[4]);
                    var sSql = GetSql(node.Arguments[5]);
                    _sql.Append(_provider.GetDateTimeOffsetFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql, tsOff));
                    return node;
                }
            }

            // 7-arg new DateTime(y, m, d, h, mi, s, ms) with at least one column arg.
            if (node.Type == typeof(DateTime)
                && node.Arguments.Count == 7
                && node.Constructor is { } dt7Ctor
                && dt7Ctor.GetParameters() is { Length: 7 } dt7Params
                && dt7Params[0].ParameterType == typeof(int)
                && dt7Params[1].ParameterType == typeof(int)
                && dt7Params[2].ParameterType == typeof(int)
                && dt7Params[3].ParameterType == typeof(int)
                && dt7Params[4].ParameterType == typeof(int)
                && dt7Params[5].ParameterType == typeof(int)
                && dt7Params[6].ParameterType == typeof(int))
            {
                bool any7NonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        any7NonConst = true;
                        break;
                    }
                if (any7NonConst)
                {
                    var ySql = GetSql(node.Arguments[0]);
                    var mSql = GetSql(node.Arguments[1]);
                    var dSql = GetSql(node.Arguments[2]);
                    var hSql = GetSql(node.Arguments[3]);
                    var miSql = GetSql(node.Arguments[4]);
                    var sSql = GetSql(node.Arguments[5]);
                    var msSql = GetSql(node.Arguments[6]);
                    _sql.Append(_provider.GetDateTimeFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql, msSql));
                    return node;
                }
            }
            foreach (var a in node.Arguments)
            {
                if (a is not ConstantExpression && !TryGetConstantValue(a, out _))
                    return base.VisitNew(node);
            }
            try
            {
                var lambda = System.Linq.Expressions.Expression.Lambda(node).Compile();
                var value = lambda.DynamicInvoke();
                // Reserve a placeholder compiled-param slot for every closure-
                // captured MemberExpression argument that the fold consumed.
                // ParameterValueExtractor walks each closure MemberExpression
                // in document order and adds a value to its list; folding the
                // NewExpression inline without compensating leaves N closure
                // values orphaned at the front of the value array, shifting
                // every subsequent @cp binding by N. Same fix shape as
                // 407e03d / eeff6e7 / cf39b61 / 04a0003.
                foreach (var arg in node.Arguments)
                {
                    if (arg is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                }
                AppendConstant(value, node.Type);
                return node;
            }
            catch
            {
                return base.VisitNew(node);
            }
        }

        private string GetRegexPatternSql(Expression patternExpr)
        {
            if (_provider.InlinesConstantRegexArguments
                && TryGetConstantValue(patternExpr, out var raw)
                && raw is string pattern)
            {
                return "\'" + pattern.Replace("\'", "\'\'") + "\'";
            }

            return GetSql(patternExpr);
        }

        private string GetRegexReplacementSql(Expression replacementExpr)
        {
            if (_provider.InlinesConstantRegexArguments
                && TryGetConstantValue(replacementExpr, out var raw)
                && raw is string replacement)
            {
                return "\'" + replacement.Replace("\'", "\'\'") + "\'";
            }

            return GetSql(replacementExpr);
        }

        private static bool TryGetConstantValue(Expression expr, out object? value)
        {
            value = null;
            if (expr is ConstantExpression c) { value = c.Value; return true; }
            try
            {
                value = System.Linq.Expressions.Expression.Lambda(expr).Compile().DynamicInvoke();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Folds a <see cref="TimeSpan"/>-typed argument to its runtime value. Used by
        /// the 7-arg <c>new DateTimeOffset(...)</c> branch whose offset arg must be a
        /// compile-time constant. ETSV-side already permits arbitrary lambda compilation
        /// (the closure fold path at the bottom of <c>VisitNew</c> does the same).
        /// </summary>
        private static bool TryGetTimeSpanConstantArg(Expression expr, out TimeSpan value)
        {
            value = default;
            if (TryGetConstantValue(expr, out var box) && box is TimeSpan ts)
            {
                value = ts;
                return true;
            }
            return false;
        }
    }
}
