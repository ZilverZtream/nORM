using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {
        // Reusable empty dictionary to avoid allocation when count has no parameters.
        // INVARIANT: _emptyParams must NEVER be mutated. It is shared across all callers as a
        // sentinel for "no parameters". Code that needs to add entries must first check
        // ReferenceEquals(parameters, _emptyParams) and allocate a new Dictionary before writing.
        private static readonly Dictionary<string, object> _emptyParams = new();
        // Cached empty includes list and table name lists for simple query path
        private static readonly List<IncludePlan> _emptyIncludes = new();
        private static readonly ConcurrentDictionary<string, List<string>> _simpleQueryTableCache = new();

        /// <summary>
        /// Direct count path that works on the source expression directly,
        /// bypassing the need to wrap in Expression.Call(Queryable.Count) and re-parse.
        /// Saves one MethodCallExpression + Type[] allocation per count call.
        /// </summary>
        internal bool TryDirectCountAsync(Expression sourceExpression, CancellationToken ct, out Task<int> result)
        {
            result = default!;
            if (_ctx.Options.GlobalFilters.Count > 0)
                return false;

            // Unwrap the source expression to find the root and optional Where predicate
            Expression source = sourceExpression;
            LambdaExpression? predicate = null;

            // Unwrap Where
            if (source is MethodCallExpression whereCall && whereCall.Method.DeclaringType == typeof(Queryable) && whereCall.Method.Name == nameof(Queryable.Where))
            {
                predicate = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                source = whereCall.Arguments[0];
            }

            // Unwrap AsNoTracking and similar passthrough methods
            while (source is MethodCallExpression m && m.Arguments.Count == 1 &&
                   m.Method.Name is "AsNoTracking" or "AsNoTrackingWithIdentityResolution" or "AsTracking")
                source = m.Arguments[0];

            if (source is not ConstantExpression constant)
                return false;

            // A FromSqlRaw/FromSqlInterpolated root supplies its own FROM (the raw SQL wrapped as a derived
            // table). This fast path counts the mapped table directly, which would ignore the raw SQL and its
            // filter — defer to the full plan path, which substitutes the raw derived table.
            if (constant.Value is INormRawSqlSource)
                return false;

            var elementType = GetElementType(constant);

            // TPH subtype root: this count fast path emits "COUNT(*) FROM base" with no discriminator
            // predicate, so counting a subtype would include sibling subtypes (silent-wrong). Defer to the
            // full plan path, which appends the discriminator filter for a subtype root.
            if (_ctx.GetMapping(elementType).DiscriminatorValue != null)
                return false;

            if (!TryBuildCountCacheKey(elementType, predicate, allowComplex: false, out var cacheKey))
                return false; // Complex predicates fall back to normal path

            Dictionary<string, object> parameters = _emptyParams;

            if (!_countSqlCache.TryGetValue(cacheKey, out var cached))
            {
                var map = _ctx.GetMapping(elementType);
                string whereClause = string.Empty;
                if (predicate != null)
                {
                    if (!TryBuildCountWhereClause(predicate, map, ref parameters, out whereClause, populateParameters: true))
                        return false;
                }
                AppendTenantCountPredicate(map, ref parameters, ref whereClause);
                var sql2 = $"SELECT COUNT(*) FROM {map.EscTable}{whereClause}";
                bool needsParam = !ReferenceEquals(parameters, _emptyParams);
                _countSqlCache[cacheKey] = (sql2, needsParam);
                cached = (sql2, needsParam);
            }
            else if (cached.NeedsParam)
            {
                // Only re-extract parameter value when the predicate actually uses a parameter
                var map = _ctx.GetMapping(elementType);
                if (predicate != null && !TryBuildCountWhereClause(predicate, map, ref parameters, out _, populateParameters: true))
                    return false;
                AddTenantCountParameter(map, ref parameters);
            }
            var sql = cached.Sql;

            if (_ctx.Options.RetryPolicy != null)
                result = new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCountAsync<int>(sql, parameters, token), ct);
            else
                result = ExecuteCountAsync<int>(sql, parameters, ct);
            return true;
        }

        private bool TryGetCountQuery(Expression expr, out string sql, out Dictionary<string, object> parameters)
        {
            sql = string.Empty;
            parameters = _emptyParams;

            if (_ctx.Options.GlobalFilters.Count > 0)
                return false;

            if (expr is not MethodCallExpression rootCall || rootCall.Method.DeclaringType != typeof(Queryable))
                return false;

            var methodName = rootCall.Method.Name;
            if (methodName is not nameof(Queryable.Count) and not nameof(Queryable.LongCount))
                return false;

            Expression source = rootCall.Arguments[0];
            LambdaExpression? predicate = rootCall.Arguments.Count > 1 ? (LambdaExpression)StripQuotes(rootCall.Arguments[1]) : null;

            if (source is MethodCallExpression whereCall && whereCall.Method.DeclaringType == typeof(Queryable) && whereCall.Method.Name == nameof(Queryable.Where))
            {
                if (predicate != null)
                    return false; // Only support a single predicate source

                predicate = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                source = whereCall.Arguments[0];
            }

            if (source is not ConstantExpression constant)
                return false;

            // A FromSqlRaw/FromSqlInterpolated root supplies its own FROM; counting the mapped table here would
            // ignore the raw SQL and its filter. Defer to the full plan path, which uses the raw derived table.
            if (constant.Value is INormRawSqlSource)
                return false;

            var elementType = GetElementType(constant);
            // TPH subtype root: this count fast path emits "COUNT(*) FROM base" with no discriminator
            // predicate, so counting a subtype would include sibling subtypes (silent-wrong). Defer to the
            // full plan path, which appends the discriminator filter for a subtype root.
            if (_ctx.GetMapping(elementType).DiscriminatorValue != null)
                return false;
            if (!TryBuildCountCacheKey(elementType, predicate, allowComplex: true, out var countCacheKey))
                return false;

            if (_countSqlCache.TryGetValue(countCacheKey, out var cached))
            {
                sql = cached.Sql;
                if (cached.NeedsParam)
                {
                    // Only call GetMapping when actually needed for parameter extraction
                    var map2 = _ctx.GetMapping(elementType);
                    if (predicate != null && !TryBuildCountWhereClause(predicate, map2, ref parameters, out _, populateParameters: true))
                        return false;
                    AddTenantCountParameter(map2, ref parameters);
                }
                return true;
            }

            {
                // First-time path: must call GetMapping for SQL generation
                var map = _ctx.GetMapping(elementType);
                string whereClause = string.Empty;
                bool needsParam = false;

                if (predicate != null)
                {
                    if (!TryBuildCountWhereClause(predicate, map, ref parameters, out whereClause, populateParameters: true))
                        return false;
                    needsParam = !ReferenceEquals(parameters, _emptyParams);
                }

                AppendTenantCountPredicate(map, ref parameters, ref whereClause);
                needsParam = !ReferenceEquals(parameters, _emptyParams);
                sql = $"SELECT COUNT(*) FROM {map.EscTable}{whereClause}";
                _countSqlCache[countCacheKey] = (sql, needsParam);
                return true;
            }
        }

        private static bool TryBuildCountCacheKey(
            Type elementType,
            LambdaExpression? predicate,
            bool allowComplex,
            out CountSqlCacheKey key)
        {
            if (predicate == null)
            {
                key = new CountSqlCacheKey(elementType, string.Empty, CountPredicateShape.None);
                return true;
            }

            if (predicate.Body is MemberExpression { Type: var memberType } member && memberType == typeof(bool))
            {
                key = new CountSqlCacheKey(elementType, member.Member.Name, CountPredicateShape.BoolTrue);
                return true;
            }

            if (predicate.Body is UnaryExpression { NodeType: ExpressionType.Not, Operand: MemberExpression { Type: var negatedType } negatedMember }
                && negatedType == typeof(bool))
            {
                key = new CountSqlCacheKey(elementType, negatedMember.Member.Name, CountPredicateShape.BoolFalse);
                return true;
            }

            if (predicate.Body is BinaryExpression binary && TryGetMemberEquality(binary, out var comparedMember, out var valueExpression))
            {
                if (comparedMember.Type == typeof(bool) &&
                    ExpressionValueExtractor.TryGetConstantValue(valueExpression, out var boolValue) &&
                    boolValue is bool expected)
                {
                    key = new CountSqlCacheKey(
                        elementType,
                        comparedMember.Member.Name,
                        expected ? CountPredicateShape.BoolTrue : CountPredicateShape.BoolFalse);
                    return true;
                }

                key = new CountSqlCacheKey(
                    elementType,
                    comparedMember.Member.Name,
                    IsNullConstant(valueExpression) ? CountPredicateShape.EqualityNull : CountPredicateShape.EqualityValue);
                return true;
            }

            if (allowComplex)
            {
                key = new CountSqlCacheKey(elementType, predicate.Body.ToString(), CountPredicateShape.Complex);
                return true;
            }

            key = default;
            return false;
        }

        private static bool TryGetMemberEquality(BinaryExpression expression, out MemberExpression member, out Expression valueExpression)
        {
            if (expression.NodeType == ExpressionType.Equal && expression.Left is MemberExpression leftMember)
            {
                member = leftMember;
                valueExpression = expression.Right;
                return true;
            }

            if (expression.NodeType == ExpressionType.Equal && expression.Right is MemberExpression rightMember)
            {
                member = rightMember;
                valueExpression = expression.Left;
                return true;
            }

            member = default!;
            valueExpression = default!;
            return false;
        }

        /// <summary>
        /// Q1 fix: returns true when <paramref name="e"/> is a null literal or a Nullable&lt;T&gt; Convert
        /// wrapping a null literal, so cache keys distinguish col==null from col==value.
        /// </summary>
        private static bool IsNullConstant(Expression e) =>
            e is ConstantExpression { Value: null } ||
            (e is UnaryExpression { NodeType: ExpressionType.Convert } ue &&
             ue.Operand is ConstantExpression { Value: null });

        /// <summary>
        /// Builds a WHERE clause for count queries from a simple lambda predicate.
        /// <para>
        /// NOTE on the <c>ref parameters</c> pattern: the <paramref name="parameters"/> argument is passed
        /// by-ref so that this method can replace the caller's <see cref="_emptyParams"/> sentinel with a
        /// freshly-allocated dictionary when the predicate requires a parameter binding. Callers must not
        /// cache the dictionary reference across calls - each invocation may replace it. The <c>out whereClause</c>
        /// is always set (to <see cref="string.Empty"/> on failure) so callers can safely ignore it when the
        /// method returns <c>false</c>.
        /// </para>
        /// </summary>
        private bool TryBuildCountWhereClause(LambdaExpression lambda, TableMapping map, ref Dictionary<string, object> parameters, out string whereClause, bool populateParameters)
        {
            whereClause = string.Empty;

            if (lambda.Body is MemberExpression boolMember && boolMember.Type == typeof(bool))
            {
                if (!map.TryGetColumnForMemberAccess(boolMember, out var boolCol))
                    return false;
                // A bool column with a value converter stores a non-boolean provider value (e.g. 'Y'/'N');
                // the boolean-literal predicate below compares against TRUE/FALSE and misses. Defer to the
                // full pipeline, which converts the compared value.
                if (boolCol.Converter != null)
                    return false;

                whereClause = $" WHERE {FormatCountBooleanPredicate(boolCol.EscCol, expectedValue: true)}";
                return true;
            }

            if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr2
                && notExpr2.Operand is MemberExpression negBoolMember2
                && negBoolMember2.Type == typeof(bool))
            {
                if (!map.TryGetColumnForMemberAccess(negBoolMember2, out var boolCol2))
                    return false;
                if (boolCol2.Converter != null)
                    return false;

                whereClause = $" WHERE {FormatCountBooleanPredicate(boolCol2.EscCol, expectedValue: false)}";
                return true;
            }

            if (lambda.Body is BinaryExpression be && TryGetMemberEquality(be, out var me, out var valueExpression))
            {
                if (!map.TryGetColumnForMemberAccess(me, out var column))
                    return false;

                if (!ExpressionValueExtractor.TryGetConstantValue(valueExpression, out var constValue))
                    return false;

                if (me.Type == typeof(bool) && constValue is bool boolValue)
                {
                    // Bool column with a converter: the boolean literal would not match the stored provider
                    // value — defer to the full pipeline.
                    if (column.Converter != null)
                        return false;
                    whereClause = $" WHERE {FormatCountBooleanPredicate(column.EscCol, boolValue)}";
                    return true;
                }

                if (constValue == null)
                {
                    whereClause = $" WHERE {column.EscCol} IS NULL";
                    return true;
                }

                // DateTimeOffset and TimeSpan equality must lower in the full translator (UTC-instant for
                // DTO; numeric fractional-seconds for TimeSpan): a raw `col = @p` lexically under-counts an
                // equal value stored in a different representation. Defer both.
                var eqClrType = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
                if (eqClrType == typeof(DateTimeOffset) || eqClrType == typeof(TimeSpan) || eqClrType == typeof(TimeOnly))
                    return false;

                // SQLite stores decimal as TEXT; a raw `col = @p` here is a lexical string compare that
                // counts the wrong rows when the stored scale differs ("24.500" = "24.5" is false though
                // 24.500m == 24.5m). Defer decimal equality to the full translator, which canonicalizes both
                // sides. Native-DECIMAL providers compare exactly and keep this fast path.
                if (_ctx.RawProvider.StoresDecimalAsText && (Nullable.GetUnderlyingType(me.Type) ?? me.Type) == typeof(decimal))
                    return false;

                // Apply the column's value converter so the count matches the STORED representation, exactly
                // as the WHERE fast path and the full pipeline do. Binding the raw model value counts the
                // wrong rows (typically zero) — e.g. Score == 42 against a column storing 42 as -42.
                var boundValue = column.Converter != null
                    ? column.Converter.ConvertToProvider(constValue)
                    : constValue;

                var paramName = _ctx.RawProvider.ParamPrefix + "p0";
                // C# string/char equality is ordinal; CI-collation providers (MySQL, SQL Server) fold case
                // on a bare `=`, so Count(predicate) would count "ABC"/"AbC" that the full translator (and
                // the sibling read fast paths) exclude. Apply the same sargable ordinal wrap here.
                var colClrType = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
                whereClause = (colClrType == typeof(string) || colClrType == typeof(char))
                              && _ctx.RawProvider.DefaultStringEqualityIsCaseInsensitive
                    ? $" WHERE {_ctx.RawProvider.OrdinalStringEqualSql(column.EscCol, paramName)}"
                    : $" WHERE {column.EscCol} = {paramName}";

                if (populateParameters)
                {
                    // Only allocate a new dictionary when we actually need to add parameters
                    if (ReferenceEquals(parameters, _emptyParams))
                        parameters = new Dictionary<string, object>(1);
                    parameters[paramName] = boundValue!;
                }

                return true;
            }

            return false;
        }

        private string FormatCountBooleanPredicate(string expressionSql, bool expectedValue)
            => $"{expressionSql} = {(expectedValue ? _ctx.RawProvider.BooleanTrueLiteral : _ctx.RawProvider.BooleanFalseLiteral)}";

        private void AppendTenantCountPredicate(TableMapping map, ref Dictionary<string, object> parameters, ref string whereClause)
        {
            if (_ctx.Options.TenantProvider == null)
                return;

            var tenantCol = _ctx.RequireTenantColumn(map, "count query");
            var paramName = AddTenantCountParameter(map, ref parameters);
            var tenantPredicate = $"{tenantCol.EscCol} = {paramName}";
            whereClause = string.IsNullOrEmpty(whereClause)
                ? " WHERE " + tenantPredicate
                : whereClause + " AND " + tenantPredicate;
        }

        private string AddTenantCountParameter(TableMapping map, ref Dictionary<string, object> parameters)
        {
            if (_ctx.Options.TenantProvider == null)
                return string.Empty;

            var tenantCol = _ctx.RequireTenantColumn(map, "count query");
            var paramName = _ctx.RawProvider.ParamPrefix + "__tenant";
            if (ReferenceEquals(parameters, _emptyParams))
                parameters = new Dictionary<string, object>(1);
            parameters[paramName] = _ctx.GetRequiredTenantId(tenantCol, map.Type, "count query");
            return paramName;
        }

        private Task<TResult> ExecuteCountAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            // Split into fast (no logger) and slow (with logger) paths.
            // The fast path avoids Stopwatch allocation and logger null checks.
            if (_ctx.Options.Logger != null)
                return ExecuteCountSlowAsync<TResult>(sql, parameters, ct);
            return ExecuteCountFastAsync<TResult>(sql, parameters, ct);
        }

        private Task<TResult> ExecuteCountFastAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            // Non-async entry point - avoids state machine when connection is already open.
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCountFastSlowAsync<TResult>(ensureTask, sql, parameters, ct);

            // Parameterless count queries are safe to pool per context. This avoids repeated
            // command allocation on hot CountAsync paths while serializing use of the command.
            if (ReferenceEquals(parameters, _emptyParams) && _ctx.Options.CommandInterceptors.Count == 0)
            {
                var entry = _pooledCountCommands.GetOrAdd(sql, static (s, ctx) =>
                {
                    var c = ctx.CreateCommand();
                    c.CommandText = s;
                    // Prepare() is optional - some providers throw NotSupportedException or InvalidOperationException.
                    try { c.Prepare(); } catch (NotSupportedException) { } catch (InvalidOperationException) { }
                    return (c, new object());
                }, _ctx);

                lock (entry.Lock)
                {
                    ct.ThrowIfCancellationRequested();
                    // Rebind CurrentTransaction on every use - CreateCommand() only binds the
                    // transaction at creation time; reuse across transaction changes would run
                    // the count against a stale (or null) transaction binding.
                    entry.Cmd.Transaction = _ctx.CurrentTransaction;
                    var scalar = entry.Cmd.ExecuteScalarWithInterceptionSerialized(_ctx);
                    if (scalar == null || scalar is DBNull)
                        return Task.FromResult(default(TResult)!);
                    return Task.FromResult(ConvertScalarResult<TResult>(scalar));
                }
            }

            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            // For providers without true async I/O (SQLite), use sync ExecuteScalar directly
            // to avoid the ValueTask/Task wrapper overhead from ExecuteScalarAsync.
            if (_ctx.RawProvider.PrefersSyncExecution)
            {
                ct.ThrowIfCancellationRequested();
                var scalar = cmd.ExecuteScalarWithInterceptionSerializedAndDispose(_ctx);
                if (scalar == null || scalar is DBNull)
                    return Task.FromResult(default(TResult)!);
                return Task.FromResult(ConvertScalarResult<TResult>(scalar));
            }

            var scalarTask = cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct);
            if (scalarTask.IsCompletedSuccessfully)
            {
                var scalar = scalarTask.Result;
                cmd.Dispose();
                if (scalar == null || scalar is DBNull)
                    return Task.FromResult(default(TResult)!);
                return Task.FromResult(ConvertScalarResult<TResult>(scalar));
            }
            return ExecuteCountFinalizeAsync<TResult>(scalarTask, cmd);
        }

        private async Task<TResult> ExecuteCountFastSlowAsync<TResult>(Task<DbConnection> ensureTask, string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var scalar = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            if (scalar == null || scalar is DBNull)
                return default!;
            return ConvertScalarResult<TResult>(scalar);
        }

        private async Task<TResult> ExecuteCountFinalizeAsync<TResult>(Task<object?> scalarTask, DbCommand cmd)
        {
            try
            {
                var scalar = await scalarTask.ConfigureAwait(false);
                if (scalar == null || scalar is DBNull)
                    return default!;
                return ConvertScalarResult<TResult>(scalar);
            }
            finally
            {
                cmd.Dispose();
            }
        }

        private async Task<TResult> ExecuteCountSlowAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var scalar = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw.Elapsed, scalar == null || scalar is DBNull ? 0 : 1);
            if (scalar == null || scalar is DBNull)
                return default!;

            return ConvertScalarResult<TResult>(scalar);
        }

        private TResult ExecuteCountSync<TResult>(string sql, Dictionary<string, object> parameters)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var scalar = cmd.ExecuteScalarWithInterceptionSerializedAndDispose(_ctx);
            sw?.Stop();
            if (sw != null)
                _ctx.Options.Logger?.LogQuery(sql, parameters, sw.Elapsed, scalar == null || scalar is DBNull ? 0 : 1);
            if (scalar == null || scalar is DBNull)
                return default!;

            return ConvertScalarResult<TResult>(scalar);
        }

    }
}
