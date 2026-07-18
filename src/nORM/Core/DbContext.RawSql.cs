using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Executes the provided SQL and materializes the results into instances of
        /// <typeparamref name="T"/> without tracking them in the <see cref="ChangeTracker"/>.
        /// </summary>
        /// <typeparam name="T">Type to materialize each row to.</typeparam>
        /// <param name="sql">Raw SQL query to execute.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Optional parameters for the SQL query.</param>
        /// <returns>A list of entities populated from the query results.</returns>
        public Task<List<T>> QueryUnchangedAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(QueryUnchangedAsync));
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, sql);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                NormValidator.ValidateRawQuerySql(sql, ctx.RawProvider, paramDict);

                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var mapping = ctx.GetMapping(typeof(T));

                var colOrdinals = ResolveRawResultOrdinals(reader, mapping, typeof(T));

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add(MaterializeRawEntity<T>(reader, colOrdinals));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);
        }

        /// <summary>
        /// Streams raw SQL results without tracking. Intended for generated provider-bound scaffolding wrappers.
        /// </summary>
        protected async IAsyncEnumerable<T> QueryUnchangedStreamAsync<T>(string sql, [EnumeratorCancellation] CancellationToken ct = default, params object[] parameters) where T : class, new()
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(QueryUnchangedAsync));
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            await using var cmd = CommandPool.Get(RawConnection, sql);
            if (CurrentTransaction != null)
                cmd.Transaction = CurrentTransaction;
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
            var paramDict = AddParametersFast(cmd, parameters);
            NormValidator.ValidateRawQuerySql(sql, RawProvider, paramDict);

            var mapping = GetMapping(typeof(T));
            var count = 0;
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.SequentialAccess, ct).ConfigureAwait(false);
            var colOrdinals = ResolveRawResultOrdinals(reader, mapping, typeof(T));
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                count++;
                yield return MaterializeRawEntity<T>(reader, colOrdinals);
            }

            Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, count);
            cmd.Parameters.Clear();
        }

        /// <summary>
        /// Executes interpolated SQL with every interpolation hole converted into a
        /// database parameter before execution.
        /// </summary>
        public Task<List<T>> QueryUnchangedInterpolatedAsync<T>(FormattableString sql, CancellationToken ct = default) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(sql);
            var prepared = BuildInterpolatedSql(sql);
            return QueryUnchangedAsync<T>(prepared.Sql, ct, prepared.Parameters);
        }

        private static object CoerceRawValue(object raw, Type propType)
        {
            if (propType.IsEnum) return Enum.ToObject(propType, raw);

            if (propType == typeof(Guid))
            {
                if (raw is string s) return Guid.Parse(s);
                if (raw is byte[] b && b.Length == 16) return new Guid(b);
            }

            if (propType == typeof(DateOnly))
            {
                if (raw is DateTime dt) return DateOnly.FromDateTime(dt);
                if (raw is DateTimeOffset dto) return DateOnly.FromDateTime(dto.DateTime);
                if (raw is string s) return DateOnly.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
            }

            if (propType == typeof(TimeOnly))
            {
                if (raw is TimeSpan ts) return TimeOnly.FromTimeSpan(ts);
                if (raw is DateTime dt) return TimeOnly.FromDateTime(dt);
                if (raw is long l) return TimeOnly.FromTimeSpan(TimeSpan.FromTicks(l));
                if (raw is string s)
                {
                    if (TimeSpan.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, out var ts2))
                        return TimeOnly.FromTimeSpan(ts2);
                    return TimeOnly.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
                }
            }

            if (propType == typeof(DateTimeOffset))
            {
                if (raw is DateTime dt) return new DateTimeOffset(dt);
                if (raw is string s) return DateTimeOffset.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
                if (raw is long l) return DateTimeOffset.FromUnixTimeMilliseconds(l);
            }

            if (propType == typeof(TimeSpan))
            {
                if (raw is long l) return TimeSpan.FromTicks(l);
                if (raw is string s) return TimeSpan.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
            }

            if (propType == typeof(char) && raw is string str && str.Length > 0)
                return str[0];

            return Convert.ChangeType(raw, propType, System.Globalization.CultureInfo.InvariantCulture);
        }

        private static (Column Col, int Ordinal)[] ResolveRawResultOrdinals(DbDataReader reader, TableMapping mapping, Type targetType)
        {
            var fieldCount = reader.FieldCount;
            var nameToOrdinal = new Dictionary<string, int>(fieldCount, StringComparer.OrdinalIgnoreCase);
            var duplicateColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < fieldCount; i++)
            {
                var name = reader.GetName(i);
                if (!nameToOrdinal.TryAdd(name, i))
                    duplicateColumnNames.Add(name);
            }

            var colOrdinals = new (Column Col, int Ordinal)[mapping.Columns.Length];
            for (int i = 0; i < mapping.Columns.Length; i++)
            {
                var col = mapping.Columns[i];
                if (duplicateColumnNames.Contains(col.Name))
                    throw new InvalidOperationException(
                        $"Column '{col.Name}' appears multiple times in the raw SQL result for {targetType.Name}. " +
                        "Use unique aliases for projected columns so materialization is unambiguous.");
                if (!nameToOrdinal.TryGetValue(col.Name, out var ordinal))
                    throw new InvalidOperationException(
                        $"Column '{col.Name}' expected by {targetType.Name} is not present in the raw SQL result. " +
                        "Include all mapped columns in the SELECT list, or use column aliases that match property names.");
                colOrdinals[i] = (col, ordinal);
            }

            return colOrdinals;
        }

        private static T MaterializeRawEntity<T>(DbDataReader reader, (Column Col, int Ordinal)[] colOrdinals) where T : class, new()
        {
            var instance = Activator.CreateInstance<T>();
            foreach (var (col, ordinal) in colOrdinals)
            {
                if (reader.IsDBNull(ordinal)) continue;
                var raw = reader.GetValue(ordinal);
                var propType = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
                if (raw.GetType() != propType)
                    raw = CoerceRawValue(raw, propType);
                col.Setter(instance, raw);
            }
            return instance;
        }

        /// <summary>
        /// Executes a raw SQL query and materializes the results into instances of
        /// <typeparamref name="T"/>.
        /// </summary>
        public Task<List<T>> FromSqlRawAsync<T>(string sql, CancellationToken ct = default, params object[] parameters) where T : class, new()
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(FromSqlRawAsync));
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, sql);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                NormValidator.ValidateRawQuerySql(sql, ctx.RawProvider, paramDict);

                var mapping = ctx.GetMapping(typeof(T));
                var list = new List<T>();

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var colOrdinals = ResolveRawResultOrdinals(reader, mapping, typeof(T));
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add(MaterializeRawEntity<T>(reader, colOrdinals));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);
        }

        /// <summary>
        /// Executes interpolated SQL with every interpolation hole converted into a
        /// database parameter before execution.
        /// </summary>
        public Task<List<T>> FromSqlInterpolatedAsync<T>(FormattableString sql, CancellationToken ct = default) where T : class, new()
        {
            ArgumentNullException.ThrowIfNull(sql);
            var prepared = BuildInterpolatedSql(sql);
            return FromSqlRawAsync<T>(prepared.Sql, ct, prepared.Parameters);
        }

        /// <summary>
        /// Executes raw SQL and materializes each row into <typeparamref name="T"/>, which — unlike
        /// <see cref="FromSqlRawAsync{T}(string, CancellationToken, object[])"/> — may be a scalar
        /// (<c>int</c>, <c>string</c>, <c>Guid</c>, …) read from the first column, or any type with a
        /// parameterless constructor whose settable properties are bound by column name. Matches EF Core's
        /// <c>Database.SqlQueryRaw&lt;T&gt;</c> and is the raw-SQL path for counts, aggregates, and DTOs.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("SqlQuery materializes an arbitrary result type by reflection; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("SqlQuery reflects over the result type's constructor and properties; trimming may remove them. See docs/aot-trimming.md.")]
        public Task<List<T>> SqlQueryRawAsync<T>(string sql, CancellationToken ct = default, params object[] parameters)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(SqlQueryRawAsync));
            ArgumentNullException.ThrowIfNull(sql);
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, sql);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                NormValidator.ValidateRawQuerySql(sql, ctx.RawProvider, paramDict);

                var list = new List<T>();
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var materialize = BuildRawResultMaterializer<T>(reader);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add(materialize(reader));

                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);
        }

        /// <summary>Interpolated counterpart of <see cref="SqlQueryRawAsync{T}(string, CancellationToken, object[])"/>.</summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("SqlQuery materializes an arbitrary result type by reflection; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("SqlQuery reflects over the result type's constructor and properties; trimming may remove them. See docs/aot-trimming.md.")]
        public Task<List<T>> SqlQueryInterpolatedAsync<T>(FormattableString sql, CancellationToken ct = default)
        {
            ArgumentNullException.ThrowIfNull(sql);
            var prepared = BuildInterpolatedSql(sql);
            return SqlQueryRawAsync<T>(prepared.Sql, ct, prepared.Parameters);
        }

        private static bool IsScalarResultType(Type t)
            => t.IsPrimitive || t.IsEnum
               || t == typeof(string) || t == typeof(decimal) || t == typeof(Guid)
               || t == typeof(DateTime) || t == typeof(DateTimeOffset) || t == typeof(DateOnly)
               || t == typeof(TimeOnly) || t == typeof(TimeSpan) || t == typeof(byte[]);

        /// <summary>
        /// Builds a per-row materializer for <typeparamref name="T"/>: a scalar reads and coerces the first
        /// column; a complex type is constructed via its parameterless constructor and its settable
        /// properties are bound from same-named columns (case-insensitive).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("SqlQuery constructs an arbitrary result type by reflection; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("SqlQuery reflects over the result type's constructor and properties; trimming may remove them. See docs/aot-trimming.md.")]
        private static Func<DbDataReader, T> BuildRawResultMaterializer<T>(DbDataReader reader)
        {
            var type = typeof(T);
            var underlying = Nullable.GetUnderlyingType(type) ?? type;

            if (IsScalarResultType(underlying))
            {
                return r =>
                {
                    if (r.IsDBNull(0))
                        return default!;
                    var raw = r.GetValue(0);
                    if (raw.GetType() != underlying)
                        raw = CoerceRawValue(raw, underlying);
                    return (T)raw;
                };
            }

            var ctor = type.GetConstructor(Type.EmptyTypes)
                ?? throw new NormUsageException(
                    $"SqlQuery cannot materialize '{type.Name}': it must be a scalar type or have a public parameterless constructor.");

            var nameToOrdinal = new Dictionary<string, int>(reader.FieldCount, StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
                nameToOrdinal[reader.GetName(i)] = i;

            var binders = new List<(int Ordinal, System.Reflection.PropertyInfo Prop, Type Target)>();
            foreach (var prop in type.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
            {
                if (!prop.CanWrite || prop.GetIndexParameters().Length > 0)
                    continue;
                if (nameToOrdinal.TryGetValue(prop.Name, out var ordinal))
                    binders.Add((ordinal, prop, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType));
            }

            return r =>
            {
                var obj = (T)ctor.Invoke(null)!;
                foreach (var (ordinal, prop, target) in binders)
                {
                    if (r.IsDBNull(ordinal))
                        continue;
                    var raw = r.GetValue(ordinal);
                    if (raw.GetType() != target)
                        raw = CoerceRawValue(raw, target);
                    prop.SetValue(obj, raw);
                }
                return obj;
            };
        }

        /// <summary>
        /// Executes a raw non-query command (INSERT/UPDATE/DELETE/DDL) and returns the affected row
        /// count. Backs <see cref="DatabaseFacade.ExecuteSqlRawAsync(string, System.Threading.CancellationToken, object[])"/>.
        /// </summary>
        internal Task<int> ExecuteSqlRawCoreAsync(string sql, object[] parameters, CancellationToken ct)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteSqlRawCoreAsync));
            ArgumentNullException.ThrowIfNull(sql);
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, sql);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Update, cmd.CommandText));
                var paramDict = ctx.AddParametersFast(cmd, parameters);
                NormValidator.ValidateRawNonQuerySql(sql, ctx.RawProvider, paramDict);
                var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, token).ConfigureAwait(false);
                ctx.Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, affected);
                cmd.Parameters.Clear();
                ctx.InvalidateResultCacheForRawSql(sql);
                return affected;
            }, ct);
        }

        /// <summary>Synchronous counterpart of <see cref="ExecuteSqlRawCoreAsync"/>.</summary>
        internal int ExecuteSqlRawCore(string sql, object[] parameters)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteSqlRawCore));
            ArgumentNullException.ThrowIfNull(sql);
            EnsureConnection();
            var sw = Stopwatch.StartNew();
            using var cmd = CommandPool.Get(RawConnection, sql);
            if (CurrentTransaction != null)
                cmd.Transaction = CurrentTransaction;
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Update, cmd.CommandText));
            var paramDict = AddParametersFast(cmd, parameters);
            NormValidator.ValidateRawNonQuerySql(sql, RawProvider, paramDict);
            var affected = cmd.ExecuteNonQueryWithInterception(this);
            Options.Logger?.LogQuery(sql, paramDict, sw.Elapsed, affected);
            cmd.Parameters.Clear();
            InvalidateResultCacheForRawSql(sql);
            return affected;
        }

        /// <summary>Executes interpolated non-query SQL, parameterizing each hole first.</summary>
        internal Task<int> ExecuteSqlInterpolatedCoreAsync(FormattableString sql, CancellationToken ct)
        {
            ArgumentNullException.ThrowIfNull(sql);
            var prepared = BuildInterpolatedSql(sql);
            return ExecuteSqlRawCoreAsync(prepared.Sql, prepared.Parameters, ct);
        }

        /// <summary>Synchronous counterpart of <see cref="ExecuteSqlInterpolatedCoreAsync"/>.</summary>
        internal int ExecuteSqlInterpolatedCore(FormattableString sql)
        {
            ArgumentNullException.ThrowIfNull(sql);
            var prepared = BuildInterpolatedSql(sql);
            return ExecuteSqlRawCore(prepared.Sql, prepared.Parameters);
        }

        // Matches the target table after a DML/DDL verb so a raw write can invalidate the result cache
        // for that table (EF does no cache invalidation for raw SQL; this is a best-effort improvement).
        private static readonly System.Text.RegularExpressions.Regex s_rawSqlTargetTableRegex = new(
            @"(?:UPDATE|INSERT\s+INTO|DELETE\s+FROM|MERGE\s+INTO|TRUNCATE\s+TABLE)\s+(?<t>[`""\[]?[A-Za-z_][A-Za-z0-9_]*[`""\]]?(?:\s*\.\s*[`""\[]?[A-Za-z_][A-Za-z0-9_]*[`""\]]?)?)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

        /// <summary>
        /// Best-effort result-cache invalidation for a raw non-query: parses the target table of each
        /// UPDATE/INSERT/DELETE/MERGE/TRUNCATE and invalidates its cache tag. Complex or unparseable SQL
        /// invalidates nothing (matching EF, which never invalidates for raw SQL).
        /// </summary>
        private void InvalidateResultCacheForRawSql(string sql)
        {
            var cache = Options.CacheProvider;
            if (cache == null)
                return;
            foreach (System.Text.RegularExpressions.Match match in s_rawSqlTargetTableRegex.Matches(sql))
            {
                var token = match.Groups["t"].Value;
                var lastDot = token.LastIndexOf('.');
                if (lastDot >= 0)
                    token = token.Substring(lastDot + 1);
                token = token.Trim().Trim('`', '"', '[', ']', ' ');
                if (token.Length > 0)
                    cache.InvalidateTag(token);
            }
        }

        private (string Sql, object[] Parameters) BuildInterpolatedSql(FormattableString sql)
        {
            var arguments = sql.GetArguments();
            if (arguments.Length == 0)
                return (sql.Format, Array.Empty<object>());

            var parameterNames = new object[arguments.Length];
            var parameters = new object[arguments.Length];
            for (var i = 0; i < arguments.Length; i++)
            {
                parameterNames[i] = $"{_p.ParamPrefix}p{i}";
                parameters[i] = arguments[i] ?? DBNull.Value;
            }

            var commandText = string.Format(CultureInfo.InvariantCulture, sql.Format, parameterNames);
            return (commandText, parameters);
        }

        /// <summary>
        /// Executes a stored procedure and materializes the first result set into
        /// instances of <typeparamref name="T"/>.
        /// </summary>
        public Task<List<T>> ExecuteStoredProcedureAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null) where T : class, new()
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteStoredProcedureAsync));
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, procedureName);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandType = ctx._p.StoredProcedureCommandType;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

                var paramDict = new Dictionary<string, object>();
                SetStoredProcedureParameters(cmd, ctx._p, parameters, paramDict);

                ValidateStoredProcedureCommandText(procedureName, ctx._p, paramDict);

                var mapping = ctx.GetMapping(typeof(T));
                var list = new List<T>();

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var colOrdinals = ResolveRawResultOrdinals(reader, mapping, typeof(T));
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add(MaterializeRawEntity<T>(reader, colOrdinals));

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return list;
            }, ct);
        }

        /// <summary>
        /// Executes a stored procedure or provider-bound routine that does not
        /// return a result set.
        /// </summary>
        /// <param name="procedureName">Provider routine name or invocation text.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Optional routine parameters.</param>
        /// <returns>The provider-reported affected row count.</returns>
        public Task<int> ExecuteStoredProcedureNonQueryAsync(string procedureName, CancellationToken ct = default, object? parameters = null)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteStoredProcedureNonQueryAsync));
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, procedureName);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandType = ctx._p.StoredProcedureCommandType;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

                var paramDict = new Dictionary<string, object>();
                SetStoredProcedureParameters(cmd, ctx._p, parameters, paramDict);

                ValidateStoredProcedureCommandText(procedureName, ctx._p, paramDict, allowTextNonQuery: true);

                var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, token).ConfigureAwait(false);
                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, affected);
                cmd.Parameters.Clear();
                return affected;
            }, ct);
        }

        /// <summary>
        /// Executes a stored procedure or provider-bound routine that does not
        /// return a result set but does expose output parameters.
        /// </summary>
        /// <param name="procedureName">Provider routine name or invocation text.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="parameters">Optional routine parameters.</param>
        /// <param name="outputParameters">Output, input/output, or return-value parameter definitions.</param>
        /// <returns>The affected row count and output parameter values.</returns>
        public Task<StoredProcedureNonQueryResult> ExecuteStoredProcedureNonQueryWithOutputAsync(
            string procedureName,
            CancellationToken ct = default,
            object? parameters = null,
            params OutputParameter[] outputParameters)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteStoredProcedureNonQueryWithOutputAsync));
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, procedureName);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandType = ctx._p.StoredProcedureCommandType;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

                var paramDict = new Dictionary<string, object>();
                SetStoredProcedureParameters(cmd, ctx._p, parameters, paramDict);
                var outputParamMap = AddStoredProcedureOutputParameters(cmd, ctx._p, outputParameters);

                ValidateStoredProcedureCommandText(procedureName, ctx._p, paramDict, allowTextNonQuery: true);

                var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, token).ConfigureAwait(false);
                var outputs = ExtractStoredProcedureOutputs(outputParamMap);

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, affected);
                cmd.Parameters.Clear();
                return new StoredProcedureNonQueryResult(affected, outputs);
            }, ct);
        }

        /// <summary>
        /// Streams the results of a stored procedure as an <see cref="IAsyncEnumerable{T}"/>.
        /// </summary>
        public async IAsyncEnumerable<T> ExecuteStoredProcedureAsAsyncEnumerable<T>(string procedureName, [EnumeratorCancellation] CancellationToken ct = default, object? parameters = null) where T : class, new()
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteStoredProcedureAsAsyncEnumerable));
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            await using var cmd = CommandPool.Get(Connection, procedureName);
            if (CurrentTransaction != null)
                cmd.Transaction = CurrentTransaction;
            cmd.CommandType = _p.StoredProcedureCommandType;
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

            var paramDict = new Dictionary<string, object>();
            SetStoredProcedureParameters(cmd, _p, parameters, paramDict);

            ValidateStoredProcedureCommandText(procedureName, Provider, paramDict);

            var mapping = GetMapping(typeof(T));
            var count = 0;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.SequentialAccess, ct).ConfigureAwait(false);
            var colOrdinals = ResolveRawResultOrdinals(reader, mapping, typeof(T));
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = MaterializeRawEntity<T>(reader, colOrdinals);
                count++;
                yield return entity;
            }

            Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, count);
            cmd.Parameters.Clear();
        }

        /// <summary>
        /// Executes a stored procedure that returns both a result set and output
        /// parameters.
        /// </summary>
        public Task<StoredProcedureResult<T>> ExecuteStoredProcedureWithOutputAsync<T>(string procedureName, CancellationToken ct = default, object? parameters = null, params OutputParameter[] outputParameters) where T : class, new()
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ExecuteStoredProcedureWithOutputAsync));
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var sw = Stopwatch.StartNew();
                await using var cmd = CommandPool.Get(ctx.RawConnection, procedureName);
                if (ctx.CurrentTransaction != null)
                    cmd.Transaction = ctx.CurrentTransaction;
                cmd.CommandType = ctx._p.StoredProcedureCommandType;
                cmd.CommandTimeout = ToSecondsClamped(ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.StoredProcedure, cmd.CommandText));

                var paramDict = new Dictionary<string, object>();
                SetStoredProcedureParameters(cmd, ctx._p, parameters, paramDict);

                var outputParamMap = AddStoredProcedureOutputParameters(cmd, ctx._p, outputParameters);

                ValidateStoredProcedureCommandText(procedureName, ctx._p, paramDict);

                var mapping = ctx.GetMapping(typeof(T));
                var list = new List<T>();

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, token).ConfigureAwait(false);
                var colOrdinals = ResolveRawResultOrdinals(reader, mapping, typeof(T));
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                    list.Add(MaterializeRawEntity<T>(reader, colOrdinals));
                await reader.DisposeAsync().ConfigureAwait(false);

                var outputs = ExtractStoredProcedureOutputs(outputParamMap);

                ctx.Options.Logger?.LogQuery(procedureName, paramDict, sw.Elapsed, list.Count);
                cmd.Parameters.Clear();
                return new StoredProcedureResult<T>(list, outputs);
            }, ct);
        }

        private static Dictionary<string, DbParameter> AddStoredProcedureOutputParameters(
            DbCommand cmd,
            Providers.DatabaseProvider provider,
            IEnumerable<OutputParameter> outputParameters)
        {
            var outputParamMap = new Dictionary<string, DbParameter>();
            foreach (var op in outputParameters)
            {
                if (!IsSafeOutputParamName(op.Name))
                    throw new NormUsageException($"Invalid output parameter name: '{op.Name}'. " +
                        "Parameter names must start with a letter or underscore and contain only letters, digits, and underscores.");
                if (op.Direction is not (ParameterDirection.Output or ParameterDirection.InputOutput or ParameterDirection.ReturnValue))
                {
                    throw new NormUsageException(
                        $"Invalid output parameter direction for '{op.Name}': {op.Direction}. " +
                        "Use Output, InputOutput, or ReturnValue.");
                }

                var pName = provider.ParamPrefix + op.Name;
                var p = FindParameter(cmd.Parameters, pName);
                if (p is null)
                {
                    p = cmd.CreateParameter();
                    p.ParameterName = pName;
                    cmd.Parameters.Add(p);
                }

                p.DbType = op.DbType;
                p.Direction = op.Direction;
                if (op.Size.HasValue) p.Size = op.Size.Value;
                if (op.Precision.HasValue) p.Precision = op.Precision.Value;
                if (op.Scale.HasValue) p.Scale = op.Scale.Value;
                if (op.Direction == ParameterDirection.InputOutput)
                {
                    if (op.Value is not null)
                        p.Value = op.Value;
                    else if (p.Value is null)
                        p.Value = DBNull.Value;
                }

                outputParamMap[op.Name] = p;
            }

            return outputParamMap;
        }

        private static IReadOnlyDictionary<string, object?> ExtractStoredProcedureOutputs(Dictionary<string, DbParameter> outputParamMap)
        {
            var outputs = new Dictionary<string, object?>();
            foreach (var kv in outputParamMap)
                outputs[kv.Key] = kv.Value.Value == DBNull.Value ? null : kv.Value.Value;

            return outputs;
        }

        private static DbParameter? FindParameter(DbParameterCollection parameters, string parameterName)
        {
            foreach (DbParameter parameter in parameters)
            {
                if (string.Equals(parameter.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                    return parameter;
            }

            return null;
        }

        private static void SetStoredProcedureParameters(
            DbCommand cmd,
            Providers.DatabaseProvider provider,
            object? parameters,
            Dictionary<string, object> paramDict)
        {
            if (parameters is null)
                return;

            if (TrySetStoredProcedureDictionaryParameters(cmd, provider, parameters, paramDict))
                return;

            var props = parameters.GetType().GetProperties();
            var span = new (string name, object value)[props.Length];
            var hasProviderParameters = false;
            for (int i = 0; i < props.Length; i++)
            {
                var pName = provider.ParamPrefix + props[i].Name;
                var pValue = props[i].GetValue(parameters) ?? DBNull.Value;
                span[i] = (pName, pValue);
                paramDict[pName] = pValue;
                hasProviderParameters |= pValue is DbParameter;
            }

            if (hasProviderParameters)
            {
                cmd.Parameters.Clear();
                foreach (var (name, value) in span)
                {
                    if (value is DbParameter providerParameter)
                    {
                        providerParameter.ParameterName = name;
                        cmd.Parameters.Add(providerParameter);
                        paramDict[name] = providerParameter.Value ?? DBNull.Value;
                    }
                    else
                    {
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = name;
                        nORM.Query.ParameterAssign.AssignValue(parameter, value);
                        cmd.Parameters.Add(parameter);
                    }
                }

                return;
            }

            cmd.SetParametersFast(span);
        }

        private static bool TrySetStoredProcedureDictionaryParameters(
            DbCommand cmd,
            Providers.DatabaseProvider provider,
            object parameters,
            Dictionary<string, object> paramDict)
        {
            var parameterType = parameters.GetType();
            var kvpEnumerable = parameterType
                .GetInterfaces()
                .Concat(new[] { parameterType })
                .FirstOrDefault(type =>
                    type.IsGenericType
                    && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                    && type.GetGenericArguments()[0].IsGenericType
                    && type.GetGenericArguments()[0].GetGenericTypeDefinition() == typeof(KeyValuePair<,>)
                    && type.GetGenericArguments()[0].GetGenericArguments()[0] == typeof(string));
            if (kvpEnumerable is null || parameters is not System.Collections.IEnumerable enumerable)
                return false;

            var keyValueType = kvpEnumerable.GetGenericArguments()[0];
            var keyProperty = keyValueType.GetProperty(nameof(KeyValuePair<string, object?>.Key))!;
            var valueProperty = keyValueType.GetProperty(nameof(KeyValuePair<string, object?>.Value))!;
            var values = new List<(string name, object value)>();
            var hasProviderParameters = false;
            foreach (var item in enumerable)
            {
                if (item is null)
                    continue;

                var key = (string?)keyProperty.GetValue(item);
                var pName = FormatStoredProcedureParameterName(provider, key ?? string.Empty);
                var pValue = valueProperty.GetValue(item) ?? DBNull.Value;
                values.Add((pName, pValue));
                hasProviderParameters |= pValue is DbParameter;
            }

            if (hasProviderParameters)
            {
                cmd.Parameters.Clear();
                foreach (var (name, value) in values)
                {
                    if (value is DbParameter providerParameter)
                    {
                        providerParameter.ParameterName = name;
                        cmd.Parameters.Add(providerParameter);
                        paramDict[name] = providerParameter.Value ?? DBNull.Value;
                    }
                    else
                    {
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = name;
                        nORM.Query.ParameterAssign.AssignValue(parameter, value);
                        cmd.Parameters.Add(parameter);
                        paramDict[name] = value;
                    }
                }
            }
            else
            {
                foreach (var (name, value) in values)
                    paramDict[name] = value;

                cmd.SetParametersFast(values.ToArray());
            }

            return true;
        }

        private static string FormatStoredProcedureParameterName(Providers.DatabaseProvider provider, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new NormUsageException("Stored procedure dictionary parameter names cannot be null, empty, or whitespace.");

            var trimmed = name.Trim();
            if (trimmed.StartsWith(provider.ParamPrefix, StringComparison.Ordinal))
                return trimmed;

            if (trimmed[0] is '@' or ':' or '?')
                trimmed = trimmed.TrimStart('@', ':', '?');

            if (trimmed.Length == 0)
                throw new NormUsageException("Stored procedure dictionary parameter names must contain a name after the provider prefix.");

            return provider.ParamPrefix + trimmed;
        }

        internal static void ValidateStoredProcedureCommandText(
            string procedureName,
            Providers.DatabaseProvider provider,
            IReadOnlyDictionary<string, object>? parameters,
            bool allowTextNonQuery = false)
        {
            if (provider.StoredProcedureCommandType == CommandType.Text)
            {
                if (allowTextNonQuery)
                    NormValidator.ValidateRawNonQuerySql(procedureName, provider, parameters);
                else
                    NormValidator.ValidateRawQuerySql(procedureName, provider, parameters);
                return;
            }

            if (!IsSafeStoredProcedureIdentifier(procedureName))
            {
                throw new NormUsageException(
                    "Stored procedure names must be simple or schema-qualified identifiers. " +
                    "Raw SQL, EXEC text, whitespace-separated command text and stacked statements are not allowed in stored procedure APIs.");
            }

            NormValidator.ValidateRawSql(procedureName, parameters);
        }

        private static bool IsSafeStoredProcedureIdentifier(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;

            var trimmed = name.Trim();
            if (trimmed.Contains(';') || trimmed.Contains("--") || trimmed.Contains("/*") ||
                trimmed.Contains("*/") || trimmed.Contains('\''))
                return false;

            var parts = trimmed.Split('.');
            foreach (var part in parts)
            {
                if (!IsSafeStoredProcedureIdentifierPart(part.Trim()))
                    return false;
            }

            return true;
        }

        private static bool IsSafeStoredProcedureIdentifierPart(string part)
        {
            if (part.Length == 0)
                return false;

            if ((part.StartsWith("[", StringComparison.Ordinal) && part.EndsWith("]", StringComparison.Ordinal)) ||
                (part.StartsWith("\"", StringComparison.Ordinal) && part.EndsWith("\"", StringComparison.Ordinal)) ||
                (part.StartsWith("`", StringComparison.Ordinal) && part.EndsWith("`", StringComparison.Ordinal)))
            {
                if (part.Length < 3)
                    return false;

                var inner = part[1..^1];
                return inner.Length > 0 && inner.All(c => char.IsLetterOrDigit(c) || c == '_' || c == ' ');
            }

            return s_safeOutputParamNameRegex.IsMatch(part);
        }
    }
}
