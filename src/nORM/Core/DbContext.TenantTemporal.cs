using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using nORM.Versioning;
#nullable enable
namespace nORM.Core
{
    public partial class DbContext
    {
        private void ValidateTenantContext<T>(T entity, TableMapping map, WriteOperation operation) where T : class
        {
            if (Options.TenantProvider == null) return;
            var tenantCol = RequireTenantColumn(map, operation.ToString());
            var tenantId = GetRequiredTenantId(map, operation.ToString());
            var entityTenant = tenantCol.Getter(entity);
            // Auto-injecting tenant ID is dangerous - developers might intend null for global records.
            // Requiring explicit tenant ID setting prevents accidental data leakage.
            if (entityTenant == null)
            {
                throw new NormConfigurationException($"Tenant ID is required for {operation} operation but was null. " +
                    "Explicitly set the tenant ID on the entity before saving. Auto-injection has been disabled for security.");
            }
            // X1 fix: use coercion-aware comparison matching the query filter path
            else if (!TenantIdsEqual(entityTenant, tenantId))
            {
                throw new NormConfigurationException("Tenant context mismatch");
            }
        }

        /// <summary>
        /// X1 fix: Compares two tenant ID values with type coercion to match the query filter path.
        /// The query path uses Convert.ChangeType, so ValidateTenantContext must also tolerate
        /// type mismatches (e.g., long provider ID vs int entity column).
        /// </summary>
        private static bool TenantIdsEqual(object a, object b)
        {
            if (Equals(a, b)) return true;
            // Coerce types like the query path does (e.g., long tenant ID vs int column)
            try
            {
                if (a is Guid && b is string guidText && Guid.TryParse(guidText, out var parsedGuid))
                    return Equals(a, parsedGuid);
                if (a.GetType().IsEnum)
                {
                    var enumValue = b is string enumText
                        ? Enum.Parse(a.GetType(), enumText, ignoreCase: true)
                        : Enum.ToObject(a.GetType(), b);
                    return Equals(a, enumValue);
                }
                var coerced = Convert.ChangeType(b, a.GetType());
                return Equals(a, coerced);
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                return false;
            }
        }

        internal Column RequireTenantColumn(TableMapping map, string operation)
        {
            if (Options.TenantProvider == null)
                throw new NormConfigurationException("Tenant provider is not configured.");

            if (map.TenantColumn != null)
                return map.TenantColumn;

            throw new NormConfigurationException(
                $"TenantProvider is configured, but entity '{map.Type.Name}' does not map tenant column " +
                $"'{Options.TenantColumnName}'. nORM fails closed for tenant-scoped {operation} paths; " +
                "add a mapped tenant column or use a non-tenant context for privileged data.");
        }

        internal object GetRequiredTenantId(TableMapping map, string operation)
        {
            var tenantCol = RequireTenantColumn(map, operation);
            return GetRequiredTenantId(tenantCol, map.Type, operation);
        }

        internal object GetRequiredTenantId(string operation)
        {
            object? tenantId;
            try
            {
                tenantId = Options.TenantProvider!.GetCurrentTenantId();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                throw new NormConfigurationException(
                    $"TenantProvider.GetCurrentTenantId() failed while preparing tenant-scoped {operation}.", ex);
            }

            if (tenantId == null)
                throw new NormConfigurationException(
                    $"TenantProvider.GetCurrentTenantId() returned null while preparing tenant-scoped " +
                    $"{operation}. nORM fails closed; return a non-null tenant ID or use a non-tenant " +
                    "context for privileged data.");

            return tenantId;
        }

        internal object GetRequiredTenantId(Column tenantCol, Type entityType, string operation)
        {
            object? tenantId;
            try
            {
                tenantId = Options.TenantProvider!.GetCurrentTenantId();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                throw new NormConfigurationException(
                    $"TenantProvider.GetCurrentTenantId() failed while preparing tenant-scoped {operation} " +
                    $"for entity '{entityType.Name}'.", ex);
            }

            if (tenantId == null)
                throw new NormConfigurationException(
                    $"TenantProvider.GetCurrentTenantId() returned null while preparing tenant-scoped " +
                    $"{operation} for entity '{entityType.Name}'. nORM fails closed; return a non-null tenant ID " +
                    "or use a non-tenant context for privileged data.");

            return CoerceTenantId(tenantId, tenantCol, entityType);
        }

        internal static object CoerceTenantId(object tenantId, Column tenantCol, Type entityType)
        {
            var propType = tenantCol.Prop.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propType) ?? propType;

            if (underlyingType.IsInstanceOfType(tenantId))
                return tenantId;

            try
            {
                if (underlyingType == typeof(Guid))
                {
                    if (tenantId is Guid guid)
                        return guid;
                    if (tenantId is string text && Guid.TryParse(text, out var parsed))
                        return parsed;
                }

                if (underlyingType.IsEnum)
                {
                    if (tenantId is string enumText)
                        return Enum.Parse(underlyingType, enumText, ignoreCase: true);
                    return Enum.ToObject(underlyingType, tenantId);
                }

                return Convert.ChangeType(tenantId, underlyingType, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                throw new NormConfigurationException(
                    $"TenantProvider.GetCurrentTenantId() returned a value of type " +
                    $"'{tenantId.GetType().FullName}' which cannot be converted to tenant column " +
                    $"'{tenantCol.PropName}' on entity '{entityType.Name}' with CLR type '{propType.FullName}'. " +
                    "Ensure TenantProvider returns a compatible type or update the entity mapping.", ex);
            }
        }

        /// <summary>
        /// Describes the tenant predicate shape nORM applies to generated paths for a mapped entity.
        /// </summary>
        /// <typeparam name="T">Mapped entity type.</typeparam>
        /// <param name="operation">Human-readable operation label used in deterministic failure messages.</param>
        /// <returns>Redacted tenant-boundary diagnostics for the mapped entity.</returns>
        public TenantBoundaryDiagnostics GetTenantBoundaryDiagnostics<T>(string operation = "diagnostics") where T : class
        {
            ThrowIfDisposed();
            var map = GetMapping(typeof(T));
            if (Options.TenantProvider == null)
            {
                return new TenantBoundaryDiagnostics(
                    map.Type,
                    map.TableName,
                    isTenantScoped: false,
                    tenantColumnName: null,
                    tenantIdType: null,
                    parameterName: null,
                    predicateSql: null);
            }

            var tenantCol = RequireTenantColumn(map, operation);
            var tenantId = GetRequiredTenantId(tenantCol, map.Type, operation);
            var parameterName = _p.ParamPrefix + "__tenant";
            var predicate = $"{map.EscTable}.{tenantCol.EscCol} = {parameterName}";
            return new TenantBoundaryDiagnostics(
                map.Type,
                map.TableName,
                isTenantScoped: true,
                tenantCol.PropName,
                tenantId.GetType().FullName,
                parameterName,
                predicate);
        }

        /// <summary>
        /// Generates provider-native tenant policy DDL for the mapped entity.
        /// </summary>
        /// <typeparam name="T">Mapped tenant-scoped entity type.</typeparam>
        /// <returns>
        /// Provider-specific DDL for optional database-native RLS defense in depth.
        /// Applications own reviewing and executing the returned script.
        /// </returns>
        /// <exception cref="NormUnsupportedFeatureException">
        /// Thrown when the provider has no database-native tenant policy generator.
        /// </exception>
        public string GenerateNativeTenantPolicySql<T>() where T : class
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(GenerateNativeTenantPolicySql));
            var map = GetMapping(typeof(T));
            if (Options.TenantProvider != null)
                RequireTenantColumn(map, "native tenant policy generation");
            return _p.GenerateNativeTenantPolicySql(map, Options.NativeTenantSessionKey);
        }

        /// <summary>
        /// Applies provider-native tenant policy DDL for the mapped entity through
        /// the current nORM connection and transaction.
        /// </summary>
        /// <typeparam name="T">Mapped tenant-scoped entity type.</typeparam>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="NormUnsupportedFeatureException">
        /// Thrown when the provider has no database-native tenant policy generator.
        /// </exception>
        /// <remarks>
        /// This method is explicit deployment tooling. nORM does not silently
        /// install RLS policies during normal query or save operations.
        /// </remarks>
        public Task ApplyNativeTenantPolicyAsync<T>(CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ApplyNativeTenantPolicyAsync));
            var map = GetMapping(typeof(T));
            if (Options.TenantProvider != null)
                RequireTenantColumn(map, "native tenant policy application");
            var sql = _p.GenerateNativeTenantPolicySql(map, Options.NativeTenantSessionKey);
            return ExecuteDdlBatchesAsync(sql, ct);
        }

        /// <summary>
        /// Drops provider-native tenant policy objects for the mapped entity through
        /// the current nORM connection and transaction.
        /// </summary>
        /// <typeparam name="T">Mapped tenant-scoped entity type.</typeparam>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="NormUnsupportedFeatureException">
        /// Thrown when the provider has no database-native tenant policy generator.
        /// </exception>
        public Task DropNativeTenantPolicyAsync<T>(CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(DropNativeTenantPolicyAsync));
            var map = GetMapping(typeof(T));
            if (Options.TenantProvider != null)
                RequireTenantColumn(map, "native tenant policy removal");
            var sql = _p.GenerateDropNativeTenantPolicySql(map);
            return ExecuteDdlBatchesAsync(sql, ct);
        }

        private async Task ExecuteDdlBatchesAsync(string sql, CancellationToken ct)
        {
            await _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                foreach (var batch in SplitDdlBatches(sql))
                {
                    await using var cmd = ctx.CreateCommand();
                    cmd.CommandText = batch;
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, token).ConfigureAwait(false);
                }
                return 0;
            }, s_nonIdempotentNoRetry, ct).ConfigureAwait(false);
        }

        private static IEnumerable<string> SplitDdlBatches(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                yield break;

            var lines = sql.Replace("\r\n", "\n", StringComparison.Ordinal)
                .Replace('\r', '\n')
                .Split('\n');
            var sb = new StringBuilder();
            foreach (var line in lines)
            {
                if (string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                {
                    var batch = sb.ToString().Trim();
                    if (batch.Length != 0)
                        yield return batch;
                    sb.Clear();
                    continue;
                }
                sb.AppendLine(line);
            }

            var finalBatch = sb.ToString().Trim();
            if (finalBatch.Length != 0)
                yield return finalBatch;
        }

        /// <summary>
        /// Sets the value of a shadow property for the specified entity instance.
        /// </summary>
        /// <param name="entity">The entity that owns the shadow property.</param>
        /// <param name="name">The name of the shadow property to set.</param>
        /// <param name="value">The value to assign.</param>
        public void SetShadowProperty(object entity, string name, object? value)
        {
            ThrowIfDisposed();
            Internal.ShadowPropertyStore.Set(entity, name, value);
        }

        /// <summary>
        /// Retrieves the value of a shadow property from the specified entity.
        /// </summary>
        /// <param name="entity">The entity that owns the shadow property.</param>
        /// <param name="name">The name of the shadow property to retrieve.</param>
        /// <returns>The current value of the shadow property, or <c>null</c> if not set.</returns>
        public object? GetShadowProperty(object entity, string name)
        {
            ThrowIfDisposed();
            return Internal.ShadowPropertyStore.Get(entity, name);
        }

        /// <summary>
        /// Creates a temporal tag entry in the database. Temporal tags can be used to
        /// correlate external events with the state of the database at a given time.
        /// </summary>
        /// <param name="tagName">The name of the tag to create.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CreateTagAsync(string tagName)
        {
            ThrowIfDisposed();
            if (!Options.IsTemporalVersioningEnabled)
                throw new NormConfigurationException(
                    "Temporal tags require DbContextOptions.EnableTemporalVersioning().");
            if (string.IsNullOrWhiteSpace(tagName))
                throw new ArgumentException("Temporal tag name cannot be null, empty, or whitespace.", nameof(tagName));
            if (tagName.Length > 200)
                throw new ArgumentException("Temporal tag name cannot exceed 200 characters.", nameof(tagName));

            await _executionStrategy.ExecuteAsync(async (ctx, ct) =>
            {
                await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var p0 = _p.ParamPrefix + "p0";
                var p1 = _p.ParamPrefix + "p1";
                // Use provider-escaped identifiers - raw table/column names are not safe across all
                // providers (e.g. SQL Server reserves "Timestamp" as a type keyword).
                await using var cmd = ctx.CreateCommand();
                cmd.CommandText = _p.GetCreateTagSql(p0, p1);
                if (_p.UsesDatabaseClockForTemporalTags)
                {
                    var span = new (string name, object value)[1];
                    span[0] = (p0, tagName);
                    cmd.SetParametersFast(span);
                }
                else
                {
                    var span = new (string name, object value)[2];
                    span[0] = (p0, tagName);
                    span[1] = (p1, DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                    cmd.SetParametersFast(span);
                }
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                cmd.Parameters.Clear();
                return 0;
            }, s_nonIdempotentNoRetry, default).ConfigureAwait(false);
        }

        /// <summary>
        /// Generates provider-native temporal bootstrap DDL for the mapped entity.
        /// </summary>
        /// <typeparam name="T">Mapped entity type.</typeparam>
        /// <returns>
        /// Provider-specific DDL for optional native temporal storage. Applications
        /// own reviewing and executing the returned script.
        /// </returns>
        /// <exception cref="NormUnsupportedFeatureException">
        /// Thrown when the provider has no native temporal table support.
        /// </exception>
        public string GenerateProviderNativeTemporalBootstrapSql<T>() where T : class
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(GenerateProviderNativeTemporalBootstrapSql));
            return _p.GenerateProviderNativeTemporalBootstrapSql(GetMapping(typeof(T)));
        }

        /// <summary>
        /// Applies provider-native temporal bootstrap DDL for the mapped entity through
        /// the current nORM connection and transaction.
        /// </summary>
        /// <typeparam name="T">Mapped entity type.</typeparam>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="NormUnsupportedFeatureException">
        /// Thrown when the provider has no native temporal table support.
        /// </exception>
        /// <remarks>
        /// This method is explicit deployment tooling for reviewed provider-native
        /// temporal DDL. nORM-managed temporal bootstrap remains controlled by
        /// <see cref="DbContextOptions.EnableTemporalVersioning()"/>.
        /// </remarks>
        public Task ApplyProviderNativeTemporalBootstrapAsync<T>(CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ApplyProviderNativeTemporalBootstrapAsync));
            var sql = _p.GenerateProviderNativeTemporalBootstrapSql(GetMapping(typeof(T)));
            return ExecuteDdlBatchesAsync(sql, ct);
        }

        /// <summary>
        /// Reads provider-neutral temporal history rows for a single-key entity.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValue">Primary-key value of the entity whose history should be read.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>History rows ordered by validity start time and version id.</returns>
        public Task<IReadOnlyList<TemporalHistoryEntry<T>>> GetTemporalHistoryAsync<T>(
            object? keyValue,
            CancellationToken ct = default) where T : class
            => GetTemporalHistoryAsync<T>(new object?[] { keyValue }, ct);

        /// <summary>
        /// Reads provider-neutral temporal history rows for an entity key.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValues">Primary-key values in mapping order.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>History rows ordered by validity start time and version id.</returns>
        /// <exception cref="NormConfigurationException">Thrown when temporal versioning is not enabled.</exception>
        /// <exception cref="NormUnsupportedFeatureException">Thrown when the key shape does not match the mapped primary key.</exception>
        public async Task<IReadOnlyList<TemporalHistoryEntry<T>>> GetTemporalHistoryAsync<T>(
            IReadOnlyList<object?> keyValues,
            CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(keyValues);
            if (!Options.IsTemporalVersioningEnabled)
                throw new NormConfigurationException(
                    "Temporal history requires DbContextOptions.EnableTemporalVersioning().");
            ThrowIfProviderNativeTemporalOperationalApi("Temporal history");

            var map = GetMapping(typeof(T));
            ValidateTemporalKeyValues(map, keyValues, "Temporal history");

            await EnsureConnectionAsync(ct).ConfigureAwait(false);

            var historyTable = _p.Escape(map.TableName + "_History");
            var validFrom = _p.Escape("__ValidFrom");
            var validTo = _p.Escape("__ValidTo");
            var operation = _p.Escape("__Operation");
            var versionId = _p.Escape("__VersionId");
            var columns = string.Join(", ", map.Columns.Select(c => c.EscCol));
            var whereParts = map.KeyColumns
                .Select((c, i) => $"{c.EscCol} = {_p.ParamPrefix}p{i}")
                .ToList();
            Column? tenantCol = null;
            if (Options.TenantProvider != null)
            {
                tenantCol = RequireTenantColumn(map, "temporal history");
                whereParts.Add($"{tenantCol.EscCol} = {_p.ParamPrefix}tenant");
            }
            var where = string.Join(" AND ", whereParts);

            await using var cmd = CreateCommand();
            cmd.CommandText =
                $"SELECT {columns}, {validFrom}, {validTo}, {operation} " +
                $"FROM {historyTable} WHERE {where} ORDER BY {validFrom}, {versionId}";
            for (var i = 0; i < map.KeyColumns.Length; i++)
            {
                var keyType = map.KeyColumns[i].Prop.PropertyType;
                cmd.AddOptimizedParam(
                    $"{_p.ParamPrefix}p{i}",
                    ConvertTemporalKeyValue(keyValues[i], keyType),
                    keyType);
            }
            if (tenantCol != null)
                cmd.AddOptimizedParam(
                    $"{_p.ParamPrefix}tenant",
                    GetRequiredTenantId(map, "temporal history"),
                    tenantCol.Prop.PropertyType);

            var materializer = new MaterializerFactory().CreateSyncMaterializer(map, typeof(T));
            var result = new List<TemporalHistoryEntry<T>>();
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = (T)materializer(reader);
                var metadataOrdinal = map.Columns.Length;
                result.Add(new TemporalHistoryEntry<T>(
                    entity,
                    ReadTemporalDateTime(reader.GetValue(metadataOrdinal)),
                    ReadTemporalDateTime(reader.GetValue(metadataOrdinal + 1)),
                    Convert.ToString(reader.GetValue(metadataOrdinal + 2), CultureInfo.InvariantCulture) ?? ""));
            }

            return result;
        }

        /// <summary>
        /// Reads changed mapped properties between consecutive temporal history versions for a single-key entity.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValue">Primary-key value of the entity whose history should be compared.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>Diff entries ordered by temporal history order.</returns>
        public Task<IReadOnlyList<TemporalDiffEntry<T>>> GetTemporalDiffAsync<T>(
            object? keyValue,
            CancellationToken ct = default) where T : class
            => GetTemporalDiffAsync<T>(new object?[] { keyValue }, ct);

        /// <summary>
        /// Reads changed mapped properties between consecutive temporal history versions for an entity key.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValues">Primary-key values in mapping order.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>Diff entries ordered by temporal history order.</returns>
        public async Task<IReadOnlyList<TemporalDiffEntry<T>>> GetTemporalDiffAsync<T>(
            IReadOnlyList<object?> keyValues,
            CancellationToken ct = default) where T : class
        {
            var history = await GetTemporalHistoryAsync<T>(keyValues, ct).ConfigureAwait(false);
            if (history.Count < 2)
                return Array.Empty<TemporalDiffEntry<T>>();

            var map = GetMapping(typeof(T));
            var result = new List<TemporalDiffEntry<T>>(history.Count - 1);
            for (var i = 1; i < history.Count; i++)
            {
                var previous = history[i - 1];
                var current = history[i];
                var changes = new List<TemporalPropertyChange>();
                foreach (var column in map.Columns)
                {
                    if (map.KeyColumns.Any(key => ReferenceEquals(key.Prop, column.Prop)))
                        continue;

                    var previousValue = column.Prop.GetValue(previous.Entity);
                    var currentValue = column.Prop.GetValue(current.Entity);
                    if (!Equals(previousValue, currentValue))
                        changes.Add(new TemporalPropertyChange(column.Prop.Name, previousValue, currentValue));
                }

                if (changes.Count > 0)
                    result.Add(new TemporalDiffEntry<T>(previous, current, changes));
            }

            return result;
        }

        /// <summary>
        /// Restores an existing current row to the state visible at the specified temporal tag.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValue">Primary-key value of the row to restore.</param>
        /// <param name="tagName">Temporal tag identifying the historical state to restore.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns><c>1</c> when the current row was restored; <c>0</c> when no current row or no historical snapshot exists.</returns>
        public Task<int> RestoreTemporalVersionAsync<T>(
            object? keyValue,
            string tagName,
            CancellationToken ct = default) where T : class
            => RestoreTemporalVersionAsync<T>(new object?[] { keyValue }, tagName, ct);

        /// <summary>
        /// Restores an existing current row to the state visible at the specified temporal tag.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValues">Primary-key values in mapping order.</param>
        /// <param name="tagName">Temporal tag identifying the historical state to restore.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns><c>1</c> when the current row was restored; <c>0</c> when no current row or no historical snapshot exists.</returns>
        public async Task<int> RestoreTemporalVersionAsync<T>(
            IReadOnlyList<object?> keyValues,
            string tagName,
            CancellationToken ct = default) where T : class
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tagName);
            var timestamp = await GetTemporalTagTimestampAsync(tagName, ct).ConfigureAwait(false);
            return await RestoreTemporalVersionAsync<T>(keyValues, timestamp, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Restores an existing current row to the state visible at the specified UTC timestamp.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValue">Primary-key value of the row to restore.</param>
        /// <param name="timestamp">Point in time identifying the historical state to restore.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns><c>1</c> when the current row was restored; <c>0</c> when no current row or no historical snapshot exists.</returns>
        public Task<int> RestoreTemporalVersionAsync<T>(
            object? keyValue,
            DateTime timestamp,
            CancellationToken ct = default) where T : class
            => RestoreTemporalVersionAsync<T>(new object?[] { keyValue }, timestamp, ct);

        /// <summary>
        /// Restores an existing current row to the state visible at the specified UTC timestamp.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity.</typeparam>
        /// <param name="keyValues">Primary-key values in mapping order.</param>
        /// <param name="timestamp">Point in time identifying the historical state to restore.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns><c>1</c> when the current row was restored; <c>0</c> when no current row or no historical snapshot exists.</returns>
        public async Task<int> RestoreTemporalVersionAsync<T>(
            IReadOnlyList<object?> keyValues,
            DateTime timestamp,
            CancellationToken ct = default) where T : class
        {
            var normalized = NormalizeTemporalCutoff(timestamp);
            return await RestoreTemporalVersionCoreAsync<T>(normalized, keyValues, ct).ConfigureAwait(false);
        }

        private async Task<int> RestoreTemporalVersionCoreAsync<T>(
            DateTime timestamp,
            IReadOnlyList<object?> keyValues,
            CancellationToken ct) where T : class
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(keyValues);
            if (!Options.IsTemporalVersioningEnabled)
                throw new NormConfigurationException(
                    "Temporal restore requires DbContextOptions.EnableTemporalVersioning().");
            ThrowIfProviderNativeTemporalOperationalApi("Temporal restore");

            var map = GetMapping(typeof(T));
            ValidateTemporalKeyValues(map, keyValues, "Temporal restore");
            var predicate = BuildKeyPredicate<T>(map, keyValues);

            var history = await GetTemporalHistoryAsync<T>(keyValues, ct).ConfigureAwait(false);
            var historical = history
                .Where(h => h.ValidFrom <= timestamp && timestamp < h.ValidTo)
                .LastOrDefault();
            if (historical == null)
                return 0;

            var current = await NormQueryable.Query<T>(this)
                .Where(predicate)
                .SingleOrDefaultAsync(ct)
                .ConfigureAwait(false);
            if (current == null)
                return 0;

            CopyTemporalSnapshotValues(map, historical.Entity, current);
            return await UpdateAsync(current, ct).ConfigureAwait(false);
        }

        private static void CopyTemporalSnapshotValues<T>(TableMapping map, T source, T target) where T : class
        {
            foreach (var column in map.Columns)
            {
                if (map.KeyColumns.Any(key => ReferenceEquals(key.Prop, column.Prop)))
                    continue;
                column.Prop.SetValue(target, column.Prop.GetValue(source));
            }
        }

        private async Task<DateTime> GetTemporalTagTimestampAsync(string tagName, CancellationToken ct)
        {
            if (!Options.IsTemporalVersioningEnabled)
                throw new NormConfigurationException(
                    "Temporal tag lookup requires DbContextOptions.EnableTemporalVersioning().");
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand();
            var pName = _p.ParamPrefix + "p0";
            cmd.CommandText = _p.GetTagLookupSql(pName);
            cmd.AddOptimizedParam(pName, tagName, typeof(string));
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result == null || result == DBNull.Value)
                throw new NormQueryException($"Tag '{tagName}' not found.");
            return ReadTemporalDateTime(result);
        }

        /// <summary>
        /// Deletes closed temporal history rows older than the supplied cutoff for one mapped entity type.
        /// </summary>
        /// <typeparam name="T">Type of the mapped entity whose history is pruned.</typeparam>
        /// <param name="olderThan">UTC cutoff; rows whose validity ended before this value are removed.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>The number of rows deleted by the provider, when reported.</returns>
        /// <exception cref="NormConfigurationException">Thrown when temporal versioning is not enabled.</exception>
        public async Task<int> PruneTemporalHistoryAsync<T>(
            DateTime olderThan,
            CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            if (!Options.IsTemporalVersioningEnabled)
                throw new NormConfigurationException(
                    "Temporal pruning requires DbContextOptions.EnableTemporalVersioning().");
            ThrowIfProviderNativeTemporalOperationalApi("Temporal pruning");

            var cutoff = NormalizeTemporalCutoff(olderThan);
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            return await PruneTemporalHistoryForMappingAsync(GetMapping(typeof(T)), cutoff, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Deletes closed temporal history rows older than the supplied cutoff.
        /// </summary>
        /// <param name="olderThan">UTC cutoff; rows whose validity ended before this value are removed.</param>
        /// <param name="pruneTags">When true, global temporal tags older than the cutoff are also removed.</param>
        /// <param name="ct">Token used to cancel the database operation.</param>
        /// <returns>The number of rows deleted by the provider, when reported.</returns>
        /// <exception cref="NormConfigurationException">Thrown when temporal versioning is not enabled.</exception>
        /// <exception cref="NormUnsupportedFeatureException">Thrown when tag pruning is requested in tenant mode.</exception>
        public async Task<int> PruneTemporalHistoryAsync(
            DateTime olderThan,
            bool pruneTags = false,
            CancellationToken ct = default)
        {
            ThrowIfDisposed();
            if (!Options.IsTemporalVersioningEnabled)
                throw new NormConfigurationException(
                    "Temporal pruning requires DbContextOptions.EnableTemporalVersioning().");
            ThrowIfProviderNativeTemporalOperationalApi("Temporal pruning");
            if (pruneTags && Options.TenantProvider != null)
                throw new NormUnsupportedFeatureException(
                    "Temporal tags are global. Disable tenant mode or prune tags through an audited administrative context.");

            var cutoff = NormalizeTemporalCutoff(olderThan);

            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            var affected = 0;
            foreach (var map in GetAllMappings())
            {
                var deleted = await PruneTemporalHistoryForMappingAsync(map, cutoff, ct).ConfigureAwait(false);
                if (deleted > 0)
                    affected += deleted;
            }

            if (pruneTags)
            {
                await using var cmd = CreateCommand();
                cmd.CommandText =
                    $"DELETE FROM {_p.Escape("__NormTemporalTags")} " +
                    $"WHERE {_p.Escape("Timestamp")} < {_p.ParamPrefix}cutoff";
                cmd.AddOptimizedParam($"{_p.ParamPrefix}cutoff", cutoff, typeof(DateTime));
                var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                if (deleted > 0)
                    affected += deleted;
            }

            return affected;
        }

        private static DateTime NormalizeTemporalCutoff(DateTime value) =>
            DateTime.SpecifyKind(
                value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value,
                DateTimeKind.Unspecified);

        private void ThrowIfProviderNativeTemporalOperationalApi(string operation)
        {
            if (Options.TemporalStorageMode != TemporalStorageMode.ProviderNative)
                return;

            throw new NormUnsupportedFeatureException(
                $"{operation} uses nORM-managed history metadata. Provider-native temporal mode currently supports " +
                "AsOf(DateTime), AsOf(tag), tag creation, and SQL Server native bootstrap; use nORM-managed temporal " +
                "storage for provider-neutral history, diff, restore, and pruning APIs.");
        }

        private async Task<int> PruneTemporalHistoryForMappingAsync(
            TableMapping map,
            DateTime cutoff,
            CancellationToken ct)
        {
            var historyTable = _p.Escape(map.TableName + "_History");
            var validTo = _p.Escape("__ValidTo");
            var where = $"{validTo} < {_p.ParamPrefix}cutoff";
            Column? tenantCol = null;
            if (Options.TenantProvider != null)
            {
                tenantCol = RequireTenantColumn(map, "temporal pruning");
                where += $" AND {tenantCol.EscCol} = {_p.ParamPrefix}tenant";
            }

            await using var cmd = CreateCommand();
            cmd.CommandText = $"DELETE FROM {historyTable} WHERE {where}";
            cmd.AddOptimizedParam($"{_p.ParamPrefix}cutoff", cutoff, typeof(DateTime));
            if (tenantCol != null)
                cmd.AddOptimizedParam(
                    $"{_p.ParamPrefix}tenant",
                    GetRequiredTenantId(map, "temporal pruning"),
                    tenantCol.Prop.PropertyType);
            return await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
        }

        private void ValidateTemporalKeyValues(TableMapping map, IReadOnlyList<object?> keyValues, string operation)
        {
            if (map.KeyColumns.Length == 0)
                throw new NormUnsupportedFeatureException(
                    $"{operation} for '{map.Type.Name}' requires a mapped primary key.");
            if (keyValues.Count != map.KeyColumns.Length)
                throw new NormUnsupportedFeatureException(
                    $"{operation} for '{map.Type.Name}' requires {map.KeyColumns.Length} key value(s), " +
                    $"but {keyValues.Count} were provided.");
            for (var i = 0; i < map.KeyColumns.Length; i++)
            {
                var keyCol = map.KeyColumns[i];
                var keyType = keyCol.Prop.PropertyType;
                if (keyValues[i] == null && Nullable.GetUnderlyingType(keyType) == null && keyType.IsValueType)
                    throw new NormUnsupportedFeatureException(
                        $"{operation} for '{map.Type.Name}' requires non-null key value '{keyCol.Prop.Name}'.");
            }
        }

        private static Expression<Func<T, bool>> BuildKeyPredicate<T>(TableMapping map, IReadOnlyList<object?> keyValues)
        {
            var parameter = Expression.Parameter(typeof(T), "e");
            Expression? body = null;
            for (var i = 0; i < map.KeyColumns.Length; i++)
            {
                var keyCol = map.KeyColumns[i];
                var property = Expression.Property(parameter, keyCol.Prop);
                var keyType = keyCol.Prop.PropertyType;
                var value = keyValues[i];
                var constant = Expression.Constant(value == null ? null : ConvertTemporalKeyValue(value, keyType), keyType);
                var comparison = Expression.Equal(property, constant);
                body = body == null ? comparison : Expression.AndAlso(body, comparison);
            }

            return Expression.Lambda<Func<T, bool>>(body!, parameter);
        }

        private static object? ConvertTemporalKeyValue(object? value, Type targetType)
        {
            if (value == null)
                return null;

            var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (underlying.IsInstanceOfType(value))
                return value;
            if (underlying == typeof(Guid))
                return value is Guid guid ? guid : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!);
            if (underlying.IsEnum)
                return Enum.ToObject(underlying, Convert.ChangeType(value, Enum.GetUnderlyingType(underlying), CultureInfo.InvariantCulture));
            return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
        }

        private static DateTime ReadTemporalDateTime(object value)
        {
            if (value is DateTime dt)
                return DateTime.SpecifyKind(dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : dt, DateTimeKind.Unspecified);
            if (value is DateTimeOffset dto)
                return DateTime.SpecifyKind(dto.UtcDateTime, DateTimeKind.Unspecified);
            return DateTime.SpecifyKind(
                DateTime.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
                DateTimeKind.Unspecified);
        }
    }
}
