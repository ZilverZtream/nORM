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
        /// <summary>
        /// Retrieves or creates a <see cref="TableMapping"/> for the specified entity
        /// type. Mappings are cached for reuse to avoid repeated reflection.
        /// </summary>
        /// <param name="t">The entity CLR type to map.</param>
        /// <returns>The mapping associated with the given type.</returns>
        internal TableMapping GetMapping(Type t) => _m.GetOrAdd(t, static (k, args) =>
            new TableMapping(k, args.p, args.ctx, args.modelBuilder.GetConfiguration(k)), (p: _p, ctx: this, modelBuilder: _modelBuilder));

        /// <summary>
        /// Enumerates mappings for all entity types that were configured via the
        /// <see cref="ModelBuilder"/>. Useful for scenarios that need to inspect
        /// or pre-generate metadata for every known entity.
        /// </summary>
        /// <returns>An enumerable sequence of <see cref="TableMapping"/> objects.</returns>
        internal IEnumerable<TableMapping> GetAllMappings()
        {
            var seen = new HashSet<Type>();
            foreach (var type in _modelBuilder.GetConfiguredEntityTypes())
            {
                seen.Add(type);
                yield return GetMapping(type);
            }

            foreach (var entry in _m)
            {
                if (seen.Add(entry.Key))
                    yield return entry.Value;
            }
        }

        /// <summary>
        /// Returns a stable hash of all entity type ? table name mappings.
        /// Computed once and cached for the lifetime of the context.
        /// </summary>
        internal int GetMappingHash()
        {
            if (_mappingHashComputed) return _mappingHashCached;
            int h = 0;
            foreach (var mapping in GetAllMappings())
            {
                // Q1/C1/Cache1 fix: include full mapping shape - not just Type+TableName.
                // Column names, converter fingerprint, shadow fingerprint, tenant column,
                // and timestamp column all affect SQL generation and materialization.
                // Without these, divergent fluent configurations sharing the same CLR type
                // and table name can poison the global static plan/fast-path caches.
                h = HashCode.Combine(h, mapping.TableName, mapping.Type);
                h = HashCode.Combine(h, mapping.ConverterFingerprint, mapping.ShadowFingerprint);
                h = HashCode.Combine(h, mapping.TenantColumn?.Name, mapping.TimestampColumn?.Name);
                foreach (var col in mapping.Columns)
                    h = HashCode.Combine(h, col.Name, col.PropName);
            }
            _mappingHashCached = h;
            _mappingHashComputed = true;
            return h;
        }

        /// <summary>
        /// Tracks types that were used as query roots via ctx.Query&lt;T&gt;().
        /// These are "real" entity types, as opposed to DTO projection result types.
        /// Thread-safe via ConcurrentDictionary (used as a set).
        /// </summary>
        private readonly ConcurrentDictionary<Type, byte> _entityQueryRoots = new();

        /// <summary>
        /// Registers a type as a query-root entity (called by Query&lt;T&gt; extension).
        /// Ensures that directly-queried entities are considered "mapped" by IsMapped.
        /// </summary>
        internal void RegisterEntityType(Type t) => _entityQueryRoots.TryAdd(t, 0);

        /// <summary>
        /// Returns true when the type is a known entity root - either explicitly
        /// registered with the ModelBuilder OR used as a direct query root via Query&lt;T&gt;.
        /// DTO projection results (from .Select(x => new Dto {...})) are never query roots
        /// and will NOT be tracked, preventing ChangeTracker pollution.
        /// </summary>
        internal bool IsMapped(Type t) =>
            _modelBuilder.GetConfiguredEntityTypes().Contains(t) ||
            _entityQueryRoots.ContainsKey(t);

        /// <summary>
        /// Validates that an identifier is safe for use in SQL queries by stripping optional
        /// delimiters and checking the inner content against a strict allowlist. Prevents SQL
        /// injection via crafted identifiers like "[foo]; DROP TABLE Users--". Allows:
        /// - Alphanumeric characters, underscores, dots, spaces (word chars only)
        /// - Quoted identifiers: "name", `name`, [name] with safe inner content
        /// - Schema-qualified identifiers: dbo.Table, [dbo].[Table], "schema"."table"
        /// </summary>
        internal static bool IsSafeIdentifier(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;

            var trimmed = name.Trim();

            // Fast reject: statement-break characters are never valid in identifiers
            if (trimmed.Contains(';') || trimmed.Contains("--") || trimmed.Contains("/*") ||
                trimmed.Contains("*/") || trimmed.Contains('\''))
                return false;

            // Split on '.' to handle schema-qualified names like dbo.Table or [dbo].[Table]
            // Each part is validated independently.
            var parts = trimmed.Split('.');
            foreach (var part in parts)
            {
                if (!IsSafeIdentifierPart(part.Trim()))
                    return false;
            }
            return true;
        }

        private static readonly HashSet<string> _sensitiveConnStringKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            "password", "pwd", "user password", "access token", "accesstoken", "token", "secret"
        };

        /// <summary>
        /// Normalizes a connection string for use as a static cache key.
        /// Uses DbConnectionStringBuilder for robust parsing (handles quoted semicolons, escaped
        /// chars, reordered keys). Strips sensitive keys so credentials never appear in cache key
        /// memory (coredump / diagnostic exposure risk). Keys are sorted case-insensitively so
        /// identical strings with different orderings map to the same cache key.
        /// </summary>
        private static string NormalizeConnectionString(string? cs)
        {
            if (string.IsNullOrEmpty(cs)) return string.Empty;
            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = cs };
                return string.Join(";",
                    builder.Keys.Cast<string>()
                        .Where(k => !_sensitiveConnStringKeys.Contains(k))
                        .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                        .Select(k => $"{k}={builder[k]}"));
            }
            catch (ArgumentException)
            {
                // Malformed connection string: fall back to opaque hash so we never
                // return credentials in the key.
                return Convert.ToHexString(
                    System.Security.Cryptography.SHA256.HashData(
                        System.Text.Encoding.UTF8.GetBytes(cs)));
            }
        }

        /// <summary>
        /// Validates a single identifier part (possibly delimited). A part is either:
        /// - A plain word (letters, digits, underscore, space)
        /// - A delimited identifier: [inner], "inner", or `inner` where inner contains only word chars
        /// </summary>
        private static bool IsSafeIdentifierPart(string part)
        {
            if (part.Length == 0)
                return false;

            string inner;

            // Strip single pair of delimiters
            if ((part.StartsWith("[") && part.EndsWith("]")) ||
                (part.StartsWith("\"") && part.EndsWith("\"")) ||
                (part.StartsWith("`") && part.EndsWith("`")))
            {
                if (part.Length < 3)
                    return false; // empty delimiter pair
                inner = part[1..^1];
            }
            else
            {
                inner = part;
            }

            if (inner.Length == 0)
                return false;

            // Inner content must be strictly word characters and spaces only
            // (no brackets, quotes, semicolons, hyphens, or other special chars).
            // Uses pre-compiled static regex to avoid per-call regex compilation overhead.
            return s_safeIdentifierRegex.IsMatch(inner);
        }

        /// <summary>
        /// Creates an untyped <see cref="IQueryable"/> for the specified table name.
        /// This API is useful when working with tables or views that do not have a
        /// corresponding CLR type at compile time. A dynamic entity type is generated
        /// on-demand and cached so subsequent calls incur minimal overhead.
        /// </summary>
        /// <param name="tableName">Name of the table to query.</param>
        /// <returns>An <see cref="IQueryable"/> that can be composed with LINQ operators.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or empty.</exception>
        /// <exception cref="NormUsageException">Thrown when the provided name contains invalid characters.</exception>
        [RequiresDynamicCode("Query(string) generates a CLR entity type at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
        [RequiresUnreferencedCode("Query(string) reflects database schema into a runtime-generated type and is not trim-safe.")]
        public IQueryable Query(string tableName)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(Query));
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            if (!IsSafeIdentifier(tableName))
                throw new NormUsageException("Invalid table name.");
            // Gate C fix: Use normalized full connection string instead of 32-bit hash to
            // eliminate key collision risk. GetHashCode() is a 32-bit projection - two
            // distinct connection strings can share the same hash, causing schema aliasing
            // where queries against different databases return stale type definitions.
            // Normalizing (sort key=value pairs, lowercase) also ensures that identical
            // connection strings written in different key orders map to the same cache entry.
            //
            // Schema-signature fix: Include a hash of the live column name+type pairs in the
            // cache key. After a schema migration (new column added, column type changed), the
            // same provider|connstring|table triple would return the stale generated CLR type.
            // Including the schema signature means any schema change produces a new cache entry.
            // Old entries become unreachable and are eligible for LRU eviction.
            EnsureConnection();
            var schemaSignature = _typeGenerator.ComputeSchemaSignature(_cn, tableName);
            var cacheKey = $"{Provider.GetType().FullName}|{NormalizeConnectionString(_cn.ConnectionString)}|{tableName}|{schemaSignature}";
            var lazyTask = _dynamicTypeCache.GetOrAdd(cacheKey,
                _ => new Lazy<Task<Type>>(() => Task.FromResult(_typeGenerator.GenerateEntityType(RawConnection, tableName))));

            // Query(string) is synchronous. Generate the cached type on the current thread
            // instead of bouncing through Task.Run, which can starve the thread pool when many
            // callers hit the same dynamic root concurrently.
            var entityType = lazyTask.Value.GetAwaiter().GetResult();

            var generic = s_queryMethodInfo.Value.MakeGenericMethod(entityType);
            return (IQueryable)generic.Invoke(null, new object[] { this })!;
        }
    }
}
