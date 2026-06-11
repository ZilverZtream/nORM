using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Reflection.Emit;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Generates entity types at runtime based on database schema information.
    /// Reuses a single shared <see cref="ModuleBuilder"/> to prevent unloadable assembly
    /// accumulation when types are evicted from cache.
    /// </summary>
    [RequiresDynamicCode("Dynamic scaffolding emits CLR types at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
    [RequiresUnreferencedCode("Dynamic scaffolding reflects database schema into runtime-generated entity types and is not trim-safe.")]
    public partial class DynamicEntityTypeGenerator
    {
        internal sealed record ColumnInfo(string ColumnName, string PropertyName, Type PropertyType, bool AllowsNull, bool IsKey, int KeyOrdinal, int SourceOrdinal, bool IsAuto, bool IsComputed, ScaffoldComputedColumn? ComputedColumn, bool IsRowVersion, int? MaxLength, bool? IsUnicode, bool IsFixedLength, ScaffoldDecimalPrecision? DecimalPrecision);

        internal readonly record struct ScaffoldComputedColumn(string Sql, bool Stored);
        internal readonly record struct ScaffoldDecimalPrecision(int Precision, int? Scale);
        internal readonly record struct ScaffoldColumnFacet(int? MaxLength, bool? IsUnicode, bool IsFixedLength);

        /// <summary>
        /// Number of leading bytes from the SHA-256 hash used as the schema signature.
        /// 16 bytes yields a 32-character hex string with negligible collision probability.
        /// </summary>
        private const int SchemaSignatureTruncationBytes = 16;

        /// <summary>
        /// Generates a CLR type representing the specified table asynchronously.
        /// Uses the shared <see cref="ModuleBuilder"/> to avoid per-type assembly allocation.
        /// </summary>
        /// <param name="connection">Database connection. Will be opened if not already open.</param>
        /// <param name="tableName">Name of the table to generate. May include schema (schema.table).</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
        [RequiresDynamicCode("Dynamic scaffolding emits CLR types at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
        [RequiresUnreferencedCode("Dynamic scaffolding reflects database schema into runtime-generated entity types and is not trim-safe.")]
        public async Task<Type> GenerateEntityTypeAsync(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                var (schemaName, bareTable, columns, isReadOnlyEntity) = ResolveTableSchema(connection, tableName);
                // Materialize columns eagerly so the reader is closed before type building begins.
                return BuildDynamicType(schemaName, bareTable, columns, isReadOnlyEntity);
            }
            finally
            {
                if (!connectionWasOpen)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generates a CLR type representing the specified table synchronously.
        /// Uses the shared <see cref="ModuleBuilder"/> to avoid per-type assembly allocation.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="tableName">Name of the table to generate. May include schema (schema.table).</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
        [RequiresDynamicCode("Dynamic scaffolding emits CLR types at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
        [RequiresUnreferencedCode("Dynamic scaffolding reflects database schema into runtime-generated entity types and is not trim-safe.")]
        public Type GenerateEntityType(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                connection.Open();

            try
            {
                var (schemaName, bareTable, columns, isReadOnlyEntity) = ResolveTableSchema(connection, tableName);
                // Materialize columns eagerly so the reader is closed before type building begins.
                return BuildDynamicType(schemaName, bareTable, columns, isReadOnlyEntity);
            }
            finally
            {
                if (!connectionWasOpen)
                    connection.Close();
            }
        }

        /// <summary>
        /// Builds a dynamic CLR type from the given column descriptors using the shared <see cref="ModuleBuilder"/>.
        /// Each invocation generates a uniquely-named type to prevent conflicts when the same table
        /// is regenerated after a schema change.
        /// </summary>
        private static Type BuildDynamicType(string? schemaName, string tableName, IReadOnlyList<ColumnInfo> columns, bool isReadOnlyEntity)
            => DynamicEntityTypeBuilder.BuildType(schemaName, tableName, columns, isReadOnlyEntity);

        /// <summary>
        /// Computes a stable hash string that represents the schema of the specified table.
        /// The signature is derived from ordered column names, their CLR types, primary-key status,
        /// nullability, identity, and length metadata. Including this in the dynamic-type cache key ensures
        /// that schema changes (added columns, changed types, generated attributes) produce a new cache entry
        /// rather than returning a stale type.
        /// Uses SHA-256 truncated to <see cref="SchemaSignatureTruncationBytes"/> bytes (32-char hex)
        /// for negligible collision probability.
        /// </summary>
        /// <param name="connection">Open database connection used to probe the schema.</param>
        /// <param name="tableName">Possibly schema-qualified table name.</param>
        /// <returns>A hex string fingerprint of the column descriptors in ordinal order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
        public string ComputeSchemaSignature(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                connection.Open();
            try
            {
                var (schemaName, bareTable, columns, isReadOnlyEntity) = ResolveTableSchema(connection, tableName);
                // Include every metadata field that changes the emitted CLR surface. Omitting one can return
                // a stale dynamic type after a schema change even though the generated C# would differ.
                var descriptor = BuildSchemaDescriptor(schemaName, bareTable, columns, isReadOnlyEntity);
                var hash = SHA256.HashData(Encoding.UTF8.GetBytes(descriptor));
                return Convert.ToHexString(hash[..SchemaSignatureTruncationBytes]);
            }
            finally
            {
                if (!connectionWasOpen)
                    connection.Close();
            }
        }

        private static string BuildSchemaDescriptor(string? schemaName, string tableName, IReadOnlyList<ColumnInfo> columns, bool isReadOnlyEntity)
            => DynamicEntitySchemaDescriptorBuilder.BuildDescriptor(schemaName, tableName, columns, isReadOnlyEntity);
    }
}
