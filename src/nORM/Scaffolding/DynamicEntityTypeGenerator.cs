using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
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
    public class DynamicEntityTypeGenerator
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

        private const int MaxScaffoldedMySqlSetValueCount = 8;

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

        private static IReadOnlyList<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
        {
            var qualified = EscapeQualified(connection, schemaName, tableName);
            var postgresDomainColumnCastTypes = GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);
            DataTable? schema;
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = BuildSchemaProbeSql(connection, schemaName, tableName, qualified, postgresDomainColumnCastTypes);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
                schema = reader.GetSchemaTable();
            }

            if (schema is null)
                return Array.Empty<ColumnInfo>();
            var existingPropertyNames = CreateReservedMemberNameSet();
            existingPropertyNames.Add(EscapeCSharpIdentifier(ToPascalCase(tableName)));
            var computedColumns = GetComputedColumns(connection, schemaName, tableName);
            var identityColumns = GetIdentityColumns(connection, schemaName, tableName);
            var rowVersionColumns = GetRowVersionColumns(connection, schemaName, tableName);
            var sqliteDeclaredTypes = GetSqliteDeclaredColumnTypes(connection, schemaName, tableName);
            var sqlServerAliasBaseTypes = GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);
            var mySqlUnsignedColumnTypes = GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);
            var decimalPrecisions = GetDecimalPrecisions(connection, schemaName, tableName);
            var columnFacets = GetStringBinaryFacets(connection, schemaName, tableName);
            var primaryKeyOrdinals = GetPrimaryKeyOrdinals(connection, schemaName, tableName);
            var columns = new List<ColumnInfo>(schema.Rows.Count);
            var sourceOrdinal = 0;
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]?.ToString();
                if (string.IsNullOrEmpty(colName))
                    continue;
                var currentSourceOrdinal = sourceOrdinal++;
                var propName = MakeUnique(EscapeCSharpIdentifier(ToPascalCase(colName)), existingPropertyNames);
                if (row["DataType"] is not Type clrType)
                    continue;
                var allowNull = row["AllowDBNull"] is bool b && b;
                var isKey = schema.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var keyOrdinal = primaryKeyOrdinals.TryGetValue(colName, out var ordinal)
                    ? ordinal
                    : isKey ? currentSourceOrdinal + 1 : 0;
                var isAuto = (schema.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai)
                    || identityColumns.Contains(colName);
                var isComputed = (schema.Columns.Contains("IsExpression") && row["IsExpression"] is bool expression && expression)
                    || computedColumns.ContainsKey(colName);
                var computedColumn = computedColumns.TryGetValue(colName, out var computed)
                    ? computed
                    : (ScaffoldComputedColumn?)null;
                var isRowVersion = rowVersionColumns.Contains(colName);
                var effectiveAllowNull = allowNull && !isKey && !isRowVersion;
                sqliteDeclaredTypes.TryGetValue(colName, out var declaredType);
                sqlServerAliasBaseTypes.TryGetValue(colName, out var sqlServerAliasBaseType);
                var normalizedClrType = NormalizeScaffoldClrType(connection, clrType, effectiveAllowNull, isKey, isAuto, declaredType);
                if (IsPostgresConnection(connection.GetType().Name)
                    && normalizedClrType == typeof(Array)
                    && postgresDomainColumnCastTypes.TryGetValue(colName, out var domainCastType)
                    && TryMapPostgresArrayCastType(domainCastType, out var arrayClrType))
                {
                    normalizedClrType = arrayClrType;
                }
                else if (IsSqlServerConnection(connection.GetType().Name)
                         && TryMapSqlServerAliasBaseClrType(sqlServerAliasBaseType, out var aliasClrType))
                {
                    normalizedClrType = aliasClrType;
                }
                else if (IsMySqlConnection(connection.GetType().Name)
                         && mySqlUnsignedColumnTypes.TryGetValue(colName, out var unsignedColumnType)
                         && TryMapMySqlUnsignedType(unsignedColumnType, out var unsignedClrType))
                {
                    normalizedClrType = unsignedClrType;
                }

                var propertyType = GetPropertyType(normalizedClrType, effectiveAllowNull);

                columnFacets.TryGetValue(colName, out var columnFacet);
                var maxLength = GetScaffoldMaxLength(normalizedClrType, row)
                    ?? GetSqlServerAliasBaseMaxLengthFromTypeText(sqlServerAliasBaseType);
                if (!maxLength.HasValue && columnFacet.MaxLength.HasValue)
                    maxLength = columnFacet.MaxLength;
                var decimalPrecision = normalizedClrType == typeof(decimal)
                    && decimalPrecisions.TryGetValue(colName, out var precision)
                    ? precision
                    : (ScaffoldDecimalPrecision?)null;

                columns.Add(new ColumnInfo(colName, propName, propertyType, effectiveAllowNull, isKey, keyOrdinal, currentSourceOrdinal, isAuto, isComputed, computedColumn, isRowVersion, maxLength, columnFacet.IsUnicode, columnFacet.IsFixedLength, decimalPrecision));
            }

            return columns;
        }

        private static string BuildSchemaProbeSql(
            DbConnection connection,
            string? schemaName,
            string tableName,
            string qualified,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
        {
            if (!IsPostgresConnection(connection.GetType().Name) || postgresDomainColumnCastTypes.Count == 0)
                return $"SELECT * FROM {qualified} WHERE 1=0";

            var columnNames = GetPostgresColumnNames(connection, schemaName, tableName);
            if (columnNames.Count == 0)
                return $"SELECT * FROM {qualified} WHERE 1=0";

            var projection = columnNames.Select(column =>
            {
                var escaped = EscapeIdentifier(connection, column);
                return postgresDomainColumnCastTypes.TryGetValue(column, out var castType)
                    ? $"{escaped}::{castType} AS {escaped}"
                    : escaped;
            });

            return $"SELECT {string.Join(", ", projection)} FROM {qualified} WHERE 1=0";
        }

        private static int? GetScaffoldMaxLength(Type clrType, DataRow row)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            if (!row.Table.Columns.Contains("ColumnSize") || row["ColumnSize"] == DBNull.Value)
                return null;

            return int.TryParse(row["ColumnSize"]?.ToString(), out var size) && size > 0 && !IsUnboundedScaffoldMaxLength(size)
                ? size
                : null;
        }

        private static bool IsUnboundedScaffoldMaxLength(int size)
            => size == int.MaxValue
               || size == 1073741823;

        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetStringBinaryFacets(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetStringBinaryFacets(connection, schemaName, tableName)
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => new ScaffoldColumnFacet(pair.Value.MaxLength, pair.Value.IsUnicode, pair.Value.IsFixedLength),
                    StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecision> GetDecimalPrecisions(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetDecimalPrecisions(connection, schemaName, tableName)
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => new ScaffoldDecimalPrecision(pair.Value.Precision, pair.Value.Scale),
                    StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> GetComputedColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.GetComputedColumns(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetSqliteDeclaredColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetSqliteDeclaredColumnTypes(connection, schemaName, tableName);

        private static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetIdentityColumns(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, int> GetPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPrimaryKeyOrdinals(connection, schemaName, tableName);

        private static IReadOnlySet<string> GetRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetRowVersionColumns(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        private static bool TryMapSqlServerAliasBaseClrType(string? typeText, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(typeText, out type);

        private static string NormalizeSqlServerAliasBaseTypeName(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeSqlServerAliasBaseTypeName(typeText);

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string? typeText)
            => string.IsNullOrWhiteSpace(typeText)
                ? null
                : ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);

        private static bool TryMapMySqlUnsignedType(string? detail, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(detail, out type);

        private static string NormalizeMySqlUnsignedTypeDetail(string detail)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeMySqlUnsignedTypeDetail(detail);

        private static string NormalizePostgresDomainProbeCastType(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizePostgresDomainProbeCastType(typeText);

        private static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryNormalizePostgresParameterizedProbeCastType(normalized, out castType);

        private static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresTypeArguments(normalized, typeName, out args);

        private static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);

        private static bool TryMapPostgresArrayCastType(string castType, out Type arrayType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(castType.Trim().ToLowerInvariant(), out arrayType);

        private static IReadOnlyList<string> GetPostgresColumnNames(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> QueryComputedColumnMap(DbConnection connection, string sql, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.QueryComputedColumnMap(connection, sql, schemaName, tableName);

        private static string? GetSqliteCreateTableSql(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.GetSqliteCreateTableSql(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> ExtractSqliteGeneratedColumns(string? createTableSql)
            => DynamicEntityComputedColumnReader.ExtractSqliteGeneratedColumns(createTableSql);

        private static (string Sql, bool Stored) NormalizeComputedColumnSql(string raw)
            => DynamicEntityComputedColumnReader.NormalizeComputedColumnSql(raw);

        private static bool TryTrimTrailingComputedStorageToken(string sql, string token, out string trimmedSql)
            => ScaffoldSqlMetadataParser.TryTrimTrailingComputedStorageToken(sql, token, out trimmedSql);

        private static bool HasBalancedOuterParentheses(string value)
            => ScaffoldSqlMetadataParser.HasBalancedOuterParentheses(value);

        private static int FindMatchingParenthesis(string sql, int openIndex)
            => ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);

        private static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
            => ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, keyword, startIndex);

        private static IReadOnlyList<string> SplitTopLevelCommaSeparated(string text)
            => ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(text);

        private static (string? schema, string table) SplitSchema(string identifier)
            => DynamicEntitySchemaResolver.SplitSchema(identifier);

        private static (string? schema, string table, List<ColumnInfo> columns, bool isReadOnlyEntity) ResolveTableSchema(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.ResolveTableSchema(
                connection,
                tableName,
                static (cn, schema, table) => GetTableSchema(cn, schema, table).ToList(),
                static () => new List<ColumnInfo>(),
                IsReadOnlyDynamicObject);

        private static bool IsReadOnlyDynamicObject(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsReadOnlyDynamicObject(connection, schemaName, tableName);

        private static bool IsDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsDynamicQueryObject(connection, schemaName, tableName);

        private static bool IsProviderOwnedSynonym(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsProviderOwnedSynonym(connection, schemaName, tableName);

        private static bool IsProviderNativeTemporalTable(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsProviderNativeTemporalTable(connection, schemaName, tableName);

        private static bool HasProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasProviderOwnedTriggers(connection, schemaName, tableName);

        private static bool HasUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasUnmodeledDefaults(connection, schemaName, tableName);

        private static bool HasUnmodeledDefaultSql(string? raw)
            => !string.IsNullOrWhiteSpace(raw)
               && !TryNormalizeDynamicDefaultSql(raw, out _);

        private static bool TryNormalizeDynamicDefaultSql(string? raw, out string defaultValueSql)
            => DynamicEntityReadOnlyClassifier.TryNormalizeDynamicDefaultSql(raw, out defaultValueSql);

        private static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasWriteBlockingProviderSpecificColumns(connection, schemaName, tableName);

        private static bool HasWriteBlockingMySqlSetColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasWriteBlockingMySqlSetColumns(connection, schemaName, tableName);

        private static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(detail, out values);

        private static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseMySqlQuotedTypeValues(detail, typeName, out values);

        private static bool IsWriteBlockingSqliteDeclaredType(string? declaredType)
            => DynamicEntityReadOnlyClassifier.IsWriteBlockingSqliteDeclaredType(declaredType);

        private static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalizedDeclaredType);

        private static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => DynamicEntityReadOnlyClassifier.ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, token);

        private static string? ResolveUniqueUnqualifiedSchema(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.ResolveUniqueUnqualifiedSchema(connection, tableName);

        private static IReadOnlyList<string> GetMatchingObjectSchemas(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.GetMatchingObjectSchemas(connection, tableName);

        private static IReadOnlyList<string> GetSqliteMatchingObjectSchemas(DbConnection connection, string tableName)
            => DynamicEntitySchemaResolver.GetSqliteMatchingObjectSchemas(connection, tableName);

        private static IReadOnlyList<string> QuerySchemaNameList(DbConnection connection, string sql, string tableName)
            => DynamicEntitySchemaResolver.QuerySchemaNameList(connection, sql, tableName);

        private static bool TryGetTableSchema(DbConnection connection, string? schemaName, string tableName, out List<ColumnInfo> columns)
        {
            var found = DynamicEntitySchemaResolver.TryGetTableSchema(
                connection,
                schemaName,
                tableName,
                static (cn, schema, table) => GetTableSchema(cn, schema, table).ToList(),
                static () => new List<ColumnInfo>(),
                out var resolvedColumns);
            columns = resolvedColumns;
            return found;
        }

        private static string EscapeQualified(DbConnection connection, string? schema, string table)
            => DynamicEntitySchemaResolver.EscapeQualified(connection, schema, table);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => DynamicEntitySchemaResolver.ReaderHasColumn(reader, name);

        /// <summary>
        /// Wraps a raw SQL identifier in the appropriate quoting characters for the provider.
        /// Escapes embedded quoting characters so legitimate identifiers are preserved
        /// without allowing crafted table or schema names to break out of the quote.
        /// </summary>
        private static string EscapeIdentifier(DbConnection connection, string identifier)
            => DynamicEntitySchemaResolver.EscapeIdentifier(connection, identifier);

        /// <summary>
        /// Maps a CLR type and its nullability to the property type for the generated entity.
        /// Value types that allow null are wrapped in <see cref="Nullable{T}"/>; reference types
        /// are returned as-is since nullability is implicit.
        /// </summary>
        private static Type GetPropertyType(Type type, bool allowNull)
        {
            if (!type.IsValueType)
                return type;

            if (allowNull)
                return typeof(Nullable<>).MakeGenericType(type);

            return type;
        }

        private static Type NormalizeScaffoldClrType(DbConnection connection, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null)
        {
            if (IsSqliteConnection(connection.GetType().Name)
                && IsSqliteUuidDeclaredType(declaredType))
            {
                return typeof(Guid);
            }

            if (IsSqliteConnection(connection.GetType().Name)
                && isKey
                && isAuto
                && !allowNull
                && clrType == typeof(int))
            {
                // SQLite INTEGER PRIMARY KEY aliases the 64-bit rowid even when
                // provider schema metadata reports Int32 for small test values.
                return typeof(long);
            }

            return clrType;
        }

        private static bool IsSqliteConnection(string connectionName)
            => connectionName.Contains("Sqlite", StringComparison.OrdinalIgnoreCase);

        private static bool IsPostgresConnection(string connectionName)
            => connectionName.Contains("Npgsql", StringComparison.OrdinalIgnoreCase);

        private static bool IsMySqlConnection(string connectionName)
            => connectionName.Contains("MySql", StringComparison.OrdinalIgnoreCase);

        private static bool IsSqlServerConnection(string connectionName)
            => connectionName.Contains("SqlConnection", StringComparison.OrdinalIgnoreCase)
               && !IsPostgresConnection(connectionName)
               && !IsMySqlConnection(connectionName)
               && !IsSqliteConnection(connectionName);

        private static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }

        private static string ToPascalCase(string name)
            => ScaffoldNameHelper.ToPascalCase(name);

        private static string MakeUnique(string baseName, HashSet<string> existingNames)
        {
            var candidate = string.IsNullOrWhiteSpace(baseName) ? "_" : baseName;
            var unique = candidate;
            var suffix = 2;
            while (existingNames.Contains(unique))
            {
                unique = candidate + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
                suffix++;
            }

            existingNames.Add(unique);
            return unique;
        }

        private static HashSet<string> CreateReservedMemberNameSet()
            => typeof(object)
                .GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Select(member => member.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static string EscapeCSharpIdentifier(string identifier)
            => ScaffoldNameHelper.EscapeCSharpIdentifier(identifier);
    }
}
