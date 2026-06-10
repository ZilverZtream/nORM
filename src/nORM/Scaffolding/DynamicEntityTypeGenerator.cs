using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;

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
        private sealed record ColumnInfo(string ColumnName, string PropertyName, Type PropertyType, bool AllowsNull, bool IsKey, int KeyOrdinal, int SourceOrdinal, bool IsAuto, bool IsComputed, ScaffoldComputedColumn? ComputedColumn, bool IsRowVersion, int? MaxLength, bool? IsUnicode, bool IsFixedLength, ScaffoldDecimalPrecision? DecimalPrecision);

        private readonly record struct ScaffoldComputedColumn(string Sql, bool Stored);
        private readonly record struct ScaffoldDecimalPrecision(int Precision, int? Scale);
        private readonly record struct ScaffoldColumnFacet(int? MaxLength, bool? IsUnicode, bool IsFixedLength);

        /// <summary>Namespace prefix used for all dynamically generated entity types.</summary>
        private const string DynamicTypeNamespace = "nORM.Dynamic";

        /// <summary>Name of the shared dynamic assembly that hosts all generated entity types.</summary>
        private const string DynamicAssemblyName = "nORM.Dynamic.Entities";

        /// <summary>Name of the dynamic module within the shared assembly.</summary>
        private const string DynamicModuleName = "MainModule";

        /// <summary>
        /// Number of leading bytes from the SHA-256 hash used as the schema signature.
        /// 16 bytes yields a 32-character hex string with negligible collision probability.
        /// </summary>
        private const int SchemaSignatureTruncationBytes = 16;

        private const int MaxScaffoldedMySqlSetValueCount = 8;

        // Shared static AssemblyBuilder and ModuleBuilder for all generated types,
        // preventing unloadable assembly accumulation when types are evicted from cache.
        private static readonly AssemblyBuilder _sharedAssembly;
        private static readonly ModuleBuilder _sharedModule;
        private static readonly object _moduleBuilderLock = new();
        private static long _typeCounter;

        static DynamicEntityTypeGenerator()
        {
            var assemblyName = new AssemblyName(DynamicAssemblyName);
            _sharedAssembly = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            _sharedModule = _sharedAssembly.DefineDynamicModule(DynamicModuleName);
        }

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
        {
            lock (_moduleBuilderLock)
            {
                var className = EscapeCSharpIdentifier(ToPascalCase(tableName));

                // Generate unique type name to avoid conflicts when same table is regenerated
                var typeId = Interlocked.Increment(ref _typeCounter);
                var uniqueTypeName = $"{DynamicTypeNamespace}.{className}_{typeId}";

                // Create type using shared module
                var typeBuilder = _sharedModule.DefineType(
                    uniqueTypeName,
                    TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed,
                    typeof(object));

                // Add parameterless constructor
                typeBuilder.DefineDefaultConstructor(
                    MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);

                var tableAttrCtor = typeof(TableAttribute).GetConstructor(new[] { typeof(string) })!;
                if (schemaName is null)
                {
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(tableAttrCtor, new object[] { tableName }));
                }
                else
                {
                    var schemaProperty = typeof(TableAttribute).GetProperty(nameof(TableAttribute.Schema))!;
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(
                        tableAttrCtor,
                        new object[] { tableName },
                        new[] { schemaProperty },
                        new object[] { schemaName }));
                }

                var orderedColumns = OrderDynamicColumns(columns);

                if (isReadOnlyEntity || !orderedColumns.Any(static column => column.IsKey))
                {
                    var readOnlyAttrCtor = typeof(ReadOnlyEntityAttribute).GetConstructor(Type.EmptyTypes)!;
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(readOnlyAttrCtor, Array.Empty<object>()));
                }

                // Add properties for each column
                foreach (var col in orderedColumns)
                {
                    var propertyType = col.PropertyType;
                    var fieldBuilder = typeBuilder.DefineField($"_{col.PropertyName}", propertyType, FieldAttributes.Private);
                    var propertyBuilder = typeBuilder.DefineProperty(col.PropertyName, PropertyAttributes.HasDefault, propertyType, null);

                    // Add [Column] attribute mapping to the original database column name
                    var columnAttrCtor = typeof(ColumnAttribute).GetConstructor(new[] { typeof(string) })!;
                    var columnAttr = col.DecimalPrecision is { } decimalPrecision
                        ? new CustomAttributeBuilder(
                            columnAttrCtor,
                            new object[] { col.ColumnName },
                            new[] { typeof(ColumnAttribute).GetProperty(nameof(ColumnAttribute.TypeName))! },
                            new object[]
                            {
                                FormatDecimalTypeName(decimalPrecision)
                            })
                        : new CustomAttributeBuilder(columnAttrCtor, new object[] { col.ColumnName });
                    propertyBuilder.SetCustomAttribute(columnAttr);

                    // Add [Key] attribute for primary key columns
                    if (col.IsKey)
                    {
                        var keyAttrCtor = typeof(KeyAttribute).GetConstructor(Type.EmptyTypes)!;
                        var keyAttr = new CustomAttributeBuilder(keyAttrCtor, Array.Empty<object>());
                        propertyBuilder.SetCustomAttribute(keyAttr);
                    }

                    if (col.IsRowVersion)
                    {
                        var timestampAttrCtor = typeof(TimestampAttribute).GetConstructor(Type.EmptyTypes)!;
                        var timestampAttr = new CustomAttributeBuilder(timestampAttrCtor, Array.Empty<object>());
                        propertyBuilder.SetCustomAttribute(timestampAttr);
                    }

                    // Add database-generated attribute for identity/computed/rowversion columns.
                    if (col.IsAuto || col.IsComputed || col.IsRowVersion)
                    {
                        var dbGenAttrCtor = typeof(DatabaseGeneratedAttribute).GetConstructor(new[] { typeof(DatabaseGeneratedOption) })!;
                        var option = col.IsAuto ? DatabaseGeneratedOption.Identity : DatabaseGeneratedOption.Computed;
                        var dbGenAttr = new CustomAttributeBuilder(dbGenAttrCtor, new object[] { option });
                        propertyBuilder.SetCustomAttribute(dbGenAttr);
                    }

                    // Add [MaxLength] attribute for string columns with a known size
                    if (col.MaxLength.HasValue)
                    {
                        var maxLenAttrCtor = typeof(MaxLengthAttribute).GetConstructor(new[] { typeof(int) })!;
                        var maxLenAttr = new CustomAttributeBuilder(maxLenAttrCtor, new object[] { col.MaxLength.Value });
                        propertyBuilder.SetCustomAttribute(maxLenAttr);
                    }

                    if (!propertyType.IsValueType && !col.AllowsNull)
                    {
                        var requiredAttrCtor = typeof(RequiredAttribute).GetConstructor(Type.EmptyTypes)!;
                        var requiredAttr = new CustomAttributeBuilder(requiredAttrCtor, Array.Empty<object>());
                        propertyBuilder.SetCustomAttribute(requiredAttr);
                    }

                    // Define getter
                    var getMethod = typeBuilder.DefineMethod(
                        $"get_{col.PropertyName}",
                        MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                        propertyType,
                        Type.EmptyTypes);

                    var getIl = getMethod.GetILGenerator();
                    getIl.Emit(OpCodes.Ldarg_0);
                    getIl.Emit(OpCodes.Ldfld, fieldBuilder);
                    getIl.Emit(OpCodes.Ret);

                    // Define setter
                    var setMethod = typeBuilder.DefineMethod(
                        $"set_{col.PropertyName}",
                        MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                        null,
                        new[] { propertyType });

                    var setIl = setMethod.GetILGenerator();
                    setIl.Emit(OpCodes.Ldarg_0);
                    setIl.Emit(OpCodes.Ldarg_1);
                    setIl.Emit(OpCodes.Stfld, fieldBuilder);
                    setIl.Emit(OpCodes.Ret);

                    propertyBuilder.SetGetMethod(getMethod);
                    propertyBuilder.SetSetMethod(setMethod);
                }

                return typeBuilder.CreateType()!;
            }
        }

        private static IReadOnlyList<ColumnInfo> OrderDynamicColumns(IReadOnlyList<ColumnInfo> columns)
            => columns
                .OrderBy(static column => column.IsKey ? 0 : 1)
                .ThenBy(static column => column.IsKey ? column.KeyOrdinal : int.MaxValue)
                .ThenBy(static column => column.SourceOrdinal)
                .ToArray();

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
        {
            var sb = new StringBuilder();
            AppendDescriptorPart(sb, schemaName ?? string.Empty);
            AppendDescriptorPart(sb, tableName);
            AppendDescriptorPart(sb, isReadOnlyEntity ? "RO" : "RW");
            foreach (var column in columns)
            {
                AppendDescriptorPart(sb, column.ColumnName);
                AppendDescriptorPart(sb, column.PropertyType.FullName ?? string.Empty);
                AppendDescriptorPart(sb, column.IsKey ? "PK" : "C");
                AppendDescriptorPart(sb, column.IsKey ? column.KeyOrdinal.ToString(System.Globalization.CultureInfo.InvariantCulture) : "-");
                AppendDescriptorPart(sb, column.AllowsNull ? "N" : "NN");
                AppendDescriptorPart(sb, column.IsAuto ? "AI" : "NA");
                AppendDescriptorPart(sb, column.IsComputed ? "CMP" : "NCMP");
                AppendDescriptorPart(sb, column.ComputedColumn?.Sql ?? "-");
                AppendDescriptorPart(sb, column.ComputedColumn is { } computedColumn ? computedColumn.Stored ? "STORED" : "VIRTUAL" : "-");
                AppendDescriptorPart(sb, column.IsRowVersion ? "RV" : "NRV");
                AppendDescriptorPart(sb, column.MaxLength?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-");
                AppendDescriptorPart(sb, column.IsUnicode.HasValue ? column.IsUnicode.Value ? "U" : "NU" : "-");
                AppendDescriptorPart(sb, column.IsFixedLength ? "FIX" : "VAR");
                AppendDescriptorPart(
                    sb,
                    column.DecimalPrecision is { } decimalPrecision
                        ? FormatDecimalTypeName(decimalPrecision)
                        : "-");
            }

            return sb.ToString();
        }

        private static void AppendDescriptorPart(StringBuilder builder, string value)
        {
            builder.Append(value.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));
            builder.Append(':');
            builder.Append(value);
            builder.Append(';');
        }

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

        private static string FormatDecimalTypeName(ScaffoldDecimalPrecision decimalPrecision)
        {
            var precision = decimalPrecision.Precision.ToString(System.Globalization.CultureInfo.InvariantCulture);
            return decimalPrecision.Scale.HasValue
                ? $"decimal({precision},{decimalPrecision.Scale.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})"
                : $"decimal({precision})";
        }

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> GetComputedColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;

            if (IsSqliteConnection(connectionName))
            {
                var result = new Dictionary<string, ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
                foreach (var (columnName, computed) in ExtractSqliteGeneratedColumns(GetSqliteCreateTableSql(connection, schemaName, tableName)))
                    result[columnName] = computed;

                using var cmd = connection.CreateCommand();
                var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                    ? string.Empty
                    : EscapeIdentifier(connection, schemaName!) + ".";
                cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({EscapeIdentifier(connection, tableName)})";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    if (!ReaderHasColumn(reader, "hidden")
                        || !ReaderHasColumn(reader, "name")
                        || Convert.ToInt32(reader["hidden"], System.Globalization.CultureInfo.InvariantCulture) is not (2 or 3))
                    {
                        continue;
                    }

                    var name = Convert.ToString(reader["name"]);
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        if (!result.ContainsKey(name))
                            result[name] = new ScaffoldComputedColumn(string.Empty, Stored: false);
                    }
                }

                return result;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryComputedColumnMap(connection, """
                    SELECT c.name AS ColumnName,
                           CONVERT(nvarchar(max), cc.definition) AS ComputedSql,
                           CONVERT(bit, cc.is_persisted) AS IsStored
                    FROM sys.computed_columns cc
                    INNER JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                    INNER JOIN sys.tables t ON t.object_id = cc.object_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryComputedColumnMap(connection, """
                    SELECT column_name AS ColumnName,
                           generation_expression AS ComputedSql,
                           1 AS IsStored
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND is_generated <> 'NEVER'
                    """, schemaName, tableName);
            }

            if (IsMySqlConnection(connectionName))
            {
                return QueryComputedColumnMap(connection, """
                    SELECT column_name AS ColumnName,
                           generation_expression AS ComputedSql,
                           CASE
                               WHEN LOWER(COALESCE(extra, '')) LIKE '%stored generated%' THEN 1
                               ELSE 0
                           END AS IsStored
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND generation_expression IS NOT NULL
                      AND generation_expression <> ''
                    """, schemaName, tableName);
            }

            return new Dictionary<string, ScaffoldComputedColumn>(0, StringComparer.OrdinalIgnoreCase);
        }

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
        {
            var result = new Dictionary<string, ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            var tableParameter = cmd.CreateParameter();
            tableParameter.ParameterName = "@tableName";
            tableParameter.DbType = DbType.String;
            tableParameter.Value = tableName;
            cmd.Parameters.Add(tableParameter);
            var schemaParameter = cmd.CreateParameter();
            schemaParameter.ParameterName = "@schemaName";
            schemaParameter.DbType = DbType.String;
            schemaParameter.Value = string.IsNullOrWhiteSpace(schemaName) ? DBNull.Value : schemaName;
            cmd.Parameters.Add(schemaParameter);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                var rawSql = Convert.ToString(reader["ComputedSql"]) ?? string.Empty;
                var isStored = ReaderHasColumn(reader, "IsStored")
                               && reader["IsStored"] != DBNull.Value
                               && ConvertMetadataBoolean(reader["IsStored"]);
                var (computedSql, stored) = NormalizeComputedColumnSql(rawSql + (isStored ? " STORED" : string.Empty));
                result[columnName] = new ScaffoldComputedColumn(computedSql, stored);
            }

            return result;
        }

        private static string? GetSqliteCreateTableSql(DbConnection connection, string? schemaName, string tableName)
        {
            using var command = connection.CreateCommand();
            var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                ? string.Empty
                : EscapeIdentifier(connection, schemaName!) + ".";
            command.CommandText = $"SELECT sql FROM {schemaPrefix}sqlite_master WHERE type = 'table' AND name = @tableName";
            var tableParameter = command.CreateParameter();
            tableParameter.ParameterName = "@tableName";
            tableParameter.DbType = DbType.String;
            tableParameter.Value = tableName;
            command.Parameters.Add(tableParameter);
            return Convert.ToString(command.ExecuteScalar());
        }

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> ExtractSqliteGeneratedColumns(string? createTableSql)
        {
            var result = new Dictionary<string, ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(createTableSql))
                return result;

            var bodyOpen = createTableSql.IndexOf('(');
            if (bodyOpen < 0)
                return result;

            var bodyClose = FindMatchingParenthesis(createTableSql, bodyOpen);
            if (bodyClose <= bodyOpen)
                return result;

            foreach (var part in SplitTopLevelCommaSeparated(createTableSql.Substring(bodyOpen + 1, bodyClose - bodyOpen - 1)))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0 || StartsWithTableConstraint(trimmed))
                    continue;

                if (!TryReadLeadingSqlIdentifier(trimmed, out var columnName, out var afterIdentifier))
                    continue;

                var generatedIndex = FindSqlKeywordOutsideQuotes(trimmed, "GENERATED", afterIdentifier);
                if (generatedIndex < 0)
                    continue;

                var openIndex = trimmed.IndexOf('(', generatedIndex);
                if (openIndex < 0)
                    continue;

                var closeIndex = FindMatchingParenthesis(trimmed, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var rawSql = trimmed.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                var suffix = trimmed[(closeIndex + 1)..];
                var stored = FindSqlKeywordOutsideQuotes(suffix, "STORED", 0) >= 0
                             || FindSqlKeywordOutsideQuotes(suffix, "PERSISTED", 0) >= 0;
                var (computedSql, normalizedStored) = NormalizeComputedColumnSql(rawSql + (stored ? " STORED" : string.Empty));
                if (!string.IsNullOrWhiteSpace(computedSql))
                    result[columnName] = new ScaffoldComputedColumn(computedSql, normalizedStored);
            }

            return result;
        }

        private static (string Sql, bool Stored) NormalizeComputedColumnSql(string raw)
        {
            var candidate = raw.Trim();
            if (candidate.EndsWith("generated column", StringComparison.OrdinalIgnoreCase))
                return (string.Empty, false);

            var stored = false;
            if (candidate.StartsWith("GENERATED", StringComparison.OrdinalIgnoreCase))
            {
                var open = candidate.IndexOf('(');
                var close = open >= 0 ? FindMatchingParenthesis(candidate, open) : -1;
                if (open >= 0 && close > open)
                {
                    var suffix = candidate[(close + 1)..];
                    stored = FindSqlKeywordOutsideQuotes(suffix, "STORED", 0) >= 0
                             || FindSqlKeywordOutsideQuotes(suffix, "PERSISTED", 0) >= 0;
                    candidate = candidate.Substring(open + 1, close - open - 1).Trim();
                }
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            if (TryTrimTrailingComputedStorageToken(candidate, "VIRTUAL", out var virtualTrimmed))
            {
                candidate = virtualTrimmed;
            }
            else if (TryTrimTrailingComputedStorageToken(candidate, "STORED", out var storedTrimmed))
            {
                candidate = storedTrimmed;
                stored = true;
            }
            else if (TryTrimTrailingComputedStorageToken(candidate, "PERSISTED", out var persistedTrimmed))
            {
                candidate = persistedTrimmed;
                stored = true;
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            return (candidate, stored);
        }

        private static bool TryTrimTrailingComputedStorageToken(string sql, string token, out string trimmedSql)
        {
            trimmedSql = sql;
            var trimmed = sql.TrimEnd();
            if (!trimmed.EndsWith(token, StringComparison.OrdinalIgnoreCase))
                return false;

            var tokenStart = trimmed.Length - token.Length;
            if (tokenStart <= 0 || !char.IsWhiteSpace(trimmed[tokenStart - 1]))
                return false;

            trimmedSql = trimmed[..tokenStart].TrimEnd();
            return true;
        }

        private static bool HasBalancedOuterParentheses(string value)
            => FindMatchingParenthesis(value, 0) == value.Length - 1;

        private static int FindMatchingParenthesis(string sql, int openIndex)
        {
            var depth = 0;
            char? quote = null;
            for (var i = openIndex; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }

            return -1;
        }

        private static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
        {
            char? quote = null;
            for (var i = Math.Max(0, startIndex); i < sql.Length; i++)
            {
                var ch = sql[i];
                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (i + keyword.Length <= sql.Length
                    && sql.AsSpan(i, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase)
                    && (i == 0 || !IsSqlIdentifierChar(sql[i - 1]))
                    && (i + keyword.Length == sql.Length || !IsSqlIdentifierChar(sql[i + keyword.Length])))
                {
                    return i;
                }
            }

            return -1;
        }

        private static bool IsSqlIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_' || value == '$';

        private static IReadOnlyList<string> SplitTopLevelCommaSeparated(string text)
        {
            var result = new List<string>();
            var start = 0;
            var depth = 0;
            char? quote = null;
            for (var i = 0; i < text.Length; i++)
            {
                var ch = text[i];
                if (quote is not null)
                {
                    var close = quote == '[' ? ']' : quote.Value;
                    if (ch == close)
                    {
                        if (i + 1 < text.Length && text[i + 1] == close)
                        {
                            i++;
                            continue;
                        }

                        quote = null;
                    }

                    continue;
                }

                if (ch is '\'' or '"' or '`' or '[')
                {
                    quote = ch;
                    continue;
                }

                if (ch == '(')
                {
                    depth++;
                    continue;
                }

                if (ch == ')' && depth > 0)
                {
                    depth--;
                    continue;
                }

                if (ch == ',' && depth == 0)
                {
                    result.Add(text.Substring(start, i - start));
                    start = i + 1;
                }
            }

            result.Add(text[start..]);
            return result;
        }

        private static bool StartsWithTableConstraint(string value)
            => value.StartsWith("CONSTRAINT ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("PRIMARY ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("FOREIGN ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("UNIQUE ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("CHECK ", StringComparison.OrdinalIgnoreCase);

        private static bool TryReadLeadingSqlIdentifier(string value, out string identifier, out int nextIndex)
        {
            identifier = string.Empty;
            nextIndex = 0;
            while (nextIndex < value.Length && char.IsWhiteSpace(value[nextIndex]))
                nextIndex++;

            if (nextIndex >= value.Length)
                return false;

            var start = nextIndex;
            var quote = value[nextIndex];
            if (quote is '"' or '`' or '[')
            {
                var close = quote == '[' ? ']' : quote;
                nextIndex++;
                var sb = new StringBuilder();
                while (nextIndex < value.Length)
                {
                    var ch = value[nextIndex++];
                    if (ch == close)
                    {
                        if (nextIndex < value.Length && value[nextIndex] == close)
                        {
                            sb.Append(close);
                            nextIndex++;
                            continue;
                        }

                        identifier = sb.ToString();
                        return identifier.Length > 0;
                    }

                    sb.Append(ch);
                }

                nextIndex = start;
                return false;
            }

            if (!(char.IsLetter(quote) || quote == '_'))
                return false;

            nextIndex++;
            while (nextIndex < value.Length && IsSqlIdentifierChar(value[nextIndex]))
                nextIndex++;

            identifier = value.Substring(start, nextIndex - start);
            return identifier.Length > 0;
        }

        private static bool ConvertMetadataBoolean(object value)
            => value switch
            {
                bool b => b,
                byte b => b != 0,
                short s => s != 0,
                int i => i != 0,
                long l => l != 0,
                string s => s.Equals("true", StringComparison.OrdinalIgnoreCase)
                            || s.Equals("yes", StringComparison.OrdinalIgnoreCase)
                            || s.Equals("1", StringComparison.OrdinalIgnoreCase),
                _ => Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture) != 0
            };

        private static (string? schema, string table) SplitSchema(string identifier)
        {
            var idx = identifier.IndexOf('.');
            if (idx > 0 && idx < identifier.Length - 1)
                return (identifier[..idx], identifier[(idx + 1)..]);
            return (null, identifier);
        }

        private static (string? schema, string table, List<ColumnInfo> columns, bool isReadOnlyEntity) ResolveTableSchema(DbConnection connection, string tableName)
        {
            if (tableName.Contains('.', StringComparison.Ordinal))
            {
                var exactFound = TryGetTableSchema(connection, null, tableName, out var exactColumns);
                var (schemaName, bareTable) = SplitSchema(tableName);
                var schemaFound = TryGetTableSchema(connection, schemaName, bareTable, out var schemaColumns);

                if (exactFound && schemaFound)
                {
                    throw new NormConfigurationException(
                        $"Dynamic scaffolding table name '{tableName}' is ambiguous: it matches both a literal table name and a schema-qualified table. " +
                        "Use a typed model or remove the naming collision before using Query(string).");
                }

                if (exactFound)
                    return (null, tableName, exactColumns, IsReadOnlyDynamicObject(connection, null, tableName));

                if (schemaFound)
                    return (schemaName, bareTable, schemaColumns, IsReadOnlyDynamicObject(connection, schemaName, bareTable));
            }

            var (fallbackSchemaName, fallbackBareTable) = SplitSchema(tableName);
            if (fallbackSchemaName is null)
                fallbackSchemaName = ResolveUniqueUnqualifiedSchema(connection, fallbackBareTable);

            var columns = GetTableSchema(connection, fallbackSchemaName, fallbackBareTable).ToList();
            return (fallbackSchemaName, fallbackBareTable, columns, IsReadOnlyDynamicObject(connection, fallbackSchemaName, fallbackBareTable));
        }

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

        private static void AddStringParameter(DbCommand command, string name, string? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = DbType.String;
            parameter.Value = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
            command.Parameters.Add(parameter);
        }

        private static string? ResolveUniqueUnqualifiedSchema(DbConnection connection, string tableName)
        {
            var schemas = GetMatchingObjectSchemas(connection, tableName);
            if (schemas.Count > 1)
            {
                throw new NormConfigurationException(
                    $"Dynamic scaffolding table name '{tableName}' is ambiguous because it exists in multiple schemas/catalogs: " +
                    string.Join(", ", schemas.Select(schema => "'" + schema + "'")) +
                    ". Use a schema-qualified table name before using Query(string).");
            }

            if (schemas.Count == 1)
            {
                var connectionName = connection.GetType().Name;
                if (IsSqlServerConnection(connectionName) || IsPostgresConnection(connectionName))
                    return schemas[0];
            }

            return null;
        }

        private static IReadOnlyList<string> GetMatchingObjectSchemas(DbConnection connection, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
                return GetSqliteMatchingObjectSchemas(connection, tableName);

            if (IsSqlServerConnection(connectionName))
            {
                return QuerySchemaNameList(connection, """
                    SELECT s.name AS SchemaName
                    FROM sys.objects o
                    INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
                    WHERE o.name = @tableName
                      AND o.type IN ('U', 'V')
                      AND o.is_ms_shipped = 0
                    ORDER BY s.name
                    """, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QuerySchemaNameList(connection, """
                    SELECT table_schema AS SchemaName
                    FROM information_schema.tables
                    WHERE table_name = @tableName
                      AND table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_schema
                    """, tableName);
            }

            return Array.Empty<string>();
        }

        private static IReadOnlyList<string> GetSqliteMatchingObjectSchemas(DbConnection connection, string tableName)
        {
            var schemas = new List<string>();
            using (var schemaCommand = connection.CreateCommand())
            {
                schemaCommand.CommandText = "PRAGMA database_list";
                using var reader = schemaCommand.ExecuteReader();
                while (reader.Read())
                {
                    var schemaName = ReaderHasColumn(reader, "name")
                        ? Convert.ToString(reader["name"])
                        : reader.FieldCount > 1 ? Convert.ToString(reader[1]) : null;
                    if (!string.IsNullOrWhiteSpace(schemaName))
                        schemas.Add(schemaName);
                }
            }

            var matches = new List<string>();
            foreach (var schema in schemas.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT name FROM {EscapeIdentifier(connection, schema)}.sqlite_master WHERE type IN ('table', 'view') AND name = @tableName LIMIT 1";
                var tableParameter = command.CreateParameter();
                tableParameter.ParameterName = "@tableName";
                tableParameter.DbType = DbType.String;
                tableParameter.Value = tableName;
                command.Parameters.Add(tableParameter);
                if (command.ExecuteScalar() is not null)
                    matches.Add(schema);
            }

            return matches;
        }

        private static IReadOnlyList<string> QuerySchemaNameList(DbConnection connection, string sql, string tableName)
        {
            var result = new List<string>();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            var tableParameter = cmd.CreateParameter();
            tableParameter.ParameterName = "@tableName";
            tableParameter.DbType = DbType.String;
            tableParameter.Value = tableName;
            cmd.Parameters.Add(tableParameter);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var schemaName = Convert.ToString(reader["SchemaName"]);
                if (!string.IsNullOrWhiteSpace(schemaName))
                    result.Add(schemaName);
            }

            return result
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(schema => schema, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        private static bool TryGetTableSchema(DbConnection connection, string? schemaName, string tableName, out List<ColumnInfo> columns)
        {
            try
            {
                columns = GetTableSchema(connection, schemaName, tableName).ToList();
                return true;
            }
            catch (DbException)
            {
                columns = new List<ColumnInfo>();
                return false;
            }
        }

        private static string EscapeQualified(DbConnection connection, string? schema, string table)
        {
            return string.IsNullOrEmpty(schema)
                ? EscapeIdentifier(connection, table)
                : $"{EscapeIdentifier(connection, schema!)}.{EscapeIdentifier(connection, table)}";
        }

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Wraps a raw SQL identifier in the appropriate quoting characters for the provider.
        /// Escapes embedded quoting characters so legitimate identifiers are preserved
        /// without allowing crafted table or schema names to break out of the quote.
        /// </summary>
        private static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            var name = connection.GetType().Name.ToLowerInvariant();
            return name switch
            {
                var n when n.Contains("sqlite") => $"\"{identifier.Replace("\"", "\"\"")}\"",
                var n when n.Contains("npgsql") => $"\"{identifier.Replace("\"", "\"\"")}\"",
                var n when n.Contains("mysql") => $"`{identifier.Replace("`", "``")}`",
                var n when n.Contains("sqlconnection") => $"[{identifier.Replace("]", "]]")}]",
                _ => $"\"{identifier.Replace("\"", "\"\"")}\""
            };
        }

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
