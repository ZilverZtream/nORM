#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;

using ColumnInfo = nORM.Scaffolding.DynamicEntityTypeGenerator.ColumnInfo;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityTableSchemaReader
    {
        public static IReadOnlyList<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
        {
            var qualified = DynamicEntitySchemaResolver.EscapeQualified(connection, schemaName, tableName);
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
                    : (DynamicEntityTypeGenerator.ScaffoldComputedColumn?)null;
                var isRowVersion = rowVersionColumns.Contains(colName);
                var effectiveAllowNull = allowNull && !isKey && !isRowVersion;
                sqliteDeclaredTypes.TryGetValue(colName, out var declaredType);
                sqlServerAliasBaseTypes.TryGetValue(colName, out var sqlServerAliasBaseType);
                var normalizedClrType = NormalizeScaffoldClrType(connection, clrType, effectiveAllowNull, isKey, isAuto, declaredType);
                if (DynamicEntitySchemaResolver.IsPostgresConnection(connection.GetType().Name)
                    && normalizedClrType == typeof(Array)
                    && postgresDomainColumnCastTypes.TryGetValue(colName, out var domainCastType)
                    && ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(domainCastType.Trim().ToLowerInvariant(), out var arrayClrType))
                {
                    normalizedClrType = arrayClrType;
                }
                else if (DynamicEntitySchemaResolver.IsSqlServerConnection(connection.GetType().Name)
                         && ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrTypeName(sqlServerAliasBaseType, out var aliasClrType))
                {
                    normalizedClrType = aliasClrType;
                }
                else if (DynamicEntitySchemaResolver.IsMySqlConnection(connection.GetType().Name)
                         && mySqlUnsignedColumnTypes.TryGetValue(colName, out var unsignedColumnType)
                         && ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(unsignedColumnType, out var unsignedClrType))
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
                    : (DynamicEntityTypeGenerator.ScaffoldDecimalPrecision?)null;

                columns.Add(new ColumnInfo(colName, propName, propertyType, effectiveAllowNull, isKey, keyOrdinal, currentSourceOrdinal, isAuto, isComputed, computedColumn, isRowVersion, maxLength, columnFacet.IsUnicode, columnFacet.IsFixedLength, decimalPrecision));
            }

            return columns;
        }

        public static string BuildSchemaProbeSql(
            DbConnection connection,
            string? schemaName,
            string tableName,
            string qualified,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
        {
            if (!DynamicEntitySchemaResolver.IsPostgresConnection(connection.GetType().Name) || postgresDomainColumnCastTypes.Count == 0)
                return $"SELECT * FROM {qualified} WHERE 1=0";

            var columnNames = DynamicEntitySchemaMetadataReader.GetPostgresColumnNames(connection, schemaName, tableName);
            if (columnNames.Count == 0)
                return $"SELECT * FROM {qualified} WHERE 1=0";

            var projection = columnNames.Select(column =>
            {
                var escaped = DynamicEntitySchemaResolver.EscapeIdentifier(connection, column);
                return postgresDomainColumnCastTypes.TryGetValue(column, out var castType)
                    ? $"{escaped}::{castType} AS {escaped}"
                    : escaped;
            });

            return $"SELECT {string.Join(", ", projection)} FROM {qualified} WHERE 1=0";
        }

        public static int? GetScaffoldMaxLength(Type clrType, DataRow row)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            if (!row.Table.Columns.Contains("ColumnSize") || row["ColumnSize"] == DBNull.Value)
                return null;

            return int.TryParse(row["ColumnSize"]?.ToString(), out var size) && size > 0 && !IsUnboundedScaffoldMaxLength(size)
                ? size
                : null;
        }

        public static bool IsUnboundedScaffoldMaxLength(int size)
            => size == int.MaxValue
               || size == 1073741823;

        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldColumnFacet> GetStringBinaryFacets(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetStringBinaryFacets(connection, schemaName, tableName)
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => new DynamicEntityTypeGenerator.ScaffoldColumnFacet(pair.Value.MaxLength, pair.Value.IsUnicode, pair.Value.IsFixedLength),
                    StringComparer.OrdinalIgnoreCase);

        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldDecimalPrecision> GetDecimalPrecisions(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetDecimalPrecisions(connection, schemaName, tableName)
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => new DynamicEntityTypeGenerator.ScaffoldDecimalPrecision(pair.Value.Precision, pair.Value.Scale),
                    StringComparer.OrdinalIgnoreCase);

        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetComputedColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.GetComputedColumns(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetSqliteDeclaredColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetSqliteDeclaredColumnTypes(connection, schemaName, tableName);

        public static IReadOnlySet<string> GetIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetIdentityColumns(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, int> GetPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPrimaryKeyOrdinals(connection, schemaName, tableName);

        public static IReadOnlySet<string> GetRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetRowVersionColumns(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetPostgresDomainColumnCastTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetMySqlUnsignedColumnTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetMySqlUnsignedColumnTypes(connection, schemaName, tableName);

        public static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntitySchemaMetadataReader.GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName);

        public static Type GetPropertyType(Type type, bool allowNull)
        {
            if (!type.IsValueType)
                return type;

            if (allowNull)
                return typeof(Nullable<>).MakeGenericType(type);

            return type;
        }

        public static Type NormalizeScaffoldClrType(DbConnection connection, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null)
        {
            if (DynamicEntitySchemaResolver.IsSqliteConnection(connection.GetType().Name)
                && IsSqliteUuidDeclaredType(declaredType))
            {
                return typeof(Guid);
            }

            if (DynamicEntitySchemaResolver.IsSqliteConnection(connection.GetType().Name)
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

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && DynamicEntityReadOnlyClassifier.ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string? typeText)
            => string.IsNullOrWhiteSpace(typeText)
                ? null
                : ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);

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
