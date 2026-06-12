#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using ColumnInfo = nORM.Scaffolding.DynamicEntityTypeGenerator.ColumnInfo;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        public static IReadOnlyList<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
        {
            var qualified = DynamicEntityConnectionKind.EscapeQualified(connection, schemaName, tableName);
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
            var columnStoreTypes = GetColumnStoreTypes(connection, schemaName, tableName);
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
                columnStoreTypes.TryGetValue(colName, out var columnStoreType);
                sqlServerAliasBaseTypes.TryGetValue(colName, out var sqlServerAliasBaseType);
                var normalizedClrType = NormalizeScaffoldClrType(connection, clrType, effectiveAllowNull, isKey, isAuto, declaredType, columnStoreType);
                normalizedClrType = ResolveProviderSpecificClrType(
                    connection,
                    normalizedClrType,
                    colName,
                    postgresDomainColumnCastTypes,
                    sqlServerAliasBaseType,
                    mySqlUnsignedColumnTypes);

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
    }
}
