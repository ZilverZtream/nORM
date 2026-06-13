#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using ColumnInfo = nORM.Scaffolding.DynamicEntityTypeGenerator.ColumnInfo;
using DynamicScaffoldColumnFacet = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldColumnFacet;
using DynamicScaffoldComputedColumn = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldComputedColumn;
using DynamicScaffoldDecimalPrecision = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldDecimalPrecision;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private static DynamicColumnMetadata ReadColumnMetadata(
            DbConnection connection,
            string? schemaName,
            string tableName,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
            => new(
                postgresDomainColumnCastTypes,
                GetComputedColumns(connection, schemaName, tableName),
                GetIdentityColumns(connection, schemaName, tableName),
                GetRowVersionColumns(connection, schemaName, tableName),
                GetSqliteDeclaredColumnTypes(connection, schemaName, tableName),
                GetColumnStoreTypes(connection, schemaName, tableName),
                GetSqlServerAliasColumnBaseTypes(connection, schemaName, tableName),
                GetMySqlUnsignedColumnTypes(connection, schemaName, tableName),
                GetDecimalPrecisions(connection, schemaName, tableName),
                GetStringBinaryFacets(connection, schemaName, tableName),
                GetPrimaryKeyOrdinals(connection, schemaName, tableName));

        private static List<ColumnInfo> BuildColumnInfos(
            DbConnection connection,
            DataTable schema,
            HashSet<string> existingPropertyNames,
            DynamicColumnMetadata metadata)
        {
            var columns = new List<ColumnInfo>(schema.Rows.Count);
            var sourceOrdinal = 0;
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]?.ToString();
                if (string.IsNullOrEmpty(colName))
                    continue;

                var currentSourceOrdinal = sourceOrdinal++;
                if (TryBuildColumnInfo(
                        connection,
                        schema,
                        row,
                        colName,
                        currentSourceOrdinal,
                        existingPropertyNames,
                        metadata,
                        out var column)
                    && column is not null)
                {
                    columns.Add(column);
                }
            }

            return columns;
        }

        private static bool TryBuildColumnInfo(
            DbConnection connection,
            DataTable schema,
            DataRow row,
            string colName,
            int currentSourceOrdinal,
            HashSet<string> existingPropertyNames,
            DynamicColumnMetadata metadata,
            out ColumnInfo? column)
        {
            var propName = MakeUnique(EscapeCSharpIdentifier(ToPascalCase(colName)), existingPropertyNames);
            if (row["DataType"] is not Type clrType)
            {
                column = null;
                return false;
            }

            var flags = ReadColumnSchemaFlags(schema, row, colName, currentSourceOrdinal, metadata);
            var normalizedClrType = ResolveColumnClrType(connection, clrType, colName, flags, metadata);
            var propertyType = GetPropertyType(normalizedClrType, flags.EffectiveAllowNull);
            metadata.ColumnFacets.TryGetValue(colName, out var columnFacet);
            var maxLength = ResolveColumnMaxLength(normalizedClrType, row, flags.SqlServerAliasBaseType, columnFacet);
            var decimalPrecision = GetColumnDecimalPrecision(normalizedClrType, colName, metadata);

            column = new ColumnInfo(
                colName,
                propName,
                propertyType,
                flags.EffectiveAllowNull,
                flags.IsKey,
                flags.KeyOrdinal,
                currentSourceOrdinal,
                flags.IsAuto,
                flags.IsComputed,
                flags.ComputedColumn,
                flags.IsRowVersion,
                maxLength,
                columnFacet.IsUnicode,
                columnFacet.IsFixedLength,
                decimalPrecision);
            return true;
        }

        private static ColumnSchemaFlags ReadColumnSchemaFlags(
            DataTable schema,
            DataRow row,
            string colName,
            int currentSourceOrdinal,
            DynamicColumnMetadata metadata)
        {
            var allowNull = row["AllowDBNull"] is bool b && b;
            var isKey = schema.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
            var keyOrdinal = metadata.PrimaryKeyOrdinals.TryGetValue(colName, out var ordinal)
                ? ordinal
                : isKey ? currentSourceOrdinal + 1 : 0;
            var isAuto = (schema.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai)
                || metadata.IdentityColumns.Contains(colName);
            var isComputed = (schema.Columns.Contains("IsExpression") && row["IsExpression"] is bool expression && expression)
                || metadata.ComputedColumns.ContainsKey(colName);
            var computedColumn = metadata.ComputedColumns.TryGetValue(colName, out var computed)
                ? computed
                : (DynamicScaffoldComputedColumn?)null;
            var isRowVersion = metadata.RowVersionColumns.Contains(colName);
            var effectiveAllowNull = allowNull && !isKey && !isRowVersion;
            metadata.SqliteDeclaredTypes.TryGetValue(colName, out var declaredType);
            metadata.ColumnStoreTypes.TryGetValue(colName, out var columnStoreType);
            metadata.SqlServerAliasBaseTypes.TryGetValue(colName, out var sqlServerAliasBaseType);

            return new ColumnSchemaFlags(
                isKey,
                keyOrdinal,
                isAuto,
                isComputed,
                computedColumn,
                isRowVersion,
                effectiveAllowNull,
                declaredType,
                columnStoreType,
                sqlServerAliasBaseType);
        }

        private static Type ResolveColumnClrType(
            DbConnection connection,
            Type clrType,
            string colName,
            ColumnSchemaFlags flags,
            DynamicColumnMetadata metadata)
        {
            var normalizedClrType = NormalizeScaffoldClrType(
                connection,
                clrType,
                flags.EffectiveAllowNull,
                flags.IsKey,
                flags.IsAuto,
                flags.DeclaredType,
                flags.ColumnStoreType);

            return ResolveProviderSpecificClrType(
                connection,
                normalizedClrType,
                colName,
                metadata.PostgresDomainColumnCastTypes,
                flags.SqlServerAliasBaseType,
                metadata.MySqlUnsignedColumnTypes);
        }

        private static int? ResolveColumnMaxLength(
            Type normalizedClrType,
            DataRow row,
            string? sqlServerAliasBaseType,
            DynamicScaffoldColumnFacet columnFacet)
            => GetScaffoldMaxLength(normalizedClrType, row)
               ?? GetSqlServerAliasBaseMaxLengthFromTypeText(sqlServerAliasBaseType)
               ?? columnFacet.MaxLength;

        private static DynamicScaffoldDecimalPrecision? GetColumnDecimalPrecision(
            Type normalizedClrType,
            string colName,
            DynamicColumnMetadata metadata)
            => normalizedClrType == typeof(decimal)
               && metadata.DecimalPrecisions.TryGetValue(colName, out var precision)
                ? precision
                : (DynamicScaffoldDecimalPrecision?)null;
    }
}
