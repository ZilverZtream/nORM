#nullable enable
using System;
using System.Collections.Generic;
using System.Data;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static IReadOnlyList<ScaffoldEntityColumnInfo> BuildEntityColumns(
            ScaffoldEntitySourceInfo entity,
            DataTable schema)
        {
            var columns = new List<ScaffoldEntityColumnInfo>();
            foreach (DataRow row in schema.Rows)
                columns.Add(BuildEntityColumn(entity, row));

            return columns;
        }

        private static ScaffoldEntityColumnInfo BuildEntityColumn(
            ScaffoldEntitySourceInfo entity,
            DataRow row)
        {
            var colName = row["ColumnName"]!.ToString()!;
            var propName = ResolveColumnPropertyName(entity, colName);
            var allowNull = ResolveColumnNullability(entity, row, colName);
            var isKey = !entity.SuppressWriteMetadata && GetSchemaBoolean(row, "IsKey");
            var isAuto = !entity.SuppressWriteMetadata
                && (GetSchemaBoolean(row, "IsAutoIncrement")
                    || entity.IdentityColumns?.Contains(colName) == true);
            var isComputed = !entity.SuppressWriteMetadata
                && entity.ComputedColumns?.Contains(colName) == true;
            var isRowVersion = !entity.SuppressWriteMetadata
                && entity.RowVersionColumns?.Contains(colName) == true;
            var effectiveAllowNull = allowNull && !isKey && !isRowVersion;
            string? declaredType = null;
            entity.SqliteDeclaredTypes?.TryGetValue(colName, out declaredType);
            string? providerSpecificType = null;
            entity.ProviderSpecificColumnTypes?.TryGetValue(colName, out providerSpecificType);
            string? columnStoreType = null;
            entity.ColumnStoreTypes?.TryGetValue(colName, out columnStoreType);
            var rawClrType = row["DataType"] is Type type ? type : typeof(object);
            var clrType = NormalizeScaffoldClrType(
                entity.Provider,
                rawClrType,
                effectiveAllowNull,
                isKey,
                isAuto,
                declaredType,
                providerSpecificType,
                columnStoreType);

            string? columnComment = null;
            entity.Comments?.ColumnComments.TryGetValue(colName, out columnComment);
            return new ScaffoldEntityColumnInfo(
                colName,
                propName,
                clrType,
                effectiveAllowNull,
                isKey,
                isAuto,
                isComputed,
                isRowVersion,
                ResolveColumnMaxLength(entity, row, colName, clrType, providerSpecificType),
                ResolveDecimalPrecision(entity, colName),
                columnComment,
                GetColumnIndexes(entity.Indexes, colName));
        }

        private static string ResolveColumnPropertyName(ScaffoldEntitySourceInfo entity, string columnName)
            => entity.ColumnPropertyNames is not null && entity.ColumnPropertyNames.TryGetValue(columnName, out var mappedProperty)
                ? mappedProperty
                : ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(columnName));

        private static bool ResolveColumnNullability(ScaffoldEntitySourceInfo entity, DataRow row, string columnName)
            => entity.NonNullableColumns is not null
                ? !entity.NonNullableColumns.Contains(columnName)
                : row["AllowDBNull"] is bool allowNull && allowNull;

        private static bool GetSchemaBoolean(DataRow row, string columnName)
            => row.Table.Columns.Contains(columnName) && row[columnName] is bool value && value;

        private static int? ResolveColumnMaxLength(
            ScaffoldEntitySourceInfo entity,
            DataRow row,
            string columnName,
            Type clrType,
            string? providerSpecificType)
        {
            var maxLength = GetScaffoldMaxLength(clrType, row)
                ?? ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLength(providerSpecificType);
            if (maxLength.HasValue
                || entity.ColumnFacets is null
                || !entity.ColumnFacets.TryGetValue(columnName, out var columnFacet))
            {
                return maxLength;
            }

            return columnFacet.MaxLength;
        }

        private static ScaffoldEntityDecimalPrecisionInfo? ResolveDecimalPrecision(
            ScaffoldEntitySourceInfo entity,
            string columnName)
            => entity.DecimalPrecisions is not null
               && entity.DecimalPrecisions.TryGetValue(columnName, out var decimalPrecision)
                ? new ScaffoldEntityDecimalPrecisionInfo(decimalPrecision.Precision, decimalPrecision.Scale)
                : null;
    }
}
