#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static IReadOnlyList<ScaffoldEntityColumnInfo> BuildEntityColumns(
            ScaffoldEntitySourceInfo entity,
            DataTable schema)
        {
            var rows = schema.Rows.Cast<DataRow>().ToList();
            var propertyNames = AssignUniqueColumnPropertyNames(
                entity.EntityName,
                entity.ColumnPropertyNames,
                rows.Select(row => row["ColumnName"]!.ToString()!).ToList());

            var columns = new List<ScaffoldEntityColumnInfo>(rows.Count);
            for (var i = 0; i < rows.Count; i++)
                columns.Add(BuildEntityColumn(entity, rows[i], propertyNames[i]));

            return columns;
        }

        /// <summary>
        /// Assigns each physical column a distinct C# property name. A view (and some stored-procedure
        /// result sets) can legally expose the same column name more than once, and the column->property
        /// map is keyed by column name, so those duplicates collapse to a single entry. Without this pass
        /// every physical occurrence would render the same property name, producing a CS0102 "already
        /// contains a definition" compile error. The first occurrence keeps its mapped name verbatim (so
        /// non-duplicate entities are byte-identical to prior output and stay aligned with any model
        /// configuration that references the property by name); each repeat gets a fresh, unique name.
        /// </summary>
        internal static IReadOnlyList<string> AssignUniqueColumnPropertyNames(
            string entityName,
            IReadOnlyDictionary<string, string>? columnPropertyNames,
            IReadOnlyList<string> orderedColumnNames)
        {
            var usedPropertyNames = ScaffoldColumnPropertyNameBuilder.CreateReservedMemberNameSet();
            usedPropertyNames.Add(entityName);

            var names = new List<string>(orderedColumnNames.Count);
            foreach (var columnName in orderedColumnNames)
            {
                var preferred = ResolveColumnPropertyName(columnPropertyNames, columnName);
                names.Add(usedPropertyNames.Add(preferred)
                    ? preferred
                    : ScaffoldNameHelper.MakeUnique(StripTrailingDigits(preferred), usedPropertyNames));
            }

            return names;
        }

        private static ScaffoldEntityColumnInfo BuildEntityColumn(
            ScaffoldEntitySourceInfo entity,
            DataRow row,
            string propName)
        {
            var colName = row["ColumnName"]!.ToString()!;
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

        private static string ResolveColumnPropertyName(
            IReadOnlyDictionary<string, string>? columnPropertyNames,
            string columnName)
            => columnPropertyNames is not null && columnPropertyNames.TryGetValue(columnName, out var mappedProperty)
                ? mappedProperty
                : ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(columnName));

        private static string StripTrailingDigits(string name)
        {
            var end = name.Length;
            while (end > 0 && char.IsDigit(name[end - 1]))
                end--;
            return end > 0 && end < name.Length ? name[..end] : name;
        }

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
