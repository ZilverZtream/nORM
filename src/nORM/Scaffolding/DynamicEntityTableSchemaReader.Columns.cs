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
                // KeyInfo pads a view's schema with hidden base-table key columns that are not part of
                // the view's projection; materializing them would fail with "invalid column name".
                if (ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(row))
                    continue;

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
    }
}
