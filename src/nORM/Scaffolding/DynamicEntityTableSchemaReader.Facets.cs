#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
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
    }
}
