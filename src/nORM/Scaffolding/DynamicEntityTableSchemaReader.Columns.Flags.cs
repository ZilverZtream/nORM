#nullable enable
using System.Data;

using DynamicScaffoldComputedColumn = nORM.Scaffolding.DynamicEntityTypeGenerator.ScaffoldComputedColumn;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
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
    }
}
