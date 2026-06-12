#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityComputedColumnReader
    {
        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetComputedColumns(
            DbConnection connection,
            string? schemaName,
            string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteComputedColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return GetSqlServerComputedColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return GetPostgresComputedColumns(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return GetMySqlComputedColumns(connection, schemaName, tableName);

            return EmptyComputedColumns();
        }

        private static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> EmptyComputedColumns()
            => new Dictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn>(0, StringComparer.OrdinalIgnoreCase);
    }
}
