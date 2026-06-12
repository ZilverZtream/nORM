#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityComputedColumnReader
    {
        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> QueryComputedColumnMap(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
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
                result[columnName] = new DynamicEntityTypeGenerator.ScaffoldComputedColumn(computedSql, stored);
            }

            return result;
        }

        public static (string Sql, bool Stored) NormalizeComputedColumnSql(string raw)
            => ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(raw);

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
                _ => Convert.ToInt32(value, CultureInfo.InvariantCulture) != 0
            };
    }
}
