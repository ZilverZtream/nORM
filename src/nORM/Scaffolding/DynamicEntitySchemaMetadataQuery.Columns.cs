#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataQuery
    {
        public static IReadOnlySet<string> QueryColumnNameSet(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (!string.IsNullOrWhiteSpace(columnName))
                    result.Add(columnName);
            }

            return result;
        }

        public static IReadOnlyDictionary<string, int> QueryColumnOrdinalMap(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
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

                var ordinal = Convert.ToInt32(reader["Ordinal"], CultureInfo.InvariantCulture);
                if (ordinal > 0)
                    result[columnName] = ordinal;
            }

            return result;
        }
    }
}
