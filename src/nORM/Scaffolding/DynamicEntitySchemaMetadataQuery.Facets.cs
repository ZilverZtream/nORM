#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataQuery
    {
        public static IReadOnlyDictionary<string, ScaffoldColumnFacet> QueryColumnFacetMap(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            if (sql.Contains("@schemaName", StringComparison.Ordinal))
                AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                var maxLength = reader["MaxLength"] == DBNull.Value
                    ? (int?)null
                    : Convert.ToInt32(reader["MaxLength"], CultureInfo.InvariantCulture);
                var isUnicode = reader["IsUnicode"] == DBNull.Value
                    ? (bool?)null
                    : Convert.ToInt32(reader["IsUnicode"], CultureInfo.InvariantCulture) != 0;
                var isFixedLength = reader["IsFixedLength"] != DBNull.Value
                    && Convert.ToInt32(reader["IsFixedLength"], CultureInfo.InvariantCulture) != 0;

                result[columnName!] = new ScaffoldColumnFacet(
                    maxLength is > 0 ? maxLength : null,
                    isUnicode,
                    isFixedLength);
            }

            return result;
        }
    }
}
