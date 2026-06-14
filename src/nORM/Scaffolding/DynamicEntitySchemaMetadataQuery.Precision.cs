#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataQuery
    {
        public static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> QueryDecimalPrecisionMap(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, ScaffoldDecimalPrecisionInfo>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName)
                    || reader["DecimalPrecision"] == DBNull.Value)
                {
                    continue;
                }

                var precision = Convert.ToInt32(reader["DecimalPrecision"], CultureInfo.InvariantCulture);
                var scale = reader["DecimalScale"] == DBNull.Value
                    ? (int?)null
                    : Convert.ToInt32(reader["DecimalScale"], CultureInfo.InvariantCulture);
                if (precision > 0 && (!scale.HasValue || (scale.Value >= 0 && scale.Value <= precision)))
                    result[columnName] = new ScaffoldDecimalPrecisionInfo(precision, scale);
            }

            return result;
        }
    }
}
