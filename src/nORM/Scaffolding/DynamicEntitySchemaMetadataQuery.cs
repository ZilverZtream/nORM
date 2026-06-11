#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static class DynamicEntitySchemaMetadataQuery
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

        public static void AddStringParameter(DbCommand command, string name, string? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = DbType.String;
            parameter.Value = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
            command.Parameters.Add(parameter);
        }
    }
}
