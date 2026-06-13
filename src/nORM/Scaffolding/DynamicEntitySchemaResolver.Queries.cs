#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaResolver
    {
        public static IReadOnlyList<string> QuerySchemaNameList(DbConnection connection, string sql, string tableName)
        {
            var result = new List<string>();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var schemaName = Convert.ToString(reader["SchemaName"]);
                if (!string.IsNullOrWhiteSpace(schemaName))
                    result.Add(schemaName);
            }

            return result
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(schema => schema, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        public static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static void AddStringParameter(DbCommand command, string name, string? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = DbType.String;
            parameter.Value = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
            command.Parameters.Add(parameter);
        }
    }
}
