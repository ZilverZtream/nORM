#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool QueryHasUnmodeledDefault(DbConnection connection, string sql, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (HasUnmodeledDefaultSql(Convert.ToString(reader["DefaultSql"])))
                    return true;
            }

            return false;
        }
    }
}
