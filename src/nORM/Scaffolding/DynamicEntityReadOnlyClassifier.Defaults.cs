#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        public static bool HasUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return HasSqliteUnmodeledDefaults(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return HasSqlServerUnmodeledDefaults(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return HasPostgresUnmodeledDefaults(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return HasMySqlUnmodeledDefaults(connection, schemaName, tableName);

            return false;
        }

        public static bool TryNormalizeDynamicDefaultSql(string? raw, out string defaultValueSql)
            => ScaffoldSqlMetadataParser.TryNormalizeScaffoldDefaultSql(raw, out defaultValueSql);

        private static bool HasUnmodeledDefaultSql(string? raw)
            => !string.IsNullOrWhiteSpace(raw)
               && !TryNormalizeDynamicDefaultSql(raw, out _);
    }
}
