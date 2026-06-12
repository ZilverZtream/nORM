#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetStringBinaryFacets(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return GetSqlServerStringBinaryFacets(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return GetPostgresStringBinaryFacets(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return GetMySqlStringBinaryFacets(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteDeclaredStringBinaryFacets(connection, schemaName, tableName);

            return EmptyFacetMap();
        }

        public static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetDecimalPrecisions(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return GetSqlServerDecimalPrecisions(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return GetPostgresDecimalPrecisions(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsMySql(connection))
                return GetMySqlDecimalPrecisions(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteDeclaredDecimalPrecisions(connection, schemaName, tableName);

            return EmptyDecimalPrecisionMap();
        }

        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> EmptyFacetMap()
            => new Dictionary<string, ScaffoldColumnFacet>(0, StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> EmptyDecimalPrecisionMap()
            => new Dictionary<string, ScaffoldDecimalPrecisionInfo>(0, StringComparer.OrdinalIgnoreCase);
    }
}
