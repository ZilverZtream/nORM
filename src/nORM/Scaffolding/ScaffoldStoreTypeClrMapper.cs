#nullable enable
using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldStoreTypeClrMapper
    {
        public static bool TryMapStoreType(DatabaseProvider provider, string? storeType, out Type clrType)
        {
            clrType = typeof(object);
            if (string.IsNullOrWhiteSpace(storeType))
                return false;

            var normalized = Normalize(storeType);
            if (ScaffoldProviderKind.IsSqlServer(provider))
                return TryMapSqlServerStoreType(normalized, out clrType);
            if (ScaffoldProviderKind.IsPostgres(provider))
                return TryMapPostgresStoreType(normalized, out clrType);
            if (ScaffoldProviderKind.IsMySql(provider))
                return TryMapMySqlStoreType(normalized, out clrType);
            if (ScaffoldProviderKind.IsSqlite(provider))
                return TryMapSqliteStoreType(storeType, out clrType);

            return false;
        }

        public static bool TryMapStoreType(DbConnection connection, string? storeType, out Type clrType)
        {
            clrType = typeof(object);
            if (string.IsNullOrWhiteSpace(storeType))
                return false;

            var normalized = Normalize(storeType);
            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return TryMapSqlServerStoreType(normalized, out clrType);
            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return TryMapPostgresStoreType(normalized, out clrType);
            if (DynamicEntityConnectionKind.IsMySql(connection))
                return TryMapMySqlStoreType(normalized, out clrType);
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return TryMapSqliteStoreType(storeType, out clrType);

            return false;
        }

        private static bool TryMapSqlServerStoreType(string normalized, out Type clrType)
            => TryMapExact(
                normalized,
                out clrType,
                ("date", typeof(DateOnly)),
                ("time", typeof(TimeOnly)),
                ("datetimeoffset", typeof(DateTimeOffset)),
                ("datetime", typeof(DateTime)),
                ("datetime2", typeof(DateTime)),
                ("smalldatetime", typeof(DateTime)),
                ("uniqueidentifier", typeof(Guid)));

        private static bool TryMapPostgresStoreType(string normalized, out Type clrType)
            => TryMapExact(
                normalized,
                out clrType,
                ("date", typeof(DateOnly)),
                ("time", typeof(TimeOnly)),
                ("time without time zone", typeof(TimeOnly)),
                ("timestamp without time zone", typeof(DateTime)),
                ("timestamp with time zone", typeof(DateTimeOffset)),
                ("interval", typeof(TimeSpan)),
                ("uuid", typeof(Guid)));

        private static bool TryMapMySqlStoreType(string normalized, out Type clrType)
            => TryMapExact(
                normalized,
                out clrType,
                ("date", typeof(DateOnly)),
                ("datetime", typeof(DateTime)),
                ("timestamp", typeof(DateTime)));

        private static bool TryMapSqliteStoreType(string storeType, out Type clrType)
        {
            clrType = typeof(object);
            var declared = storeType.Trim().ToUpperInvariant();
            if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "DECIMAL")
                || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "NUMERIC"))
                clrType = typeof(decimal);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "BINARY")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "VARBINARY")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "BLOB"))
                clrType = typeof(byte[]);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "DATETIMEOFFSET"))
                clrType = typeof(DateTimeOffset);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "DATETIME")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "TIMESTAMP"))
                clrType = typeof(DateTime);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "DATE"))
                clrType = typeof(DateOnly);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "TIME"))
                clrType = typeof(TimeOnly);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "UUID"))
                clrType = typeof(Guid);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "CHAR")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "CLOB")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "TEXT"))
                clrType = typeof(string);

            return clrType != typeof(object);
        }

        private static bool TryMapExact(string normalized, out Type clrType, params (string StoreType, Type ClrType)[] mappings)
        {
            var baseType = StripFacets(normalized);
            foreach (var (storeType, mappedType) in mappings)
            {
                if (string.Equals(baseType, storeType, StringComparison.Ordinal))
                {
                    clrType = mappedType;
                    return true;
                }
            }

            clrType = typeof(object);
            return false;
        }

        private static string Normalize(string storeType)
            => StripFacets(storeType.Trim().ToLowerInvariant())
                .Replace('_', ' ')
                .Replace("  ", " ", StringComparison.Ordinal)
                .Trim();

        private static string StripFacets(string storeType)
        {
            var paren = storeType.IndexOf('(');
            return paren >= 0 ? storeType[..paren].Trim() : storeType.Trim();
        }
    }
}
