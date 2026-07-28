#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoreTypeClrMapper
    {
        private static bool TryMapSqliteStoreType(string storeType, out Type clrType)
        {
            clrType = typeof(object);
            var declared = storeType.Trim().ToUpperInvariant();
            if (ScaffoldSqliteDdlParser.IsUnsafeProviderSpecificDeclaredType(declared))
                return false;

            if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "DECIMAL")
                || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "NUMERIC")
                // MONEY / SMALLMONEY are money types other engines emit; SQLite keeps them as NUMERIC affinity.
                || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "MONEY")
                || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "SMALLMONEY"))
                clrType = typeof(decimal);
            // Boolean has no dedicated SQLite storage class, so schemas declare it as BOOLEAN / BOOL / BIT over
            // an INTEGER 0/1. Without this the reader reports the value as text and the column scaffolds as
            // string, which then silently coerces 0/1 to "0"/"1". Nullability is decided separately downstream.
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "BOOLEAN")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "BOOL")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "BIT"))
                clrType = typeof(bool);
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
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "UUID")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "GUID")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "UNIQUEIDENTIFIER"))
                clrType = typeof(Guid);
            else if (ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "CHAR")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "CLOB")
                     || ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(declared, "TEXT"))
                clrType = typeof(string);

            return clrType != typeof(object);
        }
    }
}
