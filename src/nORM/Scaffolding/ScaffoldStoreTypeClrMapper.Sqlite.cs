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
    }
}
