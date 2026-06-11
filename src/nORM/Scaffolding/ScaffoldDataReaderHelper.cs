#nullable enable
using System;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static class ScaffoldDataReaderHelper
    {
        public static bool HasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }
    }
}
