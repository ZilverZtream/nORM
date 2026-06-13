#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoreTypeClrMapper
    {
        private static bool TryMapMySqlStoreType(string normalized, out Type clrType)
            => TryMapExact(
                normalized,
                out clrType,
                ("date", typeof(DateOnly)),
                ("datetime", typeof(DateTime)),
                ("timestamp", typeof(DateTime)));
    }
}
