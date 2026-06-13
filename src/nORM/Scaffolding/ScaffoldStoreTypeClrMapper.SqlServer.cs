#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoreTypeClrMapper
    {
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
    }
}
