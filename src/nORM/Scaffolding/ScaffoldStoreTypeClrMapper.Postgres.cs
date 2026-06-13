#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoreTypeClrMapper
    {
        private static bool TryMapPostgresStoreType(string normalized, out Type clrType)
            => TryMapExact(
                normalized,
                out clrType,
                ("date", typeof(DateOnly)),
                ("time", typeof(TimeOnly)),
                ("time without time zone", typeof(TimeOnly)),
                ("time with time zone", typeof(DateTimeOffset)),
                ("timetz", typeof(DateTimeOffset)),
                ("timestamp without time zone", typeof(DateTime)),
                ("timestamp with time zone", typeof(DateTimeOffset)),
                ("interval", typeof(TimeSpan)),
                ("uuid", typeof(Guid)));
    }
}
