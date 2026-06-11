#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
