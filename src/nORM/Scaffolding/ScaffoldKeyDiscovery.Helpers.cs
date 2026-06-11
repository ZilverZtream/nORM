#nullable enable
using System.Data.Common;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldKeyDiscovery
    {
        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
