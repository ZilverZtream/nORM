#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSchemaDiscoveryAdapter
    {
        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
