#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteIndexDiscovery
    {
        private readonly record struct SqliteIndexCatalogRow(
            string Name,
            bool IsUnique,
            string Origin,
            bool IsPartial);

        private readonly record struct SqliteIndexColumnRow(
            int Ordinal,
            string Name,
            bool IsDescending);
    }
}
