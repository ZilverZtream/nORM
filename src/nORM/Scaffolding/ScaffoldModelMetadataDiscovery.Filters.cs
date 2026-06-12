#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelMetadataDiscovery
    {
        private static IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> FilterIndexesToScaffoldedTables(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlySet<string> scaffoldedTableKeys)
            => indexes
                .Where(index => scaffoldedTableKeys.Contains(index.TableKey))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> FilterForeignKeysToScaffoldedTables(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlySet<string> scaffoldedTableKeys)
            => foreignKeys
                .Where(fk => scaffoldedTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))
                             && scaffoldedTableKeys.Contains(TableKey(fk.PrincipalSchema, fk.PrincipalTable)))
                .ToArray();

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
