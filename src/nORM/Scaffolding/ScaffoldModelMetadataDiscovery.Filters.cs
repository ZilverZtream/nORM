#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelMetadataDiscovery
    {
        private static IReadOnlyList<ScaffoldIndex> FilterIndexesToScaffoldedTables(
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> scaffoldedTableKeys)
            => indexes
                .Where(index => scaffoldedTableKeys.Contains(index.TableKey))
                .ToArray();

        private static IReadOnlyList<ScaffoldForeignKey> FilterForeignKeysToScaffoldedTables(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlySet<string> scaffoldedTableKeys)
            => foreignKeys
                .Where(fk => scaffoldedTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))
                             && scaffoldedTableKeys.Contains(TableKey(fk.PrincipalSchema, fk.PrincipalTable)))
                .ToArray();

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
