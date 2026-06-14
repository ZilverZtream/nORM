#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyShape
    {
        public static bool HasOnlyScaffoldableReferentialActions(IEnumerable<ScaffoldForeignKeyInfo> foreignKeys)
            => foreignKeys.All(static fk =>
                IsScaffoldableReferentialAction(fk.OnDelete)
                && IsScaffoldableReferentialAction(fk.OnUpdate));

        public static string NormalizeReferentialAction(string? action)
            => ScaffoldReferentialAction.Normalize(action);

        public static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static bool IsScaffoldableReferentialAction(string? action)
            => ScaffoldReferentialAction.IsScaffoldable(action);
    }
}
