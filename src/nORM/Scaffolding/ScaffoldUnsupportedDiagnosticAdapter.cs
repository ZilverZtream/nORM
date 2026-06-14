#nullable enable
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedDiagnosticAdapter
    {
        public static string NormalizeReferentialAction(string? action)
            => ScaffoldReferentialAction.Normalize(action);

        public static bool TryParseReferentialAction(string? action, out ReferentialAction referentialAction)
            => ScaffoldReferentialAction.TryParse(action, out referentialAction);

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
