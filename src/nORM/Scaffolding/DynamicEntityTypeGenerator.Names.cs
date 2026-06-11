using System.Data.Common;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        private static string EscapeQualified(DbConnection connection, string? schema, string table)
            => DynamicEntityConnectionKind.EscapeQualified(connection, schema, table);

        /// <summary>
        /// Wraps a raw SQL identifier in the appropriate quoting characters for the provider.
        /// Escapes embedded quoting characters so legitimate identifiers are preserved
        /// without allowing crafted table or schema names to break out of the quote.
        /// </summary>
        private static string EscapeIdentifier(DbConnection connection, string identifier)
            => DynamicEntityConnectionKind.EscapeIdentifier(connection, identifier);

        private static string ToPascalCase(string name)
            => ScaffoldNameHelper.ToPascalCase(name);

        private static string EscapeCSharpIdentifier(string identifier)
            => ScaffoldNameHelper.EscapeCSharpIdentifier(identifier);
    }
}
