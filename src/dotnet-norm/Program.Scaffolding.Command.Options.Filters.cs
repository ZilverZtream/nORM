using System.CommandLine;

partial class Program
{
    private static ScaffoldFilterCommandSymbols CreateScaffoldFilterCommandSymbols()
        => new(
            new Option<string?>("--schemas") { Description = "Optional comma-separated schema filter. All discovered tables and supported query artifacts in matching schemas are included." },
            new Option<string[]>("--schema") { Description = "Optional repeatable schema filter. May be specified multiple times." },
            new Option<string?>("--tables") { Description = "Optional comma-separated table filter. Entries may be table or schema.table names; literal dotted names that collide with schema-qualified names are rejected." },
            new Option<string[]>("--table", "-t") { Description = "Optional repeatable table filter for names that should not be comma-split. May be specified multiple times." });

    private static void AddScaffoldFilterCommandSymbols(Command scaffold, ScaffoldCommandSymbols symbols)
    {
        scaffold.Add(symbols.SchemasOption);
        scaffold.Add(symbols.SchemaOption);
        scaffold.Add(symbols.TablesOption);
        scaffold.Add(symbols.TableOption);
    }
}
