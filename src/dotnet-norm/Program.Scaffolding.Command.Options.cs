using System.CommandLine;

partial class Program
{
    private static ScaffoldCommandSymbols CreateScaffoldCommandSymbols()
    {
        var symbols = new ScaffoldCommandSymbols(
            CreateScaffoldConnectionCommandSymbols(),
            CreateScaffoldProjectNamingCommandSymbols(),
            CreateScaffoldCompatibilityCommandSymbols(),
            CreateScaffoldFilterCommandSymbols(),
            CreateScaffoldGenerationCommandSymbols());

        symbols.SchemaOption.AllowMultipleArgumentsPerToken = true;
        symbols.TableOption.AllowMultipleArgumentsPerToken = true;
        return symbols;
    }

    private static void AddScaffoldCommandSymbols(Command scaffold, ScaffoldCommandSymbols symbols)
    {
        AddScaffoldConnectionCommandSymbols(scaffold, symbols);
        AddScaffoldProjectNamingCommandSymbols(scaffold, symbols);
        AddScaffoldCompatibilityCommandSymbols(scaffold, symbols);
        AddScaffoldFilterCommandSymbols(scaffold, symbols);
        AddScaffoldGenerationCommandSymbols(scaffold, symbols);
    }

}
