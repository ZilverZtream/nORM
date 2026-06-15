using System.CommandLine;

partial class Program
{
    private static ScaffoldProjectNamingCommandSymbols CreateScaffoldProjectNamingCommandSymbols()
        => new(
            new Option<string>("--output", "-o", "--output-dir") { Description = "Output directory for generated code", DefaultValueFactory = _ => "." },
            new Option<string?>("--namespace", "-n") { Description = "Namespace for generated classes. Defaults to the target project's RootNamespace/AssemblyName plus output directory when --project is supplied, otherwise Scaffolded." },
            new Option<string?>("--context", "-c") { Description = "DbContext class name or namespace-qualified name. Defaults to the database name plus Context when it can be inferred." },
            new Option<string?>("--project", "-p") { Description = "Optional target .csproj or project directory. Relative output paths are resolved under this project and its namespace is used by default." },
            new Option<string?>("--startup-project", "-s") { Description = "Optional startup .csproj or project directory for EF-style named connection lookup; nORM scaffold does not execute startup code." },
            new Option<string?>("--context-dir") { Description = "Optional project-relative/current-directory-relative directory for the generated DbContext file." },
            new Option<string?>("--context-namespace") { Description = "Optional namespace for the generated DbContext. Entity classes keep --namespace." });

    private static void AddScaffoldProjectNamingCommandSymbols(Command scaffold, ScaffoldCommandSymbols symbols)
    {
        scaffold.Add(symbols.OutputOption);
        scaffold.Add(symbols.NamespaceOption);
        scaffold.Add(symbols.ContextOption);
        scaffold.Add(symbols.ProjectOption);
        scaffold.Add(symbols.StartupProjectOption);
        scaffold.Add(symbols.ContextDirectoryOption);
        scaffold.Add(symbols.ContextNamespaceOption);
    }
}
