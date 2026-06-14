using System.CommandLine;

partial class Program
{
    private static ScaffoldCommandSymbols CreateScaffoldCommandSymbols()
    {
        var symbols = new ScaffoldCommandSymbols
        {
            ConnectionArgument = new Argument<string?>("connection") { Arity = ArgumentArity.ZeroOrOne },
            ProviderArgument = new Argument<string?>("provider") { Arity = ArgumentArity.ZeroOrOne },
            ConnectionOption = new Option<string?>("--connection") { Description = "Database connection string. e.g. 'Server=.;Database=AppDb;Trusted_Connection=True;'" },
            ProviderOption = new Option<string?>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name." },
            OutputOption = new Option<string>("--output", "-o", "--output-dir") { Description = "Output directory for generated code", DefaultValueFactory = _ => "." },
            NamespaceOption = new Option<string?>("--namespace", "-n") { Description = "Namespace for generated classes. Defaults to the target project's RootNamespace/AssemblyName plus output directory when --project is supplied, otherwise Scaffolded." },
            ContextOption = new Option<string?>("--context", "-c") { Description = "DbContext class name or namespace-qualified name. Defaults to the database name plus Context when it can be inferred." },
            ProjectOption = new Option<string?>("--project", "-p") { Description = "Optional target .csproj or project directory. Relative output paths are resolved under this project and its namespace is used by default." },
            StartupProjectOption = new Option<string?>("--startup-project", "-s") { Description = "Optional startup .csproj or project directory for EF-style named connection lookup; nORM scaffold does not execute startup code." },
            FrameworkOption = new Option<string?>("--framework", "--target-framework") { Description = "Accepted for EF Core scaffold compatibility and nORM design-time CLI consistency; nORM scaffold does not build the target project." },
            ConfigurationOption = new Option<string?>("--configuration") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not build the target project." },
            RuntimeOption = new Option<string?>("--runtime") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not restore runtime-specific assets." },
            MsbuildProjectExtensionsPathOption = new Option<string?>("--msbuildprojectextensionspath") { Description = "Accepted for legacy EF-style scaffold compatibility; nORM scaffold does not invoke MSBuild." },
            NoBuildOption = new Option<bool>("--no-build") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold never builds the target project." },
            JsonOption = new Option<bool>("--json") { Description = "Emit a machine-readable scaffold result summary." },
            VerboseOption = new Option<bool>("--verbose", "-v") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is already explicit." },
            NoColorOption = new Option<bool>("--no-color") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is plain text." },
            PrefixOutputOption = new Option<bool>("--prefix-output") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is not severity-prefixed." },
            ContextDirectoryOption = new Option<string?>("--context-dir") { Description = "Optional project-relative/current-directory-relative directory for the generated DbContext file." },
            ContextNamespaceOption = new Option<string?>("--context-namespace") { Description = "Optional namespace for the generated DbContext. Entity classes keep --namespace." },
            SchemasOption = new Option<string?>("--schemas") { Description = "Optional comma-separated schema filter. All discovered tables and supported query artifacts in matching schemas are included." },
            SchemaOption = new Option<string[]>("--schema") { Description = "Optional repeatable schema filter. May be specified multiple times." },
            TablesOption = new Option<string?>("--tables") { Description = "Optional comma-separated table filter. Entries may be table or schema.table names; literal dotted names that collide with schema-qualified names are rejected." },
            TableOption = new Option<string[]>("--table", "-t") { Description = "Optional repeatable table filter for names that should not be comma-split. May be specified multiple times." },
            NoPluralizeOption = new Option<bool>("--no-pluralize") { Description = "Do not singularize entity class names or pluralize generated IQueryable<T> context property names." },
            UseDatabaseNamesOption = new Option<bool>("--use-database-names") { Description = "Preserve legal table, view, sequence, routine, column, and routine result-column names as generated CLR names instead of applying PascalCase naming." },
            NoOnConfiguringOption = new Option<bool>("--no-onconfiguring") { Description = "Accepted for EF Core scaffold compatibility; nORM generated contexts never emit OnConfiguring." },
            DataAnnotationsOption = new Option<bool>("--data-annotations", "-d") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffolding already emits data annotations where supported." },
            ForceOption = new Option<bool>("--force", "-f") { Description = "Overwrite existing generated files. By default, scaffold output conflicts are refused." },
            NoOverwriteOption = new Option<bool>("--no-overwrite") { Description = "Explicitly refuse to overwrite existing generated files." },
            DryRunOption = new Option<bool>("--dry-run") { Description = "Validate scaffold output without creating, deleting, or overwriting files." },
            FailOnWarningsOption = new Option<bool>("--fail-on-warnings") { Description = "Fail scaffolding when unsupported schema features are reported in nORM.ScaffoldWarnings.md/json." },
            EmitRoutineStubsOption = new Option<bool>("--emit-routine-stubs") { Description = "Generate provider-bound context wrapper methods for discovered routines/stored procedures. Routine bodies remain provider-owned." },
            EmitSequenceStubsOption = new Option<bool>("--emit-sequence-stubs") { Description = "Generate provider-bound context wrapper methods for discovered SQL Server/PostgreSQL standalone sequences." },
            EmitViewEntitiesOption = new Option<bool>("--emit-view-entities") { Description = "Compatibility alias for --emit-query-artifacts; generates optional query-only entity classes such as SQLite virtual tables and local table/view synonyms. Ordinary views/materialized views are generated by default." },
            EmitQueryArtifactsOption = new Option<bool>("--emit-query-artifacts") { Description = "Generate bounded read-oriented query artifacts for optional provider objects such as SQLite virtual tables and local table/view synonyms." }
        };

        symbols.SchemaOption.AllowMultipleArgumentsPerToken = true;
        symbols.TableOption.AllowMultipleArgumentsPerToken = true;
        return symbols;
    }

    private static void AddScaffoldCommandSymbols(Command scaffold, ScaffoldCommandSymbols symbols)
    {
        scaffold.Add(symbols.ConnectionArgument);
        scaffold.Add(symbols.ProviderArgument);
        scaffold.Add(symbols.ConnectionOption);
        scaffold.Add(symbols.ProviderOption);
        scaffold.Add(symbols.OutputOption);
        scaffold.Add(symbols.NamespaceOption);
        scaffold.Add(symbols.ContextOption);
        scaffold.Add(symbols.ProjectOption);
        scaffold.Add(symbols.StartupProjectOption);
        scaffold.Add(symbols.FrameworkOption);
        scaffold.Add(symbols.ConfigurationOption);
        scaffold.Add(symbols.RuntimeOption);
        scaffold.Add(symbols.MsbuildProjectExtensionsPathOption);
        scaffold.Add(symbols.NoBuildOption);
        scaffold.Add(symbols.JsonOption);
        scaffold.Add(symbols.VerboseOption);
        scaffold.Add(symbols.NoColorOption);
        scaffold.Add(symbols.PrefixOutputOption);
        scaffold.Add(symbols.ContextDirectoryOption);
        scaffold.Add(symbols.ContextNamespaceOption);
        scaffold.Add(symbols.SchemasOption);
        scaffold.Add(symbols.SchemaOption);
        scaffold.Add(symbols.TablesOption);
        scaffold.Add(symbols.TableOption);
        scaffold.Add(symbols.NoPluralizeOption);
        scaffold.Add(symbols.UseDatabaseNamesOption);
        scaffold.Add(symbols.NoOnConfiguringOption);
        scaffold.Add(symbols.DataAnnotationsOption);
        scaffold.Add(symbols.ForceOption);
        scaffold.Add(symbols.NoOverwriteOption);
        scaffold.Add(symbols.DryRunOption);
        scaffold.Add(symbols.FailOnWarningsOption);
        scaffold.Add(symbols.EmitRoutineStubsOption);
        scaffold.Add(symbols.EmitSequenceStubsOption);
        scaffold.Add(symbols.EmitViewEntitiesOption);
        scaffold.Add(symbols.EmitQueryArtifactsOption);
    }

}
