using System;
using System.CommandLine;

partial class Program
{
    static Command CreateScaffoldCommand()
    {
        // scaffold command
        var scaffold = new Command("scaffold", "Scaffold a bounded v1 nORM model from base tables. Provider-owned schema features are written to nORM.ScaffoldWarnings.md/json for review.\nExamples:\n  norm scaffold --connection \"Server=.;Database=AppDb;Trusted_Connection=True;\" --provider sqlserver --output Models\n  norm scaffold \"Data Source=app.db\" sqlite --output-dir Models");
        scaffold.TreatUnmatchedTokensAsErrors = false;
        var connectionArg = new Argument<string?>("connection") { Arity = ArgumentArity.ZeroOrOne };
        var providerArg = new Argument<string?>("provider") { Arity = ArgumentArity.ZeroOrOne };
        var connOpt = new Option<string?>("--connection") { Description = "Database connection string. e.g. 'Server=.;Database=AppDb;Trusted_Connection=True;'" };
        var providerOpt = new Option<string?>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name." };
        var outputOpt = new Option<string>("--output", "-o", "--output-dir") { Description = "Output directory for generated code", DefaultValueFactory = _ => "." };
        var nsOpt = new Option<string?>("--namespace", "-n") { Description = "Namespace for generated classes. Defaults to the target project's RootNamespace/AssemblyName plus output directory when --project is supplied, otherwise Scaffolded." };
        var ctxOpt = new Option<string?>("--context", "-c") { Description = "DbContext class name or namespace-qualified name. Defaults to the database name plus Context when it can be inferred." };
        var scaffoldProjectOpt = new Option<string?>("--project", "-p") { Description = "Optional target .csproj or project directory. Relative output paths are resolved under this project and its namespace is used by default." };
        var scaffoldStartupProjectOpt = new Option<string?>("--startup-project", "-s") { Description = "Optional startup .csproj or project directory for EF-style named connection lookup; nORM scaffold does not execute startup code." };
        var scaffoldFrameworkOpt = new Option<string?>("--framework", "--target-framework") { Description = "Accepted for EF Core scaffold compatibility and nORM design-time CLI consistency; nORM scaffold does not build the target project." };
        var scaffoldConfigurationOpt = new Option<string?>("--configuration") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not build the target project." };
        var scaffoldRuntimeOpt = new Option<string?>("--runtime") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not restore runtime-specific assets." };
        var scaffoldMsbuildProjectExtensionsPathOpt = new Option<string?>("--msbuildprojectextensionspath") { Description = "Accepted for legacy EF-style scaffold compatibility; nORM scaffold does not invoke MSBuild." };
        var scaffoldNoBuildOpt = new Option<bool>("--no-build") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold never builds the target project." };
        var scaffoldJsonOpt = new Option<bool>("--json") { Description = "Emit a machine-readable scaffold result summary." };
        var scaffoldVerboseOpt = new Option<bool>("--verbose", "-v") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is already explicit." };
        var scaffoldNoColorOpt = new Option<bool>("--no-color") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is plain text." };
        var scaffoldPrefixOutputOpt = new Option<bool>("--prefix-output") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is not severity-prefixed." };
        var contextDirOpt = new Option<string?>("--context-dir") { Description = "Optional project-relative/current-directory-relative directory for the generated DbContext file." };
        var contextNamespaceOpt = new Option<string?>("--context-namespace") { Description = "Optional namespace for the generated DbContext. Entity classes keep --namespace." };
        var schemasOpt = new Option<string?>("--schemas") { Description = "Optional comma-separated schema filter. All discovered tables and supported query artifacts in matching schemas are included." };
        var schemaOpt = new Option<string[]>("--schema") { Description = "Optional repeatable schema filter. May be specified multiple times." };
        var tablesOpt = new Option<string?>("--tables") { Description = "Optional comma-separated table filter. Entries may be table or schema.table names; literal dotted names that collide with schema-qualified names are rejected." };
        var tableOpt = new Option<string[]>("--table", "-t") { Description = "Optional repeatable table filter for names that should not be comma-split. May be specified multiple times." };
        schemaOpt.AllowMultipleArgumentsPerToken = true;
        tableOpt.AllowMultipleArgumentsPerToken = true;
        var noPluralizeOpt = new Option<bool>("--no-pluralize") { Description = "Do not singularize entity class names or pluralize generated IQueryable<T> context property names." };
        var useDatabaseNamesOpt = new Option<bool>("--use-database-names") { Description = "Preserve legal table, view, sequence, routine, column, and routine result-column names as generated CLR names instead of applying PascalCase naming." };
        var noOnConfiguringOpt = new Option<bool>("--no-onconfiguring") { Description = "Accepted for EF Core scaffold compatibility; nORM generated contexts never emit OnConfiguring." };
        var dataAnnotationsOpt = new Option<bool>("--data-annotations", "-d") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffolding already emits data annotations where supported." };
        var forceOpt = new Option<bool>("--force", "-f") { Description = "Overwrite existing generated files. By default, scaffold output conflicts are refused." };
        var noOverwriteOpt = new Option<bool>("--no-overwrite") { Description = "Explicitly refuse to overwrite existing generated files." };
        var scaffoldDryRunOpt = new Option<bool>("--dry-run") { Description = "Validate scaffold output without creating, deleting, or overwriting files." };
        var failOnWarningsOpt = new Option<bool>("--fail-on-warnings") { Description = "Fail scaffolding when unsupported schema features are reported in nORM.ScaffoldWarnings.md/json." };
        var emitRoutineStubsOpt = new Option<bool>("--emit-routine-stubs") { Description = "Generate provider-bound context wrapper methods for discovered routines/stored procedures. Routine bodies remain provider-owned." };
        var emitSequenceStubsOpt = new Option<bool>("--emit-sequence-stubs") { Description = "Generate provider-bound context wrapper methods for discovered SQL Server/PostgreSQL standalone sequences." };
        var emitViewEntitiesOpt = new Option<bool>("--emit-view-entities") { Description = "Compatibility alias for --emit-query-artifacts; generates optional query-only entity classes such as SQLite virtual tables and local table/view synonyms. Ordinary views/materialized views are generated by default." };
        var emitQueryArtifactsOpt = new Option<bool>("--emit-query-artifacts") { Description = "Generate bounded read-oriented query artifacts for optional provider objects such as SQLite virtual tables and local table/view synonyms." };
        scaffold.Add(connectionArg);
        scaffold.Add(providerArg);
        scaffold.Add(connOpt);
        scaffold.Add(providerOpt);
        scaffold.Add(outputOpt);
        scaffold.Add(nsOpt);
        scaffold.Add(ctxOpt);
        scaffold.Add(scaffoldProjectOpt);
        scaffold.Add(scaffoldStartupProjectOpt);
        scaffold.Add(scaffoldFrameworkOpt);
        scaffold.Add(scaffoldConfigurationOpt);
        scaffold.Add(scaffoldRuntimeOpt);
        scaffold.Add(scaffoldMsbuildProjectExtensionsPathOpt);
        scaffold.Add(scaffoldNoBuildOpt);
        scaffold.Add(scaffoldJsonOpt);
        scaffold.Add(scaffoldVerboseOpt);
        scaffold.Add(scaffoldNoColorOpt);
        scaffold.Add(scaffoldPrefixOutputOpt);
        scaffold.Add(contextDirOpt);
        scaffold.Add(contextNamespaceOpt);
        scaffold.Add(schemasOpt);
        scaffold.Add(schemaOpt);
        scaffold.Add(tablesOpt);
        scaffold.Add(tableOpt);
        scaffold.Add(noPluralizeOpt);
        scaffold.Add(useDatabaseNamesOpt);
        scaffold.Add(noOnConfiguringOpt);
        scaffold.Add(dataAnnotationsOpt);
        scaffold.Add(forceOpt);
        scaffold.Add(noOverwriteOpt);
        scaffold.Add(scaffoldDryRunOpt);
        scaffold.Add(failOnWarningsOpt);
        scaffold.Add(emitRoutineStubsOpt);
        scaffold.Add(emitSequenceStubsOpt);
        scaffold.Add(emitViewEntitiesOpt);
        scaffold.Add(emitQueryArtifactsOpt);
        var bindings = new ScaffoldCommandBindings
        {
            ConnectionArgument = connectionArg,
            ProviderArgument = providerArg,
            ConnectionOption = connOpt,
            ProviderOption = providerOpt,
            OutputOption = outputOpt,
            NamespaceOption = nsOpt,
            ContextOption = ctxOpt,
            ProjectOption = scaffoldProjectOpt,
            StartupProjectOption = scaffoldStartupProjectOpt,
            FrameworkOption = scaffoldFrameworkOpt,
            ConfigurationOption = scaffoldConfigurationOpt,
            RuntimeOption = scaffoldRuntimeOpt,
            MsbuildProjectExtensionsPathOption = scaffoldMsbuildProjectExtensionsPathOpt,
            NoBuildOption = scaffoldNoBuildOpt,
            JsonOption = scaffoldJsonOpt,
            VerboseOption = scaffoldVerboseOpt,
            NoColorOption = scaffoldNoColorOpt,
            PrefixOutputOption = scaffoldPrefixOutputOpt,
            ContextDirectoryOption = contextDirOpt,
            ContextNamespaceOption = contextNamespaceOpt,
            SchemasOption = schemasOpt,
            SchemaOption = schemaOpt,
            TablesOption = tablesOpt,
            TableOption = tableOpt,
            NoPluralizeOption = noPluralizeOpt,
            UseDatabaseNamesOption = useDatabaseNamesOpt,
            NoOnConfiguringOption = noOnConfiguringOpt,
            DataAnnotationsOption = dataAnnotationsOpt,
            ForceOption = forceOpt,
            NoOverwriteOption = noOverwriteOpt,
            DryRunOption = scaffoldDryRunOpt,
            FailOnWarningsOption = failOnWarningsOpt,
            EmitRoutineStubsOption = emitRoutineStubsOpt,
            EmitSequenceStubsOption = emitSequenceStubsOpt,
            EmitViewEntitiesOption = emitViewEntitiesOpt,
            EmitQueryArtifactsOption = emitQueryArtifactsOpt
        };
        scaffold.SetAction((result, cancellationToken) => RunScaffoldCommandAsync(result, bindings, cancellationToken));
        return scaffold;
    }
}
