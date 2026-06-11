using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.IO;
using System.Threading;
using nORM.Core;
using nORM.Scaffolding;
using nORM.Security;

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
        var scaffoldFrameworkOpt = new Option<string?>("--framework") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not build the target project." };
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
        var noPluralizeOpt = new Option<bool>("--no-pluralize") { Description = "Do not pluralize generated IQueryable<T> context property names. Entity class names are unchanged." };
        var useDatabaseNamesOpt = new Option<bool>("--use-database-names") { Description = "Preserve legal table, view, sequence, routine, column, and routine result-column names as generated CLR names instead of applying PascalCase naming." };
        var noOnConfiguringOpt = new Option<bool>("--no-onconfiguring") { Description = "Accepted for EF Core scaffold compatibility; nORM generated contexts never emit OnConfiguring." };
        var dataAnnotationsOpt = new Option<bool>("--data-annotations", "-d") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffolding already emits data annotations where supported." };
        var forceOpt = new Option<bool>("--force", "-f") { Description = "Overwrite existing generated files. By default, scaffold output conflicts are refused." };
        var noOverwriteOpt = new Option<bool>("--no-overwrite") { Description = "Explicitly refuse to overwrite existing generated files." };
        var scaffoldDryRunOpt = new Option<bool>("--dry-run") { Description = "Validate scaffold output without creating, deleting, or overwriting files." };
        var failOnWarningsOpt = new Option<bool>("--fail-on-warnings") { Description = "Fail scaffolding when unsupported schema features are reported in nORM.ScaffoldWarnings.md/json." };
        var emitRoutineStubsOpt = new Option<bool>("--emit-routine-stubs") { Description = "Generate provider-bound context wrapper methods for discovered routines/stored procedures. Routine bodies remain provider-owned." };
        var emitSequenceStubsOpt = new Option<bool>("--emit-sequence-stubs") { Description = "Generate provider-bound context wrapper methods for discovered SQL Server/PostgreSQL standalone sequences." };
        var emitViewEntitiesOpt = new Option<bool>("--emit-view-entities") { Description = "Generate optional query-only entity classes such as SQLite virtual tables and local table/view synonyms. Ordinary views/materialized views are generated by default." };
        var emitQueryArtifactsOpt = new Option<bool>("--emit-query-artifacts") { Description = "Alias for --emit-view-entities; generates bounded read-oriented query artifacts for optional provider objects." };
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
        scaffold.SetAction(async (ParseResult result, CancellationToken cancellationToken) =>
        {
            var jsonOutput = result.GetValue(scaffoldJsonOpt);
            var outputForJson = result.GetValue(outputOpt) ?? ".";
            var dryRunForJson = result.GetValue(scaffoldDryRunOpt);

            try
            {
                ValidateScaffoldUnmatchedTokens(result);
                var connectionOption = GetOptionalNonBlankScaffoldOption(result, connOpt, "--connection");
                var providerOption = GetOptionalNonBlankScaffoldOption(result, providerOpt, "--provider");
                var connectionPosition = NullIfWhiteSpace(result.GetValue(connectionArg));
                var providerPosition = NullIfWhiteSpace(result.GetValue(providerArg));
                if (connectionOption is not null && providerOption is null && providerPosition is null && connectionPosition is not null)
                {
                    providerPosition = connectionPosition;
                    connectionPosition = null;
                }

                var connectionReference = FirstNonBlank(connectionOption, connectionPosition)
                    ?? throw new NormConfigurationException("Scaffold requires a database connection string. Pass --connection <CONNECTION> or the EF-style positional <CONNECTION> argument.");
                var prov = FirstNonBlank(providerOption, providerPosition)
                    ?? throw new NormConfigurationException("Scaffold requires a database provider. Pass --provider <PROVIDER> or the EF-style positional <PROVIDER> argument.");
                prov = NormalizeProviderName(prov);
                var efToolConfig = LoadEfToolConfig();
                _ = FirstNonBlank(result.GetValue(scaffoldFrameworkOpt), efToolConfig?.Framework);
                _ = FirstNonBlank(result.GetValue(scaffoldConfigurationOpt), efToolConfig?.Configuration);
                _ = FirstNonBlank(result.GetValue(scaffoldRuntimeOpt), efToolConfig?.Runtime);
                _ = result.GetValue(scaffoldVerboseOpt) || efToolConfig?.Verbose == true;
                _ = result.GetValue(scaffoldNoColorOpt) || efToolConfig?.NoColor == true;
                _ = result.GetValue(scaffoldPrefixOutputOpt) || efToolConfig?.PrefixOutput == true;
                var projectInfo = ResolveScaffoldProject(FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, scaffoldProjectOpt, "--project"), efToolConfig?.Project), inferCurrentDirectory: true);
                var startupProjectInfo = IsNamedConnectionReference(connectionReference)
                    ? ResolveScaffoldProject(FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, scaffoldStartupProjectOpt, "--startup-project"), efToolConfig?.StartupProject))
                    : null;
                var scaffoldEnvironment = GetScaffoldPassThroughEnvironment();
                var connectionString = ResolveScaffoldConnectionString(connectionReference, projectInfo, startupProjectInfo, scaffoldEnvironment);
                var validated = ConnectionStringValidator.Validate(connectionString, prov);
                var output = ResolveScaffoldOutputPath(GetRequiredNonBlankScaffoldOption(result, outputOpt, "--output"), projectInfo);
                outputForJson = output;
                var explicitNamespace = GetOptionalNonBlankScaffoldOption(result, nsOpt, "--namespace");
                var ns = ValidateScaffoldNamespaceName(ResolveScaffoldNamespace(explicitNamespace, projectInfo, output), "--namespace");
                var contextDirectory = GetOptionalNonBlankScaffoldOption(result, contextDirOpt, "--context-dir");
                var contextOutputDirectory = ResolveScaffoldContextOutputDirectory(contextDirectory, projectInfo);
                var explicitContextName = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, ctxOpt, "--context"), efToolConfig?.Context);
                var contextName = explicitContextName
                    ?? InferScaffoldContextName(validated.ConnectionString, connectionReference, prov);
                var explicitContextNamespace = GetOptionalNonBlankScaffoldOption(result, contextNamespaceOpt, "--context-namespace");
                var (ctx, contextNamespace) = ResolveScaffoldContextNameAndNamespace(contextName, explicitContextNamespace, ns, contextDirectory, explicitNamespace, projectInfo);
                if (explicitContextName is not null)
                    ctx = ValidateScaffoldContextClassName(ctx);
                if (contextNamespace is not null)
                    contextNamespace = ValidateScaffoldNamespaceName(contextNamespace, explicitContextNamespace is null ? "context namespace" : "--context-namespace");
                var forceOverwrite = result.GetValue(forceOpt);
                var noOverwrite = result.GetValue(noOverwriteOpt);
                if (forceOverwrite && noOverwrite)
                    throw new NormConfigurationException("Use either --force or --no-overwrite for scaffold output conflicts, not both.");
                using var connection = CreateConnection(prov, validated.ConnectionString);
                await connection.OpenAsync();
                var provider = CreateProvider(prov);
                var options = new ScaffoldOptions
                {
                    Schemas = ParseSchemaFilters(result.GetValue(schemasOpt), result.GetValue(schemaOpt)),
                    Tables = ParseTableFilters(result.GetValue(tablesOpt), result.GetValue(tableOpt)),
                    PluralizeQueryProperties = !result.GetValue(noPluralizeOpt),
                    UseDatabaseNames = result.GetValue(useDatabaseNamesOpt),
                    UseNullableReferenceTypes = projectInfo?.UseNullableReferenceTypes ?? true,
                    ContextOutputDirectory = contextOutputDirectory,
                    ContextNamespace = contextNamespace,
                    OverwriteFiles = forceOverwrite,
                    DryRun = result.GetValue(scaffoldDryRunOpt),
                    FailOnWarnings = result.GetValue(failOnWarningsOpt),
                    EmitRoutineStubs = result.GetValue(emitRoutineStubsOpt),
                    EmitSequenceStubs = result.GetValue(emitSequenceStubsOpt),
                    EmitViewEntities = result.GetValue(emitViewEntitiesOpt),
                    EmitQueryArtifacts = result.GetValue(emitQueryArtifactsOpt)
                };
                string? dryRunTempOutput = null;
                var scaffoldOutput = output;
                var scaffoldOptions = options;
                if (options.DryRun)
                {
                    dryRunTempOutput = Path.Combine(Path.GetTempPath(), "norm_scaffold_dryrun_" + Guid.NewGuid().ToString("N"));
                    scaffoldOutput = dryRunTempOutput;
                    scaffoldOptions = new ScaffoldOptions
                    {
                        Schemas = options.Schemas,
                        Tables = options.Tables,
                        PluralizeQueryProperties = options.PluralizeQueryProperties,
                        UseDatabaseNames = options.UseDatabaseNames,
                        UseNullableReferenceTypes = options.UseNullableReferenceTypes,
                        ContextDirectory = options.ContextDirectory,
                        ContextOutputDirectory = options.ContextOutputDirectory is null
                            ? null
                            : Path.Combine(dryRunTempOutput, "__context"),
                        ContextNamespace = options.ContextNamespace,
                        OverwriteFiles = true,
                        DryRun = false,
                        FailOnWarnings = options.FailOnWarnings,
                        EmitRoutineStubs = options.EmitRoutineStubs,
                        EmitSequenceStubs = options.EmitSequenceStubs,
                        EmitViewEntities = options.EmitViewEntities,
                        EmitQueryArtifacts = options.EmitQueryArtifacts
                    };
                }

                try
                {
                    try
                    {
                        await DatabaseScaffolder.ScaffoldAsync(connection, provider, scaffoldOutput, ns, ctx, scaffoldOptions);
                    }
                    catch (NormConfigurationException ex) when (IsScaffoldWarningsFailure(ex, scaffoldOutput))
                    {
                        if (jsonOutput)
                            WriteScaffoldResultJson("failed", output, options.DryRun, reportsWritten: !options.DryRun, ReadScaffoldWarningSummary(scaffoldOutput), ex);
                        else
                            PrintScaffoldWarningSummary(scaffoldOutput);
                        return jsonOutput ? 1 : Fail(ex);
                    }

                    var warningSummary = ReadScaffoldWarningSummary(scaffoldOutput);
                    if (jsonOutput)
                    {
                        WriteScaffoldResultJson("succeeded", output, options.DryRun, reportsWritten: !options.DryRun && warningSummary.TotalWarnings > 0, warningSummary);
                    }
                    else if (options.DryRun)
                    {
                        if (warningSummary.TotalWarnings > 0 || warningSummary.Error is not null)
                            PrintScaffoldWarningSummary(scaffoldOutput);
                        Console.WriteLine($"Scaffolding dry run completed. No files were written to {output}.");
                    }
                    else
                    {
                        Console.WriteLine($"Scaffolding completed. Files written to {output}.");
                    }

                    if (!jsonOutput && !options.DryRun && warningSummary.TotalWarnings > 0)
                    {
                        Console.WriteLine("Scaffolding warnings were written to nORM.ScaffoldWarnings.md and nORM.ScaffoldWarnings.json.");
                        PrintScaffoldWarningSummary(output);
                    }
                }
                finally
                {
                    if (dryRunTempOutput is not null)
                        TryDeleteDirectory(dryRunTempOutput);
                }

                return 0;
            }
            catch (Exception ex)
            {
                if (jsonOutput)
                {
                    WriteScaffoldResultJson("failed", outputForJson, dryRunForJson, reportsWritten: false, ScaffoldWarningSummary.Empty, ex);
                    return 1;
                }

                return Fail(ex);
            }
        });
        return scaffold;
    }
}
