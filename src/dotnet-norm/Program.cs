using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using nORM.Cli;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using nORM.Scaffolding;
using nORM.Security;

var root = new RootCommand("Command line tools for the nORM ORM framework");

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
root.Add(scaffold);

var dbcontext = new Command("dbcontext", "EF-style DbContext command aliases. Use 'norm dbcontext scaffold ...' to run bounded nORM scaffolding.");
root.Add(dbcontext);

// database update/drop commands
var database = new Command("database", "Database related commands");

var update = new Command("update", "Apply pending migrations to the database.\nExample:\n  norm database update --connection \"...\" --provider sqlserver --assembly Migrations.dll");
var migConnOpt = new Option<string>("--connection") { Description = "Database connection string", Required = true };
var migProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name.", Required = true };
var assemblyOpt = new Option<string>("--assembly") { Description = "Path to migrations assembly (e.g. ./bin/Debug/net8.0/App.Migrations.dll)", Required = true };
update.Add(migConnOpt);
update.Add(migProvOpt);
update.Add(assemblyOpt);
update.SetAction(async (ParseResult result, CancellationToken _) =>
{
    try
    {
        var prov = NormalizeProviderName(result.GetValue(migProvOpt)!);
        var validated = ConnectionStringValidator.Validate(result.GetValue(migConnOpt)!, prov);
        var asmPath = result.GetValue(assemblyOpt)!;
        if (!File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return 2;
        }
        using var connection = CreateConnection(prov, validated.ConnectionString);
        await connection.OpenAsync();
        var assembly = LoadDesignTimeAssembly(asmPath);
        IMigrationRunner runner = prov.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlServerMigrationRunner(connection, assembly),
            "sqlite" => new SqliteMigrationRunner(connection, assembly),
            "postgres" or "postgresql" => new PostgresMigrationRunner(connection, assembly),
            "mysql" => new MySqlMigrationRunner(connection, assembly),
            _ => throw new ArgumentException($"Provider '{prov}' does not support migrations.")
        };
        if (!await runner.HasPendingMigrationsAsync())
        {
            Console.WriteLine("No pending migrations found.");
            return 0;
        }
        await runner.ApplyMigrationsAsync();
        Console.WriteLine("Migrations applied successfully.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});

var drop = new Command("drop", "Drop the target database or all tables. Useful for resetting test databases.\nExample:\n  norm database drop --connection \"...\" --provider postgres");
var dropConnOpt = new Option<string>("--connection") { Description = "Database connection string", Required = true };
var dropProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name.", Required = true };
var dropYesOpt = new Option<bool>("--yes") { Description = "Confirm destructive deletion. Required unless --dry-run is used." };
var dropDryRunOpt = new Option<bool>("--dry-run") { Description = "Print the objects that would be dropped without deleting anything." };
drop.Add(dropConnOpt);
drop.Add(dropProvOpt);
drop.Add(dropYesOpt);
drop.Add(dropDryRunOpt);
drop.SetAction(async (ParseResult result, CancellationToken _) =>
{
    try
    {
        var prov = NormalizeProviderName(result.GetValue(dropProvOpt)!);
        var validated = ConnectionStringValidator.Validate(result.GetValue(dropConnOpt)!, prov);
        var yes = result.GetValue(dropYesOpt);
        var dryRun = result.GetValue(dropDryRunOpt);
        if (!yes && !dryRun)
        {
            Console.Error.WriteLine("Refusing to run destructive database drop without --yes. Use --dry-run to preview.");
            return 3;
        }

        if (prov.Equals("sqlite", StringComparison.OrdinalIgnoreCase))
        {
            var builder = new SqliteConnectionStringBuilder(validated.ConnectionString);
            var file = builder.DataSource;
            if (dryRun)
            {
                Console.WriteLine(File.Exists(file)
                    ? $"Would delete SQLite database file '{file}'."
                    : $"SQLite database file '{file}' does not exist.");
                return 0;
            }

            if (File.Exists(file))
            {
                File.Delete(file);
                Console.WriteLine($"Database '{file}' deleted.");
            }
            else
            {
                Console.WriteLine($"Database file '{file}' not found.");
            }
            return 0;
        }

        using var connection = CreateConnection(prov, validated.ConnectionString);
        await connection.OpenAsync();
        if (IsProtectedDatabaseName(prov, connection.Database))
        {
            Console.Error.WriteLine($"Refusing to drop protected {prov} database '{connection.Database}'.");
            return 4;
        }

        var provider = CreateProvider(prov);
        var schema = connection.GetSchema("Tables");
        foreach (DataRow row in schema.Rows)
        {
            var tableSchema = row["TABLE_SCHEMA"]?.ToString();
            var tableName = row["TABLE_NAME"]?.ToString();
            if (string.IsNullOrEmpty(tableName)) continue;
            // Exclude system schemas to avoid accidentally dropping provider-internal objects.
            if (IsSystemSchema(prov, tableSchema))
                continue;
            var full = string.IsNullOrEmpty(tableSchema)
                ? provider.Escape(tableName)
                : $"{provider.Escape(tableSchema!)}.{provider.Escape(tableName!)}";
            if (dryRun)
            {
                Console.WriteLine($"Would drop table {full}");
                continue;
            }

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE {full}";
            try { await cmd.ExecuteNonQueryAsync(); }
            catch (DbException ex) { Console.Error.WriteLine($"  Warning: DROP TABLE {full} failed: {ex.Message}"); }
        }
        Console.WriteLine(dryRun ? "Dry run completed." : "Database dropped successfully.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});

database.Add(update);
database.Add(drop);
root.Add(database);

var portability = new Command("portability", "Provider mobility certification and diagnostics");
var portabilityCertify = new Command("certify", "Scan an application for provider-bound code that cannot pass strict provider mobility certification.");
var portabilityScanPathOpt = new Option<string>("--scan-path") { Description = "Application source directory or file to scan.", Required = true };
var portabilityReportOpt = new Option<string?>("--report") { Description = "Path to write the JSON certification report." };
var portabilityHtmlOpt = new Option<string?>("--html") { Description = "Path to write an HTML certification dashboard." };
var portabilityProfileOpt = new Option<string>("--profile") { Description = "Certification profile label, e.g. all-four, sqlite-postgres, sqlserver-postgres.", DefaultValueFactory = _ => "all-four" };
var portabilityProvidersOpt = new Option<string?>("--providers") { Description = "Comma-separated provider target list for capability profiling, e.g. sqlite,postgres,mysql." };
var portabilitySqliteConnectionOpt = new Option<string?>("--sqlite-connection") { Description = "Optional SQLite target connection string to open and validate for actual provider mobility evidence." };
var portabilitySqlServerConnectionOpt = new Option<string?>("--sqlserver-connection") { Description = "Optional SQL Server target connection string to open and validate for actual provider mobility evidence." };
var portabilityPostgresConnectionOpt = new Option<string?>("--postgres-connection") { Description = "Optional PostgreSQL target connection string to open and validate for actual provider mobility evidence." };
var portabilityMySqlConnectionOpt = new Option<string?>("--mysql-connection") { Description = "Optional MySQL target connection string to open and validate for actual provider mobility evidence." };
var portabilitySchemaSnapshotOpt = new Option<string?>("--schema-snapshot") { Description = "Optional nORM schema.snapshot.json to inspect for provider-mobile schema metadata." };
var portabilityAssemblyOpt = new Option<string?>("--assembly") { Description = "Optional application assembly containing a design-time DbContext used to inspect provider-mobile schema metadata." };
var portabilityAttributeOnlyOpt = new Option<bool>("--attribute-only") { Description = "When --assembly is used, build the schema from attributes only instead of requiring a design-time DbContext." };
portabilityCertify.Add(portabilityScanPathOpt);
portabilityCertify.Add(portabilityReportOpt);
portabilityCertify.Add(portabilityHtmlOpt);
portabilityCertify.Add(portabilityProfileOpt);
portabilityCertify.Add(portabilityProvidersOpt);
portabilityCertify.Add(portabilitySqliteConnectionOpt);
portabilityCertify.Add(portabilitySqlServerConnectionOpt);
portabilityCertify.Add(portabilityPostgresConnectionOpt);
portabilityCertify.Add(portabilityMySqlConnectionOpt);
portabilityCertify.Add(portabilitySchemaSnapshotOpt);
portabilityCertify.Add(portabilityAssemblyOpt);
portabilityCertify.Add(portabilityAttributeOnlyOpt);
portabilityCertify.SetAction((ParseResult result) =>
{
    try
    {
        var schemaSnapshotPath = result.GetValue(portabilitySchemaSnapshotOpt);
        var assemblyPath = result.GetValue(portabilityAssemblyOpt);
        if (!string.IsNullOrWhiteSpace(schemaSnapshotPath) && !string.IsNullOrWhiteSpace(assemblyPath))
        {
            Console.Error.WriteLine("Use either --schema-snapshot or --assembly for schema inspection, not both.");
            return 2;
        }

        SchemaSnapshot? schemaSnapshot = null;
        if (!string.IsNullOrWhiteSpace(assemblyPath))
        {
            if (!File.Exists(assemblyPath))
            {
                Console.Error.WriteLine($"Assembly '{assemblyPath}' not found.");
                return 2;
            }

            var assembly = LoadDesignTimeAssembly(assemblyPath);
            schemaSnapshot = BuildMigrationSnapshot(assembly, result.GetValue(portabilityAttributeOnlyOpt), Array.Empty<string>());
        }

        var targetProviders = ParseProviderTargetList(result.GetValue(portabilityProvidersOpt));
        var targetVersions = new Dictionary<string, Version?>(StringComparer.OrdinalIgnoreCase);
        var targetFindings = new List<ProviderMobilityFinding>();
        var connectionTargets = new (string Provider, string? ConnectionString)[]
        {
            ("sqlite", result.GetValue(portabilitySqliteConnectionOpt)),
            ("sqlserver", result.GetValue(portabilitySqlServerConnectionOpt)),
            ("postgres", result.GetValue(portabilityPostgresConnectionOpt)),
            ("mysql", result.GetValue(portabilityMySqlConnectionOpt))
        };
        var explicitTargetProviders = targetProviders?.ToList();
        foreach (var target in connectionTargets)
        {
            if (string.IsNullOrWhiteSpace(target.ConnectionString))
                continue;

            explicitTargetProviders ??= new List<string>();
            if (!explicitTargetProviders.Contains(target.Provider, StringComparer.OrdinalIgnoreCase))
                explicitTargetProviders.Add(target.Provider);
            ProbeProviderTargetConnection(target.Provider, target.ConnectionString!, targetVersions, targetFindings);
        }

        var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
        {
            ScanPath = result.GetValue(portabilityScanPathOpt)!,
            JsonReportPath = result.GetValue(portabilityReportOpt),
            HtmlReportPath = result.GetValue(portabilityHtmlOpt),
            Profile = result.GetValue(portabilityProfileOpt)!,
            TargetProviders = explicitTargetProviders,
            TargetProviderVersions = targetVersions,
            TargetProviderFindings = targetFindings,
            SchemaSnapshotPath = schemaSnapshotPath,
            SchemaSnapshot = schemaSnapshot
        });

        Console.WriteLine($"Provider mobility certification {report.Status}: {report.ErrorCount} error(s), {report.WarningCount} warning(s), {report.ScannedFiles} file(s) scanned, {report.SchemaTables} schema table(s) inspected.");
        if (!string.IsNullOrWhiteSpace(result.GetValue(portabilityReportOpt)))
            Console.WriteLine($"JSON report written to {Path.GetFullPath(result.GetValue(portabilityReportOpt)!)}");
        if (!string.IsNullOrWhiteSpace(result.GetValue(portabilityHtmlOpt)))
            Console.WriteLine($"HTML report written to {Path.GetFullPath(result.GetValue(portabilityHtmlOpt)!)}");
        return report.Status == "PASS" ? 0 : 1;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});
portability.Add(portabilityCertify);
root.Add(portability);

// migrations add
var migrations = new Command("migrations", "Migration management commands");
var add = new Command("add", "Add a new migration based on model changes.\nExample:\n  norm migrations add InitialCreate --provider sqlserver --assembly App.dll");
var migNameArg = new Argument<string>("name") { Description = "Migration name" };
var addProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name.", Required = true };
// --assembly is optional when --project is supplied; the CLI resolves the output assembly from the project automatically.
var addAsmOpt = new Option<string?>("--assembly") { Description = "Path to assembly containing DbContext and entities. Optional when --project is supplied." };
var addOutOpt = new Option<string>("--output") { Description = "Output directory for migrations", DefaultValueFactory = _ => "Migrations" };
var addForceOpt = new Option<bool>("--force") { Description = "Allow destructive table/column drops when generating the migration." };
var addAttributeOnlyOpt = new Option<bool>("--attribute-only") { Description = "Generate from attribute-only entity scanning instead of a design-time DbContext." };
var addProjectOpt = new Option<string?>("--project") { Description = "Path to the .csproj file. When provided, the output assembly is resolved via 'dotnet msbuild' if --assembly is not supplied." };
// Explicit design-time loading flags for realistic application layouts (multi-project
// solutions, multi-TFM projects, external dependency graphs). All optional; when omitted, the
// design-time loader falls back to the existing project/assembly resolution path.
var addStartupProjectOpt = new Option<string?>("--startup-project") { Description = "Path to the startup .csproj used to host the design-time DbContext (for multi-project solutions)." };
var addDepsOpt = new Option<string?>("--deps") { Description = "Path to a .deps.json file describing assembly dependencies for the design-time load." };
var addRuntimeConfigOpt = new Option<string?>("--runtimeconfig") { Description = "Path to a .runtimeconfig.json file describing the runtime configuration for the design-time load." };
var addTargetFrameworkOpt = new Option<string?>("--target-framework", "--framework") { Description = "Target framework moniker (e.g., net8.0) used when resolving the output assembly from --project." };
var addConfigurationOpt = new Option<string?>("--configuration") { Description = "Build configuration used when resolving the output assembly from --project or --startup-project." };
var addRuntimeOpt = new Option<string?>("--runtime") { Description = "Runtime identifier used when resolving the output assembly from --project or --startup-project." };
var addEnvironmentOpt = new Option<string?>("--environment") { Description = "Design-time environment passed to factories and exposed as ASPNETCORE_ENVIRONMENT/DOTNET_ENVIRONMENT while building the model." };
add.Add(migNameArg);
add.Add(addProvOpt);
add.Add(addAsmOpt);
add.Add(addOutOpt);
add.Add(addForceOpt);
add.Add(addAttributeOnlyOpt);
add.Add(addProjectOpt);
add.Add(addStartupProjectOpt);
add.Add(addDepsOpt);
add.Add(addRuntimeConfigOpt);
add.Add(addTargetFrameworkOpt);
add.Add(addConfigurationOpt);
add.Add(addRuntimeOpt);
add.Add(addEnvironmentOpt);
add.SetAction((ParseResult result) =>
{
    try
    {
        var name = result.GetValue(migNameArg)!;
        var prov = NormalizeProviderName(result.GetValue(addProvOpt)!);
        var asmPath = NullIfWhiteSpace(result.GetValue(addAsmOpt));
        var output = result.GetValue(addOutOpt)!;
        var force = result.GetValue(addForceOpt);
        var attributeOnly = result.GetValue(addAttributeOnlyOpt);
        var projectPath = NullIfWhiteSpace(result.GetValue(addProjectOpt));
        var startupProjectPath = NullIfWhiteSpace(result.GetValue(addStartupProjectOpt));
        var depsPath = NullIfWhiteSpace(result.GetValue(addDepsOpt));
        var runtimeConfigPath = NullIfWhiteSpace(result.GetValue(addRuntimeConfigOpt));
        var targetFramework = NullIfWhiteSpace(result.GetValue(addTargetFrameworkOpt));
        var configuration = NullIfWhiteSpace(result.GetValue(addConfigurationOpt));
        var runtime = NullIfWhiteSpace(result.GetValue(addRuntimeOpt));
        var environment = NullIfWhiteSpace(result.GetValue(addEnvironmentOpt));

        if (!TryResolveExistingDesignTimeFile(depsPath, "Deps file", out var resolvedDepsPath) ||
            !TryResolveExistingDesignTimeFile(runtimeConfigPath, "Runtime config file", out var resolvedRuntimeConfigPath))
        {
            return 2;
        }

        // --startup-project takes precedence over --project for design-time host resolution
        // when both are supplied (matches the EF tooling convention).
        var effectiveProject = startupProjectPath ?? projectPath;

        // When --project (or --startup-project) is supplied and --assembly was not (or points to
        // a non-existent file), resolve the output assembly path via 'dotnet msbuild'.
        if (effectiveProject != null && (string.IsNullOrEmpty(asmPath) || !File.Exists(asmPath)))
        {
            if (!File.Exists(effectiveProject))
            {
                Console.Error.WriteLine($"Project file '{effectiveProject}' not found.");
                return 2;
            }
            asmPath = ResolveAssemblyFromProject(effectiveProject, targetFramework, configuration, runtime);
            if (asmPath == null)
            {
                Console.Error.WriteLine($"Could not determine output assembly from project '{effectiveProject}'. Build the project first or supply --assembly explicitly.");
                return 2;
            }
        }

        if (string.IsNullOrEmpty(asmPath) || !File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return 2;
        }
        var assembly = LoadDesignTimeAssembly(asmPath!, resolvedDepsPath, resolvedRuntimeConfigPath);

        var snapshotPath = Path.Combine(output, "schema.snapshot.json");
        SchemaSnapshot oldSnap = File.Exists(snapshotPath)
            ? JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(snapshotPath)) ?? new SchemaSnapshot()
            : new SchemaSnapshot();

        using var environmentScope = DesignTimeEnvironmentScope.Apply(environment);
        var newSnap = BuildMigrationSnapshot(assembly, attributeOnly, BuildDesignTimeArgs(environment));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        if (!diff.HasChanges)
        {
            Console.WriteLine("No changes detected.");
            return 0;
        }

        var destructiveWarnings = diff.GetDestructiveChangeWarnings();
        if (destructiveWarnings.Count > 0 && !force)
        {
            Console.Error.WriteLine("Destructive schema changes detected. No migration was written.");
            foreach (var warning in destructiveWarnings)
                Console.Error.WriteLine($"  - {warning}");
            Console.Error.WriteLine("Re-run with --force after replacing rename-like drops/adds with explicit rename operations or after accepting the data or integrity risk.");
            return 3;
        }

        IMigrationSqlGenerator generator = prov.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlServerMigrationSqlGenerator(),
            "sqlite" => new SqliteMigrationSqlGenerator(),
            "postgres" or "postgresql" => new PostgresMigrationSqlGenerator(),
            "mysql" => new MySqlMigrationSqlGenerator(),
            _ => throw new ArgumentException($"Provider '{prov}' not supported.")
        };

        var sql = generator.GenerateSql(diff);
        Directory.CreateDirectory(output);

        var version = long.Parse(DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
        var className = $"Migration_{version}_{ToCSharpIdentifier(name)}";
        var filePath = Path.Combine(output, className + ".cs");

        File.WriteAllText(filePath, MigrationCodeWriter.WriteMigrationSource(className, version, name, sql, destructiveWarnings));

        var snapJson = JsonSerializer.Serialize(newSnap, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(snapshotPath, snapJson);
        Console.WriteLine($"Migration '{className}' generated at {filePath}.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});

migrations.Add(add);
root.Add(migrations);

return await root.Parse(NormalizeEfStyleCommandArgs(args)).InvokeAsync(new InvocationConfiguration());

static string[] NormalizeEfStyleCommandArgs(string[] args)
{
    if (args.Length == 0 || !string.Equals(args[0], "dbcontext", StringComparison.OrdinalIgnoreCase))
        return args;

    if (args.Length == 1)
        return new[] { "scaffold", "--help" };

    if (IsHelpToken(args[1]))
        return new[] { "scaffold" }.Concat(args.Skip(1)).ToArray();

    if (string.Equals(args[1], "scaffold", StringComparison.OrdinalIgnoreCase))
    {
        return new[] { "scaffold" }.Concat(args.Skip(2)).ToArray();
    }

    return args;
}

static bool IsHelpToken(string token)
    => token is "--help" or "-h" or "/?";

static Assembly LoadDesignTimeAssembly(string assemblyPath, string? depsPath = null, string? runtimeConfigPath = null)
{
    var fullPath = Path.GetFullPath(assemblyPath);
    var loadContext = new DesignTimeAssemblyLoadContext(
        fullPath,
        GetDesignTimeProbeDirectories(fullPath, depsPath, runtimeConfigPath),
        GetDesignTimeDependencyProbePaths(depsPath, runtimeConfigPath));
    return loadContext.LoadFromAssemblyPath(fullPath);
}

static IReadOnlyList<string> GetDesignTimeProbeDirectories(string assemblyPath, params string?[] designTimeFiles)
{
    var directories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    AddProbeDirectory(Path.GetDirectoryName(assemblyPath));

    foreach (var file in designTimeFiles)
    {
        if (!string.IsNullOrWhiteSpace(file))
            AddProbeDirectory(Path.GetDirectoryName(Path.GetFullPath(file)));
    }

    return directories.ToArray();

    void AddProbeDirectory(string? directory)
    {
        if (!string.IsNullOrWhiteSpace(directory) && Directory.Exists(directory))
            directories.Add(directory);
    }
}

static bool TryResolveExistingDesignTimeFile(string? path, string description, out string? fullPath)
{
    fullPath = null;
    if (string.IsNullOrWhiteSpace(path))
        return true;

    fullPath = Path.GetFullPath(path);
    if (File.Exists(fullPath))
        return true;

    Console.Error.WriteLine($"{description} '{fullPath}' not found.");
    return false;
}

static DesignTimeDependencyProbePaths GetDesignTimeDependencyProbePaths(string? depsPath, string? runtimeConfigPath)
{
    if (string.IsNullOrWhiteSpace(depsPath))
        return DesignTimeDependencyProbePaths.Empty;

    var fullDepsPath = Path.GetFullPath(depsPath);
    var depsDirectory = Path.GetDirectoryName(fullDepsPath) ?? Directory.GetCurrentDirectory();
    var packageRoots = GetDesignTimePackageRoots(runtimeConfigPath).ToArray();
    var pathsByAssembly = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
    var pathsByNativeLibrary = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

    try
    {
        using var document = JsonDocument.Parse(File.ReadAllText(fullDepsPath));
        if (!document.RootElement.TryGetProperty("targets", out var targets) ||
            targets.ValueKind != JsonValueKind.Object)
        {
            return DesignTimeDependencyProbePaths.Empty;
        }

        foreach (var target in targets.EnumerateObject())
        {
            if (target.Value.ValueKind != JsonValueKind.Object)
                continue;

            foreach (var library in target.Value.EnumerateObject())
            {
                var (libraryName, libraryVersion) = SplitDepsLibraryName(library.Name);
                if (library.Value.ValueKind != JsonValueKind.Object)
                    continue;

                if (library.Value.TryGetProperty("runtime", out var runtimeAssets))
                    AddDepsRuntimeAssetPaths(pathsByAssembly, runtimeAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: false);

                if (library.Value.TryGetProperty("runtimeTargets", out var runtimeTargetAssets))
                {
                    AddDepsRuntimeAssetPaths(pathsByAssembly, runtimeTargetAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: true);
                    AddDepsNativeAssetPaths(pathsByNativeLibrary, runtimeTargetAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: true);
                }

                if (library.Value.TryGetProperty("native", out var nativeAssets))
                    AddDepsNativeAssetPaths(pathsByNativeLibrary, nativeAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: false);
            }
        }
    }
    catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
    {
        throw new InvalidOperationException($"Could not read design-time deps file '{fullDepsPath}': {ex.Message}", ex);
    }

    return new DesignTimeDependencyProbePaths(
        ToReadOnlyPathDictionary(pathsByAssembly),
        ToReadOnlyPathDictionary(pathsByNativeLibrary));
}

static IReadOnlyDictionary<string, IReadOnlyList<string>> ToReadOnlyPathDictionary(Dictionary<string, List<string>> pathsByKey)
    => pathsByKey.ToDictionary(
        static pair => pair.Key,
        static pair => (IReadOnlyList<string>)pair.Value,
        StringComparer.OrdinalIgnoreCase);

static (string? Name, string? Version) SplitDepsLibraryName(string libraryName)
{
    var separator = libraryName.LastIndexOf('/');
    if (separator <= 0 || separator == libraryName.Length - 1)
        return (NullIfWhiteSpace(libraryName), null);

    return (NullIfWhiteSpace(libraryName[..separator]), NullIfWhiteSpace(libraryName[(separator + 1)..]));
}

static void AddDepsRuntimeAssetPaths(
    Dictionary<string, List<string>> pathsByAssembly,
    JsonElement assets,
    string depsDirectory,
    IReadOnlyCollection<string> packageRoots,
    string? libraryName,
    string? libraryVersion,
    bool filterRuntimeTargets)
{
    if (assets.ValueKind != JsonValueKind.Object)
        return;

    foreach (var asset in assets.EnumerateObject())
    {
        if (filterRuntimeTargets &&
            asset.Value.ValueKind == JsonValueKind.Object &&
            asset.Value.TryGetProperty("assetType", out var assetType) &&
            assetType.ValueKind == JsonValueKind.String &&
            !string.Equals(assetType.GetString(), "runtime", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (!asset.Name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
            continue;

        var assemblyName = Path.GetFileNameWithoutExtension(asset.Name);
        if (string.IsNullOrWhiteSpace(assemblyName))
            continue;

        var candidates = GetDepsAssetCandidates(depsDirectory, packageRoots, libraryName, libraryVersion, asset.Name);
        foreach (var candidate in candidates)
            AddDesignTimeDependencyCandidate(pathsByAssembly, assemblyName, candidate);
    }
}

static void AddDepsNativeAssetPaths(
    Dictionary<string, List<string>> pathsByNativeLibrary,
    JsonElement assets,
    string depsDirectory,
    IReadOnlyCollection<string> packageRoots,
    string? libraryName,
    string? libraryVersion,
    bool filterRuntimeTargets)
{
    if (assets.ValueKind != JsonValueKind.Object)
        return;

    foreach (var asset in assets.EnumerateObject())
    {
        if (filterRuntimeTargets &&
            asset.Value.ValueKind == JsonValueKind.Object &&
            asset.Value.TryGetProperty("assetType", out var assetType) &&
            assetType.ValueKind == JsonValueKind.String &&
            !string.Equals(assetType.GetString(), "native", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (!IsNativeLibraryAsset(asset.Name))
            continue;

        var candidates = GetDepsAssetCandidates(depsDirectory, packageRoots, libraryName, libraryVersion, asset.Name);
        foreach (var key in GetNativeLibraryLookupKeys(asset.Name))
        {
            foreach (var candidate in candidates)
                AddDesignTimeDependencyCandidate(pathsByNativeLibrary, key, candidate);
        }
    }
}

static bool IsNativeLibraryAsset(string assetPath)
{
    var extension = Path.GetExtension(assetPath);
    return extension.Equals(".dll", StringComparison.OrdinalIgnoreCase)
        || extension.Equals(".so", StringComparison.OrdinalIgnoreCase)
        || extension.Equals(".dylib", StringComparison.OrdinalIgnoreCase);
}

static IEnumerable<string> GetNativeLibraryLookupKeys(string assetPath)
{
    var fileName = Path.GetFileName(assetPath);
    if (!string.IsNullOrWhiteSpace(fileName))
        yield return fileName;

    var stem = Path.GetFileNameWithoutExtension(assetPath);
    if (!string.IsNullOrWhiteSpace(stem))
    {
        yield return stem;
        if (stem.StartsWith("lib", StringComparison.OrdinalIgnoreCase) && stem.Length > 3)
            yield return stem[3..];
    }
}

static IEnumerable<string> GetDepsAssetCandidates(
    string depsDirectory,
    IReadOnlyCollection<string> packageRoots,
    string? libraryName,
    string? libraryVersion,
    string assetPath)
{
    var normalizedAssetPath = assetPath.Replace('/', Path.DirectorySeparatorChar);
    yield return Path.GetFullPath(Path.Combine(depsDirectory, normalizedAssetPath));

    if (!string.IsNullOrWhiteSpace(libraryName) && !string.IsNullOrWhiteSpace(libraryVersion))
    {
        yield return Path.GetFullPath(Path.Combine(depsDirectory, libraryName, libraryVersion, normalizedAssetPath));

        foreach (var packageRoot in packageRoots)
            yield return Path.GetFullPath(Path.Combine(packageRoot, libraryName.ToLowerInvariant(), libraryVersion.ToLowerInvariant(), normalizedAssetPath));
    }
}

static IEnumerable<string> GetDesignTimePackageRoots(string? runtimeConfigPath)
{
    var roots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    foreach (var path in GetRuntimeConfigAdditionalProbingPaths(runtimeConfigPath))
        if (roots.Add(path))
            yield return path;

    foreach (var path in GetNuGetPackageRoots())
        if (roots.Add(path))
            yield return path;
}

static IEnumerable<string> GetRuntimeConfigAdditionalProbingPaths(string? runtimeConfigPath)
{
    if (string.IsNullOrWhiteSpace(runtimeConfigPath))
        yield break;

    foreach (var configPath in EnumerateRuntimeConfigProbeFiles(Path.GetFullPath(runtimeConfigPath)))
    {
        if (!File.Exists(configPath))
            continue;

        string? configDirectory = Path.GetDirectoryName(configPath);
        using var document = JsonDocument.Parse(File.ReadAllText(configPath));
        if (!document.RootElement.TryGetProperty("runtimeOptions", out var runtimeOptions) ||
            runtimeOptions.ValueKind != JsonValueKind.Object ||
            !runtimeOptions.TryGetProperty("additionalProbingPaths", out var additionalProbingPaths) ||
            additionalProbingPaths.ValueKind != JsonValueKind.Array)
        {
            continue;
        }

        foreach (var path in additionalProbingPaths.EnumerateArray())
        {
            if (path.ValueKind != JsonValueKind.String)
                continue;

            var value = NullIfWhiteSpace(Environment.ExpandEnvironmentVariables(path.GetString() ?? string.Empty));
            if (value is null)
                continue;

            yield return Path.GetFullPath(Path.IsPathFullyQualified(value)
                ? value
                : Path.Combine(configDirectory ?? Directory.GetCurrentDirectory(), value));
        }
    }
}

static IEnumerable<string> EnumerateRuntimeConfigProbeFiles(string runtimeConfigPath)
{
    yield return runtimeConfigPath;

    const string runtimeConfigSuffix = ".runtimeconfig.json";
    if (runtimeConfigPath.EndsWith(runtimeConfigSuffix, StringComparison.OrdinalIgnoreCase))
        yield return runtimeConfigPath[..^runtimeConfigSuffix.Length] + ".runtimeconfig.dev.json";
}

static IEnumerable<string> GetNuGetPackageRoots()
{
    var configuredRoot = Environment.GetEnvironmentVariable("NUGET_PACKAGES");
    if (!string.IsNullOrWhiteSpace(configuredRoot))
        yield return configuredRoot;

    var userProfile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
    if (!string.IsNullOrWhiteSpace(userProfile))
        yield return Path.Combine(userProfile, ".nuget", "packages");
}

static void AddDesignTimeDependencyCandidate(
    Dictionary<string, List<string>> pathsByAssembly,
    string assemblyName,
    string candidate)
{
    if (!pathsByAssembly.TryGetValue(assemblyName, out var paths))
    {
        paths = new List<string>();
        pathsByAssembly[assemblyName] = paths;
    }

    if (!paths.Contains(candidate, StringComparer.OrdinalIgnoreCase))
        paths.Add(candidate);
}

static string[] BuildDesignTimeArgs(string? environment)
    => string.IsNullOrWhiteSpace(environment)
        ? Array.Empty<string>()
        : new[] { "--environment", environment };

static SchemaSnapshot BuildMigrationSnapshot(Assembly assembly, bool attributeOnly, string[] designTimeArgs)
{
    var factory = FindDesignTimeFactory(assembly);
    if (factory != null)
    {
        using var ctx = CreateDesignTimeContext(factory.Value.FactoryType, factory.Value.InterfaceType, designTimeArgs);
        Console.WriteLine($"Using design-time DbContext factory {factory.Value.FactoryType.FullName}.");
        return SchemaSnapshotBuilder.Build(ctx);
    }

    if (attributeOnly)
    {
        Console.WriteLine("Using attribute-only model snapshot.");
        return SchemaSnapshotBuilder.Build(assembly);
    }

    var ctxType = assembly.GetTypes()
        .FirstOrDefault(t => t.IsClass && !t.IsAbstract && typeof(DbContext).IsAssignableFrom(t));
    if (ctxType == null)
    {
        throw new InvalidOperationException(
            "No DbContext type was found. Add an INormDesignTimeDbContextFactory<TContext> implementation " +
            "or re-run with --attribute-only to generate from attributes only.");
    }

    try
    {
        using var modelCn = new SqliteConnection("Data Source=:memory:");
        modelCn.Open();
        var provider = new SqliteProvider();
        using var modelCtx = CreateModelContext(ctxType, modelCn, provider);
        Console.WriteLine($"Using fluent model from {ctxType.Name}.");
        return SchemaSnapshotBuilder.Build(modelCtx);
    }
    catch (Exception ex) when (ex is MissingMethodException or TargetInvocationException or InvalidOperationException or MemberAccessException)
    {
        throw new InvalidOperationException(
            $"Could not instantiate DbContext type '{ctxType.FullName}' for migration generation. " +
            "Add an INormDesignTimeDbContextFactory<TContext> implementation or re-run with --attribute-only " +
            "if you intentionally want to ignore fluent model configuration.",
            ex);
    }
}

static DbContext CreateModelContext(Type ctxType, DbConnection connection, DatabaseProvider provider)
{
    var twoArgConstructor = ctxType.GetConstructor(new[] { typeof(DbConnection), typeof(DatabaseProvider) });
    if (twoArgConstructor != null)
        return (DbContext)twoArgConstructor.Invoke(new object[] { connection, provider });

    var threeArgConstructor = ctxType.GetConstructor(new[] { typeof(DbConnection), typeof(DatabaseProvider), typeof(DbContextOptions) });
    if (threeArgConstructor != null)
        return (DbContext)threeArgConstructor.Invoke(new object?[] { connection, provider, null });

    throw new MissingMethodException(ctxType.FullName, ".ctor(DbConnection, DatabaseProvider[, DbContextOptions])");
}

static (Type FactoryType, Type InterfaceType)? FindDesignTimeFactory(Assembly assembly)
{
    foreach (var type in assembly.GetTypes().Where(static t => t.IsClass && !t.IsAbstract))
    {
        var interfaceType = type.GetInterfaces().FirstOrDefault(static i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(INormDesignTimeDbContextFactory<>));
        if (interfaceType != null)
            return (type, interfaceType);
    }

    return null;
}

static DbContext CreateDesignTimeContext(Type factoryType, Type interfaceType, string[] designTimeArgs)
{
    object factory;
    try
    {
        factory = Activator.CreateInstance(factoryType)
            ?? throw new InvalidOperationException($"Could not create design-time factory '{factoryType.FullName}'.");
    }
    catch (TargetInvocationException ex) when (ex.InnerException != null)
    {
        throw new InvalidOperationException(
            $"Design-time factory '{factoryType.FullName}' constructor failed: {ex.InnerException.Message}",
            ex.InnerException);
    }

    var method = interfaceType.GetMethod(nameof(INormDesignTimeDbContextFactory<DbContext>.CreateDbContext))
        ?? throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' does not expose CreateDbContext.");
    object? context;
    try
    {
        context = method.Invoke(factory, new object[] { designTimeArgs });
    }
    catch (TargetInvocationException ex) when (ex.InnerException != null)
    {
        throw new InvalidOperationException(
            $"Design-time factory '{factoryType.FullName}' failed while creating the DbContext: {ex.InnerException.Message}",
            ex.InnerException);
    }

    if (context is null)
        throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' returned null.");

    if (context is not DbContext dbContext)
        throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' did not return a nORM DbContext.");
    return dbContext;
}

static DbConnection CreateConnection(string provider, string connectionString)
{
    try
    {
        return NormalizeProviderName(provider) switch
        {
            "sqlserver" => new SqlConnection(connectionString),
            "sqlite" => new SqliteConnection(connectionString),
            "postgres" => CreateConnectionFromType(new[] { "Npgsql.NpgsqlConnection, Npgsql" }, "PostgreSQL", connectionString),
            "mysql" => CreateConnectionFromType(new[] { "MySql.Data.MySqlClient.MySqlConnection, MySql.Data", "MySqlConnector.MySqlConnection, MySqlConnector" }, "MySQL", connectionString),
            _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
        };
    }
    catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or TypeLoadException)
    {
        throw new InvalidOperationException($"Failed to create connection: {ex.Message}", ex);
    }
}

static DbConnection CreateConnectionFromType(string[] typeNames, string friendly, string connString)
{
    foreach (var name in typeNames)
    {
        var type = Type.GetType(name);
        if (type != null)
        {
            return (DbConnection)Activator.CreateInstance(type, connString)!;
        }
    }
    throw new InvalidOperationException($"{friendly} provider assembly not found. Ensure the appropriate package is referenced.");
}

static DatabaseProvider CreateProvider(string provider) =>
    NormalizeProviderName(provider) switch
    {
        "sqlserver" => new SqlServerProvider(),
        "sqlite" => new SqliteProvider(),
        "postgres" => new PostgresProvider(),
        "mysql" => new MySqlProvider(),
        _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
    };

static bool IsSystemSchema(string provider, string? schemaName)
{
    if (string.IsNullOrWhiteSpace(schemaName))
        return false;

    var s = schemaName.Trim();
    // Universal system schemas present in multiple providers.
    if (s.Equals("sys", StringComparison.OrdinalIgnoreCase)
        || s.Equals("information_schema", StringComparison.OrdinalIgnoreCase))
        return true;

    return provider.ToLowerInvariant() switch
    {
        // PostgreSQL system schemas: pg_catalog, pg_toast, pg_temp_*, pg_toast_temp_*, pg_*.
        "postgres" or "postgresql" =>
            s.StartsWith("pg_", StringComparison.OrdinalIgnoreCase),
        // MySQL: the mysql system database tables appear under the "mysql" schema.
        "mysql" =>
            s.Equals("mysql", StringComparison.OrdinalIgnoreCase)
            || s.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
}

static bool IsProtectedDatabaseName(string provider, string databaseName)
{
    if (string.IsNullOrWhiteSpace(databaseName))
        return false;

    var normalized = databaseName.Trim();
    return provider.ToLowerInvariant() switch
    {
        "sqlserver" => normalized.Equals("master", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("model", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("msdb", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
        "postgres" or "postgresql" => normalized.Equals("postgres", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("template0", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("template1", StringComparison.OrdinalIgnoreCase),
        "mysql" => normalized.Equals("mysql", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("sys", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
}

static IReadOnlyList<string>? ParseProviderTargetList(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    return value
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(static provider => NormalizeProviderTargetName(provider))
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();
}

static string NormalizeProviderTargetName(string providerName)
    => NormalizeProviderName(providerName);

static void ProbeProviderTargetConnection(
    string providerName,
    string connectionString,
    IDictionary<string, Version?> targetVersions,
    ICollection<ProviderMobilityFinding> findings)
{
    try
    {
        var validated = ConnectionStringValidator.Validate(connectionString, providerName);
        using var connection = CreateConnection(providerName, validated.ConnectionString);
        connection.Open();
        var provider = CreateProvider(providerName);
        provider.InitializeConnection(connection);
        ProbeProviderTargetFeatures(providerName, connection, findings);
        targetVersions[providerName] = ProviderMobilityTranslator.ParseProviderVersion(connection.ServerVersion);
    }
    catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
    {
        findings.Add(new ProviderMobilityFinding(
            "provider-target",
            0,
            "provider-target-open",
            "Error",
            $"Provider target '{providerName}' could not be opened and validated: {RedactConnectionStrings(ex.Message)}",
            "Fix the target connection string, driver, credentials, network access, or server version before claiming live provider mobility evidence."));
    }
}

static void ProbeProviderTargetFeatures(
    string providerName,
    DbConnection connection,
    ICollection<ProviderMobilityFinding> findings)
{
    var scalarProbes = providerName.ToLowerInvariant() switch
    {
        "sqlite" => new[]
        {
            ("JSON translation", "SELECT json_extract('{\"a\":1}', '$.a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY 1)")
        },
        "sqlserver" => new[]
        {
            ("JSON translation", "SELECT JSON_VALUE(N'{\"a\":1}', '$.a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1))")
        },
        "postgres" => new[]
        {
            ("JSON translation", "SELECT ('{\"a\":1}'::jsonb ->> 'a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY 1)")
        },
        "mysql" => new[]
        {
            ("JSON translation", "SELECT JSON_EXTRACT('{\"a\":1}', '$.a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY v) FROM (SELECT 1 AS v) AS t")
        },
        _ => Array.Empty<(string Feature, string Sql)>()
    };

    foreach (var (feature, sql) in scalarProbes)
    {
        ProbeProviderTargetFeature(providerName, feature, findings, () => ExecuteProviderTargetScalar(connection, sql));
    }

    ProbeProviderTargetFeature(providerName, "savepoint", findings, () => ProbeProviderTargetSavepoint(providerName, connection));
    ProbeProviderTargetFeature(providerName, "generated-value retrieval", findings, () => ProbeProviderTargetGeneratedValue(providerName, connection));
    ProbeProviderTargetFeature(providerName, "rename column DDL", findings, () => ProbeProviderTargetRenameColumn(providerName, connection));
    ProbeProviderTargetFeature(providerName, "idempotent insert/ignore", findings, () => ProbeProviderTargetIdempotentInsert(providerName, connection));
}

static void ProbeProviderTargetFeature(
    string providerName,
    string feature,
    ICollection<ProviderMobilityFinding> findings,
    Action probe)
{
    try
    {
        probe();
    }
    catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
    {
        findings.Add(new ProviderMobilityFinding(
            "provider-target",
            0,
            "provider-target-capability",
            "Error",
            $"Provider target '{providerName}' failed the {feature} probe: {RedactConnectionStrings(ex.Message)}",
            "Enable the required database feature/extension or remove that provider target before claiming live provider mobility evidence."));
    }
}

static object? ExecuteProviderTargetScalar(DbConnection connection, string sql, DbTransaction? transaction = null)
{
    using var command = connection.CreateCommand();
    command.CommandText = sql;
    command.Transaction = transaction;
    return command.ExecuteScalar();
}

static int ExecuteProviderTargetNonQuery(DbConnection connection, string sql, DbTransaction? transaction = null)
{
    using var command = connection.CreateCommand();
    command.CommandText = sql;
    command.Transaction = transaction;
    return command.ExecuteNonQuery();
}

static string ProviderTargetProbeName()
    => "__norm_cert_" + Guid.NewGuid().ToString("N")[..16];

static string ProviderTargetTable(DatabaseProvider provider, string tableName, string? schema = null)
    => string.IsNullOrWhiteSpace(schema)
        ? provider.Escape(tableName)
        : provider.Escape(schema) + "." + provider.Escape(tableName);

static void ProbeProviderTargetSavepoint(string providerName, DbConnection connection)
{
    using var transaction = connection.BeginTransaction();
    try
    {
        if (providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteProviderTargetNonQuery(connection, "SAVE TRANSACTION norm_cert_sp", transaction);
            ExecuteProviderTargetNonQuery(connection, "ROLLBACK TRANSACTION norm_cert_sp", transaction);
        }
        else
        {
            ExecuteProviderTargetNonQuery(connection, "SAVEPOINT norm_cert_sp", transaction);
            ExecuteProviderTargetNonQuery(connection, "ROLLBACK TO SAVEPOINT norm_cert_sp", transaction);
            ExecuteProviderTargetNonQuery(connection, "RELEASE SAVEPOINT norm_cert_sp", transaction);
        }

        transaction.Rollback();
    }
    catch
    {
        try { transaction.Rollback(); } catch { }
        throw;
    }
}

static void ProbeProviderTargetGeneratedValue(string providerName, DbConnection connection)
{
    var provider = CreateProvider(providerName);
    var tableName = ProviderTargetProbeName();
    var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);

    try
    {
        switch (providerName.ToLowerInvariant())
        {
            case "sqlite":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("Id")} INTEGER PRIMARY KEY AUTOINCREMENT, {provider.Escape("Name")} TEXT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a')");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, "SELECT last_insert_rowid()"));
                break;
            case "sqlserver":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({provider.Escape("Id")} int IDENTITY(1,1) NOT NULL PRIMARY KEY, {provider.Escape("Name")} nvarchar(20) NULL)");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) OUTPUT INSERTED.{provider.Escape("Id")} VALUES (N'a')"));
                break;
            case "postgres":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("Id")} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {provider.Escape("Name")} text NULL)");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a') RETURNING {provider.Escape("Id")}"));
                break;
            case "mysql":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({provider.Escape("Id")} INT NOT NULL AUTO_INCREMENT PRIMARY KEY, {provider.Escape("Name")} varchar(20) NULL)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a')");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, "SELECT LAST_INSERT_ID()"));
                break;
        }
    }
    finally
    {
        DropProviderTargetProbeTable(providerName, connection, provider, tableName);
    }
}

static void ProbeProviderTargetRenameColumn(string providerName, DbConnection connection)
{
    var provider = CreateProvider(providerName);
    var tableName = ProviderTargetProbeName();
    var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);

    try
    {
        switch (providerName.ToLowerInvariant())
        {
            case "sqlite":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("OldName")} INTEGER NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                break;
            case "sqlserver":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({provider.Escape("OldName")} int NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"EXEC sp_rename N'dbo.{tableName}.OldName', N'NewName', N'COLUMN'");
                break;
            case "postgres":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("OldName")} integer NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                break;
            case "mysql":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({provider.Escape("OldName")} INT NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                break;
        }
    }
    finally
    {
        DropProviderTargetProbeTable(providerName, connection, provider, tableName);
    }
}

static void ProbeProviderTargetIdempotentInsert(string providerName, DbConnection connection)
{
    var provider = CreateProvider(providerName);
    var tableName = ProviderTargetProbeName();
    var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);
    var code = provider.Escape("Code");

    try
    {
        switch (providerName.ToLowerInvariant())
        {
            case "sqlite":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({code} INTEGER NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT OR IGNORE INTO {table} ({code}) VALUES (1)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT OR IGNORE INTO {table} ({code}) VALUES (1)");
                break;
            case "sqlserver":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({code} int NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {code} = 1) INSERT INTO {table} ({code}) VALUES (1)");
                ExecuteProviderTargetNonQuery(connection, $"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {code} = 1) INSERT INTO {table} ({code}) VALUES (1)");
                break;
            case "postgres":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({code} integer NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({code}) VALUES (1) ON CONFLICT DO NOTHING");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({code}) VALUES (1) ON CONFLICT DO NOTHING");
                break;
            case "mysql":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({code} INT NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT IGNORE INTO {table} ({code}) VALUES (1)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT IGNORE INTO {table} ({code}) VALUES (1)");
                break;
        }

        var count = Convert.ToInt64(ExecuteProviderTargetScalar(connection, $"SELECT COUNT(*) FROM {table}") ?? 0);
        if (count != 1)
            throw new InvalidOperationException($"Idempotent insert probe expected one row but found {count}.");
    }
    finally
    {
        DropProviderTargetProbeTable(providerName, connection, provider, tableName);
    }
}

static void EnsureProviderTargetScalarValue(string feature, object? value)
{
    if (value is null || value is DBNull)
        throw new InvalidOperationException($"The {feature} probe returned no value.");
}

static void DropProviderTargetProbeTable(string providerName, DbConnection connection, DatabaseProvider provider, string tableName)
{
    try
    {
        if (providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteProviderTargetNonQuery(
                connection,
                $"IF OBJECT_ID(N'dbo.{tableName}', N'U') IS NOT NULL DROP TABLE {ProviderTargetTable(provider, tableName, "dbo")}");
            return;
        }

        var table = ProviderTargetTable(provider, tableName);
        if (providerName.Equals("mysql", StringComparison.OrdinalIgnoreCase))
            ExecuteProviderTargetNonQuery(connection, $"DROP TEMPORARY TABLE IF EXISTS {table}");
        else
            ExecuteProviderTargetNonQuery(connection, $"DROP TABLE IF EXISTS {table}");
    }
    catch
    {
        // Best-effort cleanup; the probe failure path reports the original feature error.
    }
}

static int Fail(Exception ex, int exitCode = 1)
{
    Console.Error.WriteLine($"Error: {RedactConnectionStrings(ex.Message)}");
    return exitCode;
}

/// <summary>
/// Resolves the build output assembly path for a project by running
/// <c>dotnet msbuild -getProperty:TargetPath</c> on the project file.
/// Returns null if the path cannot be determined.
/// </summary>
static string? ResolveAssemblyFromProject(
    string projectPath,
    string? targetFramework = null,
    string? configuration = null,
    string? runtime = null)
{
    try
    {
        var startInfo = new System.Diagnostics.ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        startInfo.ArgumentList.Add("msbuild");
        startInfo.ArgumentList.Add(projectPath);
        AddMSBuildProperty(startInfo, "TargetFramework", targetFramework);
        AddMSBuildProperty(startInfo, "Configuration", configuration);
        AddMSBuildProperty(startInfo, "RuntimeIdentifier", runtime);
        startInfo.ArgumentList.Add("-getProperty:TargetPath");
        startInfo.ArgumentList.Add("--verbosity:quiet");

        using var proc = System.Diagnostics.Process.Start(startInfo);
        if (proc == null) return null;
        var output = proc.StandardOutput.ReadToEnd().Trim();
        proc.WaitForExit();
        if (proc.ExitCode == 0 && !string.IsNullOrWhiteSpace(output) && File.Exists(output))
            return output;
        return null;
    }
    catch
    {
        return null;
    }
}

static void AddMSBuildProperty(System.Diagnostics.ProcessStartInfo startInfo, string propertyName, string? value)
{
    if (!string.IsNullOrWhiteSpace(value))
        startInfo.ArgumentList.Add($"-property:{propertyName}={value}");
}

sealed record DesignTimeDependencyProbePaths(
    IReadOnlyDictionary<string, IReadOnlyList<string>> Assemblies,
    IReadOnlyDictionary<string, IReadOnlyList<string>> NativeLibraries)
{
    public static DesignTimeDependencyProbePaths Empty { get; } = new(
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase));
}

sealed class DesignTimeEnvironmentScope : IDisposable
{
    private readonly bool _changed;
    private readonly string? _previousAspNetCoreEnvironment;
    private readonly string? _previousDotnetEnvironment;

    private DesignTimeEnvironmentScope(string? environment)
    {
        if (string.IsNullOrWhiteSpace(environment))
            return;

        _changed = true;
        _previousAspNetCoreEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        _previousDotnetEnvironment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", environment);
        Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", environment);
    }

    public static DesignTimeEnvironmentScope Apply(string? environment) => new(environment);

    public void Dispose()
    {
        if (!_changed)
            return;

        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", _previousAspNetCoreEnvironment);
        Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", _previousDotnetEnvironment);
    }
}

sealed class DesignTimeAssemblyLoadContext : AssemblyLoadContext
{
    private readonly AssemblyDependencyResolver _resolver;
    private readonly IReadOnlyList<string> _probeDirectories;
    private readonly DesignTimeDependencyProbePaths _dependencyProbePaths;

    public DesignTimeAssemblyLoadContext(
        string mainAssemblyPath,
        IReadOnlyList<string> probeDirectories,
        DesignTimeDependencyProbePaths dependencyProbePaths)
    {
        _resolver = new AssemblyDependencyResolver(mainAssemblyPath);
        _probeDirectories = probeDirectories;
        _dependencyProbePaths = dependencyProbePaths;
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var sharedAssembly = AssemblyLoadContext.Default.Assemblies.FirstOrDefault(assembly =>
            AssemblyName.ReferenceMatchesDefinition(assembly.GetName(), assemblyName));
        if (sharedAssembly != null)
            return sharedAssembly;

        var assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
        assemblyPath ??= ResolveAssemblyFromProbeDirectories(assemblyName);
        return assemblyPath == null ? null : LoadFromAssemblyPath(assemblyPath);
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        var libraryPath = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        libraryPath ??= ResolveNativeLibraryFromDeps(unmanagedDllName);
        return libraryPath == null ? IntPtr.Zero : LoadUnmanagedDllFromPath(libraryPath);
    }

    private string? ResolveAssemblyFromProbeDirectories(AssemblyName assemblyName)
    {
        if (string.IsNullOrWhiteSpace(assemblyName.Name))
            return null;

        if (_dependencyProbePaths.Assemblies.TryGetValue(assemblyName.Name, out var dependencyPaths))
        {
            foreach (var dependencyPath in dependencyPaths)
            {
                if (File.Exists(dependencyPath))
                    return dependencyPath;
            }
        }

        foreach (var directory in _probeDirectories)
        {
            var candidate = Path.Combine(directory, assemblyName.Name + ".dll");
            if (File.Exists(candidate))
                return candidate;

            var refsCandidate = Path.Combine(directory, "refs", assemblyName.Name + ".dll");
            if (File.Exists(refsCandidate))
                return refsCandidate;
        }

        return null;
    }

    private string? ResolveNativeLibraryFromDeps(string unmanagedDllName)
    {
        foreach (var key in GetNativeLibraryResolverKeys(unmanagedDllName))
        {
            if (!_dependencyProbePaths.NativeLibraries.TryGetValue(key, out var dependencyPaths))
                continue;

            foreach (var dependencyPath in dependencyPaths)
            {
                if (File.Exists(dependencyPath))
                    return dependencyPath;
            }
        }

        return null;
    }

    private static IEnumerable<string> GetNativeLibraryResolverKeys(string unmanagedDllName)
    {
        yield return unmanagedDllName;

        var fileName = Path.GetFileName(unmanagedDllName);
        if (!string.IsNullOrWhiteSpace(fileName) &&
            !string.Equals(fileName, unmanagedDllName, StringComparison.OrdinalIgnoreCase))
        {
            yield return fileName;
        }

        var stem = Path.GetFileNameWithoutExtension(unmanagedDllName);
        if (!string.IsNullOrWhiteSpace(stem) &&
            !string.Equals(stem, unmanagedDllName, StringComparison.OrdinalIgnoreCase))
        {
            yield return stem;
        }
    }
}
