using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Scaffolding;
using nORM.Security;

partial class Program
{
    private sealed class ScaffoldCommandBindings
    {
        public required Argument<string?> ConnectionArgument { get; init; }
        public required Argument<string?> ProviderArgument { get; init; }
        public required Option<string?> ConnectionOption { get; init; }
        public required Option<string?> ProviderOption { get; init; }
        public required Option<string> OutputOption { get; init; }
        public required Option<string?> NamespaceOption { get; init; }
        public required Option<string?> ContextOption { get; init; }
        public required Option<string?> ProjectOption { get; init; }
        public required Option<string?> StartupProjectOption { get; init; }
        public required Option<string?> FrameworkOption { get; init; }
        public required Option<string?> ConfigurationOption { get; init; }
        public required Option<string?> RuntimeOption { get; init; }
        public required Option<bool> JsonOption { get; init; }
        public required Option<bool> VerboseOption { get; init; }
        public required Option<bool> NoColorOption { get; init; }
        public required Option<bool> PrefixOutputOption { get; init; }
        public required Option<string?> ContextDirectoryOption { get; init; }
        public required Option<string?> ContextNamespaceOption { get; init; }
        public required Option<string?> SchemasOption { get; init; }
        public required Option<string[]> SchemaOption { get; init; }
        public required Option<string?> TablesOption { get; init; }
        public required Option<string[]> TableOption { get; init; }
        public required Option<bool> NoPluralizeOption { get; init; }
        public required Option<bool> UseDatabaseNamesOption { get; init; }
        public required Option<bool> ForceOption { get; init; }
        public required Option<bool> NoOverwriteOption { get; init; }
        public required Option<bool> DryRunOption { get; init; }
        public required Option<bool> FailOnWarningsOption { get; init; }
        public required Option<bool> EmitRoutineStubsOption { get; init; }
        public required Option<bool> EmitSequenceStubsOption { get; init; }
        public required Option<bool> EmitViewEntitiesOption { get; init; }
        public required Option<bool> EmitQueryArtifactsOption { get; init; }
    }

    private sealed record ScaffoldCommandRequest(
        string ProviderName,
        string ConnectionString,
        string Output,
        string Namespace,
        string ContextName,
        ScaffoldOptions Options);

    private static async Task<int> RunScaffoldCommandAsync(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        CancellationToken cancellationToken)
    {
        var jsonOutput = result.GetValue(bindings.JsonOption);
        var outputForJson = result.GetValue(bindings.OutputOption) ?? ".";
        var dryRunForJson = result.GetValue(bindings.DryRunOption);

        try
        {
            var request = ResolveScaffoldCommandRequest(result, bindings);
            outputForJson = request.Output;
            return await ExecuteScaffoldCommandAsync(request, jsonOutput, cancellationToken);
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
    }

    private static ScaffoldCommandRequest ResolveScaffoldCommandRequest(ParseResult result, ScaffoldCommandBindings bindings)
    {
        ValidateScaffoldUnmatchedTokens(result);
        var connectionOption = GetOptionalNonBlankScaffoldOption(result, bindings.ConnectionOption, "--connection");
        var providerOption = GetOptionalNonBlankScaffoldOption(result, bindings.ProviderOption, "--provider");
        var connectionPosition = NullIfWhiteSpace(result.GetValue(bindings.ConnectionArgument));
        var providerPosition = NullIfWhiteSpace(result.GetValue(bindings.ProviderArgument));
        if (connectionOption is not null && providerOption is null && providerPosition is null && connectionPosition is not null)
        {
            providerPosition = connectionPosition;
            connectionPosition = null;
        }

        var connectionReference = FirstNonBlank(connectionOption, connectionPosition)
            ?? throw new NormConfigurationException("Scaffold requires a database connection string. Pass --connection <CONNECTION> or the EF-style positional <CONNECTION> argument.");
        var providerName = FirstNonBlank(providerOption, providerPosition)
            ?? throw new NormConfigurationException("Scaffold requires a database provider. Pass --provider <PROVIDER> or the EF-style positional <PROVIDER> argument.");
        providerName = NormalizeProviderName(providerName);

        var efToolConfig = LoadEfToolConfig();
        _ = FirstNonBlank(result.GetValue(bindings.FrameworkOption), efToolConfig?.Framework);
        _ = FirstNonBlank(result.GetValue(bindings.ConfigurationOption), efToolConfig?.Configuration);
        _ = FirstNonBlank(result.GetValue(bindings.RuntimeOption), efToolConfig?.Runtime);
        _ = result.GetValue(bindings.VerboseOption) || efToolConfig?.Verbose == true;
        _ = result.GetValue(bindings.NoColorOption) || efToolConfig?.NoColor == true;
        _ = result.GetValue(bindings.PrefixOutputOption) || efToolConfig?.PrefixOutput == true;

        var projectInfo = ResolveScaffoldProject(FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ProjectOption, "--project"), efToolConfig?.Project), inferCurrentDirectory: true);
        var startupProjectInfo = IsNamedConnectionReference(connectionReference)
            ? ResolveScaffoldProject(FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.StartupProjectOption, "--startup-project"), efToolConfig?.StartupProject))
            : null;
        var scaffoldEnvironment = GetScaffoldPassThroughEnvironment();
        var connectionString = ResolveScaffoldConnectionString(connectionReference, projectInfo, startupProjectInfo, scaffoldEnvironment);
        var validated = ConnectionStringValidator.Validate(connectionString, providerName);
        var output = ResolveScaffoldOutputPath(GetRequiredNonBlankScaffoldOption(result, bindings.OutputOption, "--output"), projectInfo);

        var explicitNamespace = GetOptionalNonBlankScaffoldOption(result, bindings.NamespaceOption, "--namespace");
        var ns = ValidateScaffoldNamespaceName(ResolveScaffoldNamespace(explicitNamespace, projectInfo, output), "--namespace");
        var contextDirectory = GetOptionalNonBlankScaffoldOption(result, bindings.ContextDirectoryOption, "--context-dir");
        var contextOutputDirectory = ResolveScaffoldContextOutputDirectory(contextDirectory, projectInfo);
        var explicitContextName = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextOption, "--context"), efToolConfig?.Context);
        var contextName = explicitContextName
            ?? InferScaffoldContextName(validated.ConnectionString, connectionReference, providerName);
        var explicitContextNamespace = GetOptionalNonBlankScaffoldOption(result, bindings.ContextNamespaceOption, "--context-namespace");
        var (ctx, contextNamespace) = ResolveScaffoldContextNameAndNamespace(contextName, explicitContextNamespace, ns, contextDirectory, explicitNamespace, projectInfo);
        if (explicitContextName is not null)
            ctx = ValidateScaffoldContextClassName(ctx);
        if (contextNamespace is not null)
            contextNamespace = ValidateScaffoldNamespaceName(contextNamespace, explicitContextNamespace is null ? "context namespace" : "--context-namespace");

        var forceOverwrite = result.GetValue(bindings.ForceOption);
        var noOverwrite = result.GetValue(bindings.NoOverwriteOption);
        if (forceOverwrite && noOverwrite)
            throw new NormConfigurationException("Use either --force or --no-overwrite for scaffold output conflicts, not both.");

        var options = new ScaffoldOptions
        {
            Schemas = ParseSchemaFilters(result.GetValue(bindings.SchemasOption), result.GetValue(bindings.SchemaOption)),
            Tables = ParseTableFilters(result.GetValue(bindings.TablesOption), result.GetValue(bindings.TableOption)),
            PluralizeQueryProperties = !result.GetValue(bindings.NoPluralizeOption),
            UseDatabaseNames = result.GetValue(bindings.UseDatabaseNamesOption),
            UseNullableReferenceTypes = projectInfo?.UseNullableReferenceTypes ?? true,
            ContextOutputDirectory = contextOutputDirectory,
            ContextNamespace = contextNamespace,
            OverwriteFiles = forceOverwrite,
            DryRun = result.GetValue(bindings.DryRunOption),
            FailOnWarnings = result.GetValue(bindings.FailOnWarningsOption),
            EmitRoutineStubs = result.GetValue(bindings.EmitRoutineStubsOption),
            EmitSequenceStubs = result.GetValue(bindings.EmitSequenceStubsOption),
            EmitViewEntities = result.GetValue(bindings.EmitViewEntitiesOption),
            EmitQueryArtifacts = result.GetValue(bindings.EmitQueryArtifactsOption)
        };

        return new ScaffoldCommandRequest(providerName, validated.ConnectionString, output, ns, ctx, options);
    }

    private static async Task<int> ExecuteScaffoldCommandAsync(
        ScaffoldCommandRequest request,
        bool jsonOutput,
        CancellationToken cancellationToken)
    {
        using var connection = CreateConnection(request.ProviderName, request.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        var provider = CreateProvider(request.ProviderName);

        string? dryRunTempOutput = null;
        var scaffoldOutput = request.Output;
        var scaffoldOptions = request.Options;
        if (request.Options.DryRun)
        {
            dryRunTempOutput = Path.Combine(Path.GetTempPath(), "norm_scaffold_dryrun_" + Guid.NewGuid().ToString("N"));
            scaffoldOutput = dryRunTempOutput;
            scaffoldOptions = CreateDryRunScaffoldOptions(request.Options, dryRunTempOutput);
        }

        try
        {
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(connection, provider, scaffoldOutput, request.Namespace, request.ContextName, scaffoldOptions);
            }
            catch (NormConfigurationException ex) when (IsScaffoldWarningsFailure(ex, scaffoldOutput))
            {
                if (jsonOutput)
                    WriteScaffoldResultJson("failed", request.Output, request.Options.DryRun, reportsWritten: !request.Options.DryRun, ReadScaffoldWarningSummary(scaffoldOutput), ex);
                else
                    PrintScaffoldWarningSummary(scaffoldOutput);
                return jsonOutput ? 1 : Fail(ex);
            }

            var warningSummary = ReadScaffoldWarningSummary(scaffoldOutput);
            if (jsonOutput)
            {
                WriteScaffoldResultJson("succeeded", request.Output, request.Options.DryRun, reportsWritten: !request.Options.DryRun && warningSummary.TotalWarnings > 0, warningSummary);
            }
            else if (request.Options.DryRun)
            {
                if (warningSummary.TotalWarnings > 0 || warningSummary.Error is not null)
                    PrintScaffoldWarningSummary(scaffoldOutput);
                Console.WriteLine($"Scaffolding dry run completed. No files were written to {request.Output}.");
            }
            else
            {
                Console.WriteLine($"Scaffolding completed. Files written to {request.Output}.");
            }

            if (!jsonOutput && !request.Options.DryRun && warningSummary.TotalWarnings > 0)
            {
                Console.WriteLine("Scaffolding warnings were written to nORM.ScaffoldWarnings.md and nORM.ScaffoldWarnings.json.");
                PrintScaffoldWarningSummary(request.Output);
            }
        }
        finally
        {
            if (dryRunTempOutput is not null)
                TryDeleteDirectory(dryRunTempOutput);
        }

        return 0;
    }

    private static ScaffoldOptions CreateDryRunScaffoldOptions(ScaffoldOptions options, string dryRunTempOutput)
        => new()
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
