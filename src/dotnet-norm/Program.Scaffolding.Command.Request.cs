using System.CommandLine;
using System.CommandLine.Parsing;
using nORM.Core;
using nORM.Scaffolding;
using nORM.Security;

partial class Program
{
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
        _ = FirstNonBlank(result.GetValue(bindings.MsbuildProjectExtensionsPathOption), efToolConfig?.MsbuildProjectExtensionsPath);
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
        var output = ResolveScaffoldOutputPath(GetRequiredNonBlankScaffoldOption(result, bindings.OutputOption, "--output", efToolConfig?.OutputDir), projectInfo);

        var explicitNamespace = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.NamespaceOption, "--namespace"), efToolConfig?.Namespace);
        var ns = ValidateScaffoldNamespaceName(ResolveScaffoldNamespace(explicitNamespace, projectInfo, output), "--namespace");
        var contextDirectory = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextDirectoryOption, "--context-dir"), efToolConfig?.ContextDir);
        var contextOutputDirectory = ResolveScaffoldContextOutputDirectory(contextDirectory, projectInfo);
        var explicitContextName = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextOption, "--context"), efToolConfig?.Context);
        var contextName = explicitContextName
            ?? InferScaffoldContextName(validated.ConnectionString, connectionReference, providerName);
        var explicitContextNamespace = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextNamespaceOption, "--context-namespace"), efToolConfig?.ContextNamespace);
        var (ctx, contextNamespace) = ResolveScaffoldContextNameAndNamespace(contextName, explicitContextNamespace, ns, contextDirectory, explicitNamespace, projectInfo);
        if (explicitContextName is not null)
            ctx = ValidateScaffoldContextClassName(ctx);
        if (contextNamespace is not null)
            contextNamespace = ValidateScaffoldNamespaceName(contextNamespace, explicitContextNamespace is null ? "context namespace" : "--context-namespace");

        var forceExplicit = IsScaffoldOptionExplicit(result, bindings.ForceOption);
        var noOverwriteExplicit = IsScaffoldOptionExplicit(result, bindings.NoOverwriteOption);
        var forceOverwrite = forceExplicit
            ? result.GetValue(bindings.ForceOption)
            : !noOverwriteExplicit && efToolConfig?.Force == true;
        var noOverwrite = noOverwriteExplicit
            ? result.GetValue(bindings.NoOverwriteOption)
            : !forceExplicit && efToolConfig?.NoOverwrite == true;
        if (forceOverwrite && noOverwrite)
            throw new NormConfigurationException("Use either --force or --no-overwrite for scaffold output conflicts, not both.");

        var hasExplicitCliFilters =
            IsScaffoldOptionExplicit(result, bindings.SchemasOption) ||
            IsScaffoldOptionExplicit(result, bindings.SchemaOption) ||
            IsScaffoldOptionExplicit(result, bindings.TablesOption) ||
            IsScaffoldOptionExplicit(result, bindings.TableOption);
        var schemaFilters = ParseSchemaFilters(result.GetValue(bindings.SchemasOption), result.GetValue(bindings.SchemaOption));
        var tableFilters = ParseTableFilters(result.GetValue(bindings.TablesOption), result.GetValue(bindings.TableOption));
        if (!hasExplicitCliFilters)
        {
            if (efToolConfig?.Schemas.Count > 0)
                schemaFilters = efToolConfig.Schemas;
            if (efToolConfig?.Tables.Count > 0)
                tableFilters = efToolConfig.Tables;
        }

        var options = new ScaffoldOptions
        {
            Schemas = schemaFilters,
            Tables = tableFilters,
            UsePluralizer = !GetScaffoldBoolOptionOrConfig(result, bindings.NoPluralizeOption, efToolConfig?.NoPluralize),
            UseDatabaseNames = GetScaffoldBoolOptionOrConfig(result, bindings.UseDatabaseNamesOption, efToolConfig?.UseDatabaseNames),
            UseNullableReferenceTypes = projectInfo?.UseNullableReferenceTypes ?? true,
            ContextOutputDirectory = contextOutputDirectory,
            ContextNamespace = contextNamespace,
            OverwriteFiles = forceOverwrite,
            DryRun = GetScaffoldBoolOptionOrConfig(result, bindings.DryRunOption, efToolConfig?.DryRun),
            FailOnWarnings = GetScaffoldBoolOptionOrConfig(result, bindings.FailOnWarningsOption, efToolConfig?.FailOnWarnings),
            EmitRoutineStubs = GetScaffoldBoolOptionOrConfig(result, bindings.EmitRoutineStubsOption, efToolConfig?.EmitRoutineStubs),
            EmitSequenceStubs = GetScaffoldBoolOptionOrConfig(result, bindings.EmitSequenceStubsOption, efToolConfig?.EmitSequenceStubs),
            EmitViewEntities = GetScaffoldBoolOptionOrConfig(result, bindings.EmitViewEntitiesOption, efToolConfig?.EmitViewEntities),
            EmitQueryArtifacts = GetScaffoldBoolOptionOrConfig(result, bindings.EmitQueryArtifactsOption, efToolConfig?.EmitQueryArtifacts)
        };

        return new ScaffoldCommandRequest(providerName, validated.ConnectionString, output, ns, ctx, options);
    }
}
