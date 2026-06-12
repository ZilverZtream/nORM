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
}
