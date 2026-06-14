using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using nORM.Core;
using nORM.Scaffolding;

partial class Program
{
    private static ScaffoldConnectionProviderInput ResolveScaffoldConnectionProviderInput(
        ParseResult result,
        ScaffoldCommandBindings bindings)
    {
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

        return new ScaffoldConnectionProviderInput(connectionReference, NormalizeProviderName(providerName));
    }

    private static void ReadScaffoldCompatibilityOptions(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig)
    {
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.FrameworkOption, "--framework"), efToolConfig?.Framework);
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ConfigurationOption, "--configuration"), efToolConfig?.Configuration);
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.RuntimeOption, "--runtime"), efToolConfig?.Runtime);
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.MsbuildProjectExtensionsPathOption, "--msbuildprojectextensionspath"), efToolConfig?.MsbuildProjectExtensionsPath);
        _ = GetScaffoldBoolOptionOrConfig(result, bindings.NoBuildOption, efToolConfig?.NoBuild);
        _ = result.GetValue(bindings.VerboseOption) || efToolConfig?.Verbose == true;
        _ = result.GetValue(bindings.NoColorOption) || efToolConfig?.NoColor == true;
        _ = result.GetValue(bindings.PrefixOutputOption) || efToolConfig?.PrefixOutput == true;
        _ = GetScaffoldBoolOptionOrConfig(result, bindings.NoOnConfiguringOption, efToolConfig?.NoOnConfiguring);
        _ = GetScaffoldBoolOptionOrConfig(result, bindings.DataAnnotationsOption, efToolConfig?.DataAnnotations);
    }

    private static ScaffoldOutputNaming ResolveScaffoldOutputNaming(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig,
        ScaffoldProjectInfo? projectInfo,
        string output,
        string connectionString,
        ScaffoldConnectionProviderInput input)
    {
        var explicitNamespace = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.NamespaceOption, "--namespace"), efToolConfig?.Namespace);
        var ns = ValidateScaffoldNamespaceName(ResolveScaffoldNamespace(explicitNamespace, projectInfo, output), "--namespace");
        var contextDirectory = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextDirectoryOption, "--context-dir"), efToolConfig?.ContextDir);
        var contextOutputDirectory = ResolveScaffoldContextOutputDirectory(contextDirectory, projectInfo);
        var explicitContextName = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextOption, "--context"), efToolConfig?.Context);
        var contextName = explicitContextName
            ?? InferScaffoldContextName(connectionString, input.ConnectionReference, input.ProviderName);
        var explicitContextNamespace = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextNamespaceOption, "--context-namespace"), efToolConfig?.ContextNamespace);
        var (ctx, contextNamespace) = ResolveScaffoldContextNameAndNamespace(contextName, explicitContextNamespace, ns, contextDirectory, explicitNamespace, projectInfo);
        if (explicitContextName is not null)
            ctx = ValidateScaffoldContextClassName(ctx);
        if (contextNamespace is not null)
            contextNamespace = ValidateScaffoldNamespaceName(contextNamespace, explicitContextNamespace is null ? "context namespace" : "--context-namespace");

        return new ScaffoldOutputNaming(ns, ctx, contextNamespace, contextOutputDirectory);
    }

    private static bool ResolveScaffoldOverwrite(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig)
    {
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

        return forceOverwrite;
    }

    private static ScaffoldFilters ResolveScaffoldFilters(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig)
    {
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

        return new ScaffoldFilters(schemaFilters, tableFilters);
    }

    private static ScaffoldOptions CreateScaffoldOptions(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig,
        ScaffoldProjectInfo? projectInfo,
        ScaffoldOutputNaming naming,
        ScaffoldFilters filters,
        bool forceOverwrite)
        => new()
        {
            Schemas = filters.Schemas,
            Tables = filters.Tables,
            UsePluralizer = !GetScaffoldBoolOptionOrConfig(result, bindings.NoPluralizeOption, efToolConfig?.NoPluralize),
            UseDatabaseNames = GetScaffoldBoolOptionOrConfig(result, bindings.UseDatabaseNamesOption, efToolConfig?.UseDatabaseNames),
            UseNullableReferenceTypes = projectInfo?.UseNullableReferenceTypes ?? true,
            ContextOutputDirectory = naming.ContextOutputDirectory,
            ContextNamespace = naming.ContextNamespace,
            OverwriteFiles = forceOverwrite,
            DryRun = GetScaffoldBoolOptionOrConfig(result, bindings.DryRunOption, efToolConfig?.DryRun),
            FailOnWarnings = GetScaffoldBoolOptionOrConfig(result, bindings.FailOnWarningsOption, efToolConfig?.FailOnWarnings),
            EmitRoutineStubs = GetScaffoldBoolOptionOrConfig(result, bindings.EmitRoutineStubsOption, efToolConfig?.EmitRoutineStubs),
            EmitSequenceStubs = GetScaffoldBoolOptionOrConfig(result, bindings.EmitSequenceStubsOption, efToolConfig?.EmitSequenceStubs),
            EmitViewEntities = GetScaffoldBoolOptionOrConfig(result, bindings.EmitViewEntitiesOption, efToolConfig?.EmitViewEntities),
            EmitQueryArtifacts = GetScaffoldBoolOptionOrConfig(result, bindings.EmitQueryArtifactsOption, efToolConfig?.EmitQueryArtifacts)
        };

    private sealed record ScaffoldConnectionProviderInput(string ConnectionReference, string ProviderName);

    private sealed record ScaffoldOutputNaming(
        string Namespace,
        string ContextName,
        string? ContextNamespace,
        string? ContextOutputDirectory);

    private sealed record ScaffoldFilters(
        IReadOnlyCollection<string> Schemas,
        IReadOnlyCollection<string> Tables);
}
