using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using nORM.Core;
using nORM.Scaffolding;

partial class Program
{
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
