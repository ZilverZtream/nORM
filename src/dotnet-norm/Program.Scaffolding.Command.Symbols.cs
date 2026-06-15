using System.CommandLine;

partial class Program
{
    private sealed class ScaffoldCommandSymbols
    {
        public ScaffoldCommandSymbols(
            ScaffoldConnectionCommandSymbols connection,
            ScaffoldProjectNamingCommandSymbols projectNaming,
            ScaffoldCompatibilityCommandSymbols compatibility,
            ScaffoldFilterCommandSymbols filters,
            ScaffoldGenerationCommandSymbols generation)
        {
            ConnectionArgument = connection.ConnectionArgument;
            ProviderArgument = connection.ProviderArgument;
            ConnectionOption = connection.ConnectionOption;
            ProviderOption = connection.ProviderOption;
            OutputOption = projectNaming.OutputOption;
            NamespaceOption = projectNaming.NamespaceOption;
            ContextOption = projectNaming.ContextOption;
            ProjectOption = projectNaming.ProjectOption;
            StartupProjectOption = projectNaming.StartupProjectOption;
            ContextDirectoryOption = projectNaming.ContextDirectoryOption;
            ContextNamespaceOption = projectNaming.ContextNamespaceOption;
            FrameworkOption = compatibility.FrameworkOption;
            ConfigurationOption = compatibility.ConfigurationOption;
            RuntimeOption = compatibility.RuntimeOption;
            MsbuildProjectExtensionsPathOption = compatibility.MsbuildProjectExtensionsPathOption;
            NoBuildOption = compatibility.NoBuildOption;
            VerboseOption = compatibility.VerboseOption;
            NoColorOption = compatibility.NoColorOption;
            PrefixOutputOption = compatibility.PrefixOutputOption;
            NoOnConfiguringOption = compatibility.NoOnConfiguringOption;
            DataAnnotationsOption = compatibility.DataAnnotationsOption;
            SchemasOption = filters.SchemasOption;
            SchemaOption = filters.SchemaOption;
            TablesOption = filters.TablesOption;
            TableOption = filters.TableOption;
            NoPluralizeOption = generation.NoPluralizeOption;
            UseDatabaseNamesOption = generation.UseDatabaseNamesOption;
            JsonOption = generation.JsonOption;
            ForceOption = generation.ForceOption;
            NoOverwriteOption = generation.NoOverwriteOption;
            DryRunOption = generation.DryRunOption;
            FailOnWarningsOption = generation.FailOnWarningsOption;
            EmitRoutineStubsOption = generation.EmitRoutineStubsOption;
            EmitSequenceStubsOption = generation.EmitSequenceStubsOption;
            EmitViewEntitiesOption = generation.EmitViewEntitiesOption;
            EmitQueryArtifactsOption = generation.EmitQueryArtifactsOption;
        }

        public Argument<string?> ConnectionArgument { get; }
        public Argument<string?> ProviderArgument { get; }
        public Option<string?> ConnectionOption { get; }
        public Option<string?> ProviderOption { get; }
        public Option<string> OutputOption { get; }
        public Option<string?> NamespaceOption { get; }
        public Option<string?> ContextOption { get; }
        public Option<string?> ProjectOption { get; }
        public Option<string?> StartupProjectOption { get; }
        public Option<string?> FrameworkOption { get; }
        public Option<string?> ConfigurationOption { get; }
        public Option<string?> RuntimeOption { get; }
        public Option<string?> MsbuildProjectExtensionsPathOption { get; }
        public Option<bool> NoBuildOption { get; }
        public Option<bool> JsonOption { get; }
        public Option<bool> VerboseOption { get; }
        public Option<bool> NoColorOption { get; }
        public Option<bool> PrefixOutputOption { get; }
        public Option<string?> ContextDirectoryOption { get; }
        public Option<string?> ContextNamespaceOption { get; }
        public Option<string?> SchemasOption { get; }
        public Option<string[]> SchemaOption { get; }
        public Option<string?> TablesOption { get; }
        public Option<string[]> TableOption { get; }
        public Option<bool> NoPluralizeOption { get; }
        public Option<bool> UseDatabaseNamesOption { get; }
        public Option<bool> NoOnConfiguringOption { get; }
        public Option<bool> DataAnnotationsOption { get; }
        public Option<bool> ForceOption { get; }
        public Option<bool> NoOverwriteOption { get; }
        public Option<bool> DryRunOption { get; }
        public Option<bool> FailOnWarningsOption { get; }
        public Option<bool> EmitRoutineStubsOption { get; }
        public Option<bool> EmitSequenceStubsOption { get; }
        public Option<bool> EmitViewEntitiesOption { get; }
        public Option<bool> EmitQueryArtifactsOption { get; }
    }

    private sealed record ScaffoldConnectionCommandSymbols(
        Argument<string?> ConnectionArgument,
        Argument<string?> ProviderArgument,
        Option<string?> ConnectionOption,
        Option<string?> ProviderOption);

    private sealed record ScaffoldProjectNamingCommandSymbols(
        Option<string> OutputOption,
        Option<string?> NamespaceOption,
        Option<string?> ContextOption,
        Option<string?> ProjectOption,
        Option<string?> StartupProjectOption,
        Option<string?> ContextDirectoryOption,
        Option<string?> ContextNamespaceOption);

    private sealed record ScaffoldCompatibilityCommandSymbols(
        Option<string?> FrameworkOption,
        Option<string?> ConfigurationOption,
        Option<string?> RuntimeOption,
        Option<string?> MsbuildProjectExtensionsPathOption,
        Option<bool> NoBuildOption,
        Option<bool> VerboseOption,
        Option<bool> NoColorOption,
        Option<bool> PrefixOutputOption,
        Option<bool> NoOnConfiguringOption,
        Option<bool> DataAnnotationsOption);

    private sealed record ScaffoldFilterCommandSymbols(
        Option<string?> SchemasOption,
        Option<string[]> SchemaOption,
        Option<string?> TablesOption,
        Option<string[]> TableOption);

    private sealed record ScaffoldGenerationCommandSymbols(
        Option<bool> NoPluralizeOption,
        Option<bool> UseDatabaseNamesOption,
        Option<bool> JsonOption,
        Option<bool> ForceOption,
        Option<bool> NoOverwriteOption,
        Option<bool> DryRunOption,
        Option<bool> FailOnWarningsOption,
        Option<bool> EmitRoutineStubsOption,
        Option<bool> EmitSequenceStubsOption,
        Option<bool> EmitViewEntitiesOption,
        Option<bool> EmitQueryArtifactsOption);
}
