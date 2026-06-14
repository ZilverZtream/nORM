using System.CommandLine;
using nORM.Scaffolding;

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
        public required Option<string?> MsbuildProjectExtensionsPathOption { get; init; }
        public required Option<bool> NoBuildOption { get; init; }
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
        public required Option<bool> NoOnConfiguringOption { get; init; }
        public required Option<bool> DataAnnotationsOption { get; init; }
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
}
