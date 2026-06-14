using System.Collections.Generic;

partial class Program
{
    sealed record EfToolConfig(
        string? Project,
        string? StartupProject,
        string? OutputDir,
        string? Namespace,
        string? Context,
        string? ContextDir,
        string? ContextNamespace,
        IReadOnlyList<string> Schemas,
        IReadOnlyList<string> Tables,
        string? Framework,
        string? Configuration,
        string? Runtime,
        string? MsbuildProjectExtensionsPath,
        bool? NoBuild,
        bool? Json,
        bool? Verbose,
        bool? NoColor,
        bool? PrefixOutput,
        bool? NoPluralize,
        bool? UseDatabaseNames,
        bool? NoOnConfiguring,
        bool? DataAnnotations,
        bool? Force,
        bool? NoOverwrite,
        bool? DryRun,
        bool? FailOnWarnings,
        bool? EmitRoutineStubs,
        bool? EmitSequenceStubs,
        bool? EmitViewEntities,
        bool? EmitQueryArtifacts);
}
