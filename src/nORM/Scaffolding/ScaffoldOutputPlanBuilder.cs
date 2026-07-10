#nullable enable
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Database scaffolding emits dynamic entity types and traverses live mapping metadata; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Database scaffolding reflects over provider and entity metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal static partial class ScaffoldOutputPlanBuilder
    {
        public static async Task<ScaffoldOutputPlan> BuildAsync(
            ScaffoldOutputPlanRequest request)
        {
            var entityFiles = await BuildEntityFilesAsync(request).ConfigureAwait(false);
            var generatedFiles = entityFiles.GeneratedFiles.ToList();
            generatedFiles.Add(BuildContextFile(request, entityFiles));

            var diagnostics = BuildDiagnostics(request);
            var diagnosticsJson = BuildDiagnosticsJson(request, diagnostics);

            return new ScaffoldOutputPlan(generatedFiles, diagnostics, diagnosticsJson);
        }

        private static Task<ScaffoldEntityFileSet> BuildEntityFilesAsync(
            ScaffoldOutputPlanRequest request)
            => ScaffoldEntityFileAdapter.BuildScaffoldEntityFilesAsync(new ScaffoldEntityFileSetRequest(
                request.Connection,
                request.Provider,
                request.OutputDirectory,
                request.NamespaceName,
                request.Discovery,
                request.Composition,
                request.Options));
    }

    internal sealed record ScaffoldOutputPlan(
        List<(string Path, string Content)> GeneratedFiles,
        string Diagnostics,
        string? DiagnosticsJson);
}
