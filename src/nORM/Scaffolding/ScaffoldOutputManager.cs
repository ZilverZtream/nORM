#nullable enable
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldOutputManager
    {
        public static async Task EmitAsync(
            string outputDirectory,
            ICollection<(string Path, string Content)> generatedFiles,
            string diagnostics,
            string? diagnosticsJson,
            ScaffoldOptions options)
        {
            if (!string.IsNullOrWhiteSpace(diagnostics))
            {
                generatedFiles.Add((Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"), diagnostics));
                generatedFiles.Add((Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json"), diagnosticsJson ?? "{}"));
            }
            else if (!options.DryRun)
            {
                EnsureNoStaleScaffoldWarningReports(outputDirectory, options);
            }

            if (!options.DryRun)
            {
                EnsureNoOutputFileConflicts(generatedFiles.Select(file => file.Path), options);
                foreach (var (path, content) in generatedFiles)
                    await WriteGeneratedFileAsync(path, content).ConfigureAwait(false);
            }

            if (!string.IsNullOrWhiteSpace(diagnostics) && options.FailOnWarnings)
            {
                throw new NormConfigurationException(
                    "Scaffolding produced warnings for schema features that cannot be emitted as runnable nORM model code. " +
                    (options.DryRun
                        ? "Rerun without ScaffoldOptions.DryRun to write nORM.ScaffoldWarnings.md, or disable ScaffoldOptions.FailOnWarnings."
                        : "Review nORM.ScaffoldWarnings.md or disable ScaffoldOptions.FailOnWarnings."));
            }
        }

    }
}
