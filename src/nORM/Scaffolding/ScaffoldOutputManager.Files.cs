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
        public static void EnsureNoOutputFileConflicts(IEnumerable<string> paths, ScaffoldOptions options)
        {
            if (options.OverwriteFiles)
                return;

            foreach (var path in paths)
            {
                if (File.Exists(path))
                {
                    throw new NormConfigurationException(
                        $"Scaffolding output file already exists: '{path}'. Enable overwrite or choose an empty output directory.");
                }
            }
        }

        public static void EnsureNoStaleScaffoldWarningReports(string outputDirectory, ScaffoldOptions options)
        {
            var warningPaths = new[]
            {
                Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"),
                Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json")
            };
            var existingWarnings = warningPaths.Where(File.Exists).ToArray();
            if (existingWarnings.Length == 0)
                return;

            if (!options.OverwriteFiles)
            {
                throw new NormConfigurationException(
                    "Scaffolding produced no warnings, but stale scaffold warning report files already exist: " +
                    string.Join(", ", existingWarnings.Select(path => $"'{path}'")) +
                    ". Enable overwrite or remove the stale report files before running with overwrite protection.");
            }

            foreach (var path in existingWarnings)
                File.Delete(path);
        }

        public static async Task WriteGeneratedFileAsync(string path, string content)
        {
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            await File.WriteAllTextAsync(path, content).ConfigureAwait(false);
        }
    }
}
