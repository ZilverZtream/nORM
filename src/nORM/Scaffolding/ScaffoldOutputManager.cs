#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static class ScaffoldOutputManager
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

        public static string NormalizeContextNamespace(string entityNamespaceName, string? contextNamespaceName)
        {
            if (string.IsNullOrWhiteSpace(contextNamespaceName))
                return entityNamespaceName;

            var trimmed = contextNamespaceName.Trim();
            if (!ScaffoldNameHelper.IsValidNamespaceName(trimmed))
            {
                throw new NormConfigurationException(
                    $"Scaffold context namespace '{trimmed}' is not a valid C# namespace. " +
                    "Use a dot-separated namespace such as 'MyApp.Data.Contexts'.");
            }

            return trimmed;
        }

        public static string ResolveContextOutputDirectory(
            string outputDirectory,
            string? contextDirectory,
            string? contextOutputDirectory)
        {
            var normalizedContextDirectory = NormalizeContextDirectory(contextDirectory);
            var normalizedContextOutputDirectory = NormalizeContextOutputDirectory(contextOutputDirectory);
            if (normalizedContextDirectory is not null && normalizedContextOutputDirectory is not null)
            {
                throw new NormConfigurationException(
                    "Use either ScaffoldOptions.ContextDirectory or ScaffoldOptions.ContextOutputDirectory, not both.");
            }

            return normalizedContextOutputDirectory
                ?? (normalizedContextDirectory is null
                    ? outputDirectory
                    : Path.Combine(outputDirectory, normalizedContextDirectory));
        }

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

        private static string? NormalizeContextDirectory(string? contextDirectory)
        {
            if (string.IsNullOrWhiteSpace(contextDirectory))
                return null;

            var trimmed = contextDirectory.Trim();
            if (Path.IsPathRooted(trimmed))
            {
                throw new NormConfigurationException(
                    "Scaffold context directory must be a relative path below the output directory.");
            }

            var separators = new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar, '/', '\\' };
            var segments = trimmed
                .Split(separators, StringSplitOptions.RemoveEmptyEntries)
                .Select(segment => segment.Trim())
                .ToArray();

            if (segments.Length == 0
                || segments.Any(segment =>
                    segment.Length == 0
                    || segment == "."
                    || segment == ".."
                    || segment.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0))
            {
                throw new NormConfigurationException(
                    "Scaffold context directory must contain only relative child directory segments.");
            }

            return Path.Combine(segments);
        }

        private static string? NormalizeContextOutputDirectory(string? contextOutputDirectory)
        {
            if (string.IsNullOrWhiteSpace(contextOutputDirectory))
                return null;

            var trimmed = contextOutputDirectory.Trim();
            if (!Path.IsPathRooted(trimmed))
            {
                throw new NormConfigurationException(
                    "Scaffold context output directory must be an absolute path. Use ContextDirectory for output-relative context placement.");
            }

            return Path.GetFullPath(trimmed);
        }
    }
}
