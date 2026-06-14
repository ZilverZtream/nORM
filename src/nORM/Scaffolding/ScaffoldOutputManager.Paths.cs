#nullable enable
using System;
using System.IO;
using System.Linq;
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldOutputManager
    {
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
