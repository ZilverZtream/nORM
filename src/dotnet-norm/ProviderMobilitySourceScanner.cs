using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace nORM.Cli;

public sealed record ProviderMobilityScanResult(string RootPath, int ScannedFiles, IReadOnlyList<ProviderMobilityFinding> Findings);

public static partial class ProviderMobilitySourceScanner
{
    public static ProviderMobilityScanResult Scan(string rootPath)
    {
        var fullRoot = Path.GetFullPath(rootPath);
        if (!Directory.Exists(fullRoot) && !File.Exists(fullRoot))
        {
            return new ProviderMobilityScanResult(fullRoot, 0, new[]
            {
                new ProviderMobilityFinding(
                    fullRoot,
                    0,
                    "scan-path-missing",
                    "Error",
                    "The requested portability scan path does not exist.",
                    "Pass a valid application source directory or file.")
            });
        }

        var files = File.Exists(fullRoot)
            ? new[] { fullRoot }
            : Directory.EnumerateFiles(fullRoot, "*.*", SearchOption.AllDirectories)
                .Where(path => !IsGeneratedOrBuildOutput(fullRoot, path))
                .Where(static path =>
                    path.EndsWith(".cs", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".props", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".targets", StringComparison.OrdinalIgnoreCase))
                .ToArray();

        var findings = new List<ProviderMobilityFinding>();
        foreach (var file in files)
            ScanFile(fullRoot, file, findings);

        return new ProviderMobilityScanResult(fullRoot, files.Length, findings);
    }

    private static bool IsGeneratedOrBuildOutput(string root, string path)
    {
        var normalized = Path.GetRelativePath(root, path).Replace('\\', '/');
        return normalized.StartsWith("bin/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/bin/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("obj/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/obj/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("artifacts/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/artifacts/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith(".git/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/.git/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith(".tmp/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/.tmp/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("node_modules/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/node_modules/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("packages/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/packages/", StringComparison.OrdinalIgnoreCase) ||
               normalized.EndsWith(".g.cs", StringComparison.OrdinalIgnoreCase) ||
               normalized.EndsWith(".designer.cs", StringComparison.OrdinalIgnoreCase);
    }
}
