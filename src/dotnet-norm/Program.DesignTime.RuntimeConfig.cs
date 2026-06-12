using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

partial class Program
{
    static IEnumerable<string> GetDesignTimePackageRoots(string? runtimeConfigPath)
    {
        var roots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var path in GetRuntimeConfigAdditionalProbingPaths(runtimeConfigPath))
            if (roots.Add(path))
                yield return path;

        foreach (var path in GetNuGetPackageRoots())
            if (roots.Add(path))
                yield return path;
    }

    static IEnumerable<string> GetRuntimeConfigAdditionalProbingPaths(string? runtimeConfigPath)
    {
        if (string.IsNullOrWhiteSpace(runtimeConfigPath))
            yield break;

        foreach (var configPath in EnumerateRuntimeConfigProbeFiles(Path.GetFullPath(runtimeConfigPath)))
        {
            if (!File.Exists(configPath))
                continue;

            string? configDirectory = Path.GetDirectoryName(configPath);
            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            if (!document.RootElement.TryGetProperty("runtimeOptions", out var runtimeOptions) ||
                runtimeOptions.ValueKind != JsonValueKind.Object ||
                !runtimeOptions.TryGetProperty("additionalProbingPaths", out var additionalProbingPaths) ||
                additionalProbingPaths.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            foreach (var path in additionalProbingPaths.EnumerateArray())
            {
                if (path.ValueKind != JsonValueKind.String)
                    continue;

                var value = NullIfWhiteSpace(Environment.ExpandEnvironmentVariables(path.GetString() ?? string.Empty));
                if (value is null)
                    continue;

                yield return Path.GetFullPath(Path.IsPathFullyQualified(value)
                    ? value
                    : Path.Combine(configDirectory ?? Directory.GetCurrentDirectory(), value));
            }
        }
    }

    static IEnumerable<string> EnumerateRuntimeConfigProbeFiles(string runtimeConfigPath)
    {
        yield return runtimeConfigPath;

        const string runtimeConfigSuffix = ".runtimeconfig.json";
        if (runtimeConfigPath.EndsWith(runtimeConfigSuffix, StringComparison.OrdinalIgnoreCase))
            yield return runtimeConfigPath[..^runtimeConfigSuffix.Length] + ".runtimeconfig.dev.json";
    }

    static IEnumerable<string> GetNuGetPackageRoots()
    {
        var configuredRoot = Environment.GetEnvironmentVariable("NUGET_PACKAGES");
        if (!string.IsNullOrWhiteSpace(configuredRoot))
            yield return configuredRoot;

        var userProfile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if (!string.IsNullOrWhiteSpace(userProfile))
            yield return Path.Combine(userProfile, ".nuget", "packages");
    }
}
