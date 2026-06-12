using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

partial class Program
{
    static DesignTimeDependencyProbePaths GetDesignTimeDependencyProbePaths(string? depsPath, string? runtimeConfigPath)
    {
        if (string.IsNullOrWhiteSpace(depsPath))
            return DesignTimeDependencyProbePaths.Empty;

        var fullDepsPath = Path.GetFullPath(depsPath);
        var depsDirectory = Path.GetDirectoryName(fullDepsPath) ?? Directory.GetCurrentDirectory();
        var packageRoots = GetDesignTimePackageRoots(runtimeConfigPath).ToArray();
        var pathsByAssembly = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var pathsByNativeLibrary = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(fullDepsPath));
            if (!document.RootElement.TryGetProperty("targets", out var targets) ||
                targets.ValueKind != JsonValueKind.Object)
            {
                return DesignTimeDependencyProbePaths.Empty;
            }

            foreach (var target in targets.EnumerateObject())
            {
                if (target.Value.ValueKind != JsonValueKind.Object)
                    continue;

                foreach (var library in target.Value.EnumerateObject())
                {
                    var (libraryName, libraryVersion) = SplitDepsLibraryName(library.Name);
                    if (library.Value.ValueKind != JsonValueKind.Object)
                        continue;

                    if (library.Value.TryGetProperty("runtime", out var runtimeAssets))
                        AddDepsRuntimeAssetPaths(pathsByAssembly, runtimeAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: false);

                    if (library.Value.TryGetProperty("runtimeTargets", out var runtimeTargetAssets))
                    {
                        AddDepsRuntimeAssetPaths(pathsByAssembly, runtimeTargetAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: true);
                        AddDepsNativeAssetPaths(pathsByNativeLibrary, runtimeTargetAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: true);
                    }

                    if (library.Value.TryGetProperty("native", out var nativeAssets))
                        AddDepsNativeAssetPaths(pathsByNativeLibrary, nativeAssets, depsDirectory, packageRoots, libraryName, libraryVersion, filterRuntimeTargets: false);
                }
            }
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException($"Could not read design-time deps file '{fullDepsPath}': {ex.Message}", ex);
        }

        return new DesignTimeDependencyProbePaths(
            ToReadOnlyPathDictionary(pathsByAssembly),
            ToReadOnlyPathDictionary(pathsByNativeLibrary));
    }

    static IReadOnlyDictionary<string, IReadOnlyList<string>> ToReadOnlyPathDictionary(Dictionary<string, List<string>> pathsByKey)
        => pathsByKey.ToDictionary(
            static pair => pair.Key,
            static pair => (IReadOnlyList<string>)pair.Value,
            StringComparer.OrdinalIgnoreCase);

    static (string? Name, string? Version) SplitDepsLibraryName(string libraryName)
    {
        var separator = libraryName.LastIndexOf('/');
        if (separator <= 0 || separator == libraryName.Length - 1)
            return (NullIfWhiteSpace(libraryName), null);

        return (NullIfWhiteSpace(libraryName[..separator]), NullIfWhiteSpace(libraryName[(separator + 1)..]));
    }

    static void AddDepsRuntimeAssetPaths(
        Dictionary<string, List<string>> pathsByAssembly,
        JsonElement assets,
        string depsDirectory,
        IReadOnlyCollection<string> packageRoots,
        string? libraryName,
        string? libraryVersion,
        bool filterRuntimeTargets)
    {
        if (assets.ValueKind != JsonValueKind.Object)
            return;

        foreach (var asset in assets.EnumerateObject())
        {
            if (filterRuntimeTargets &&
                asset.Value.ValueKind == JsonValueKind.Object &&
                asset.Value.TryGetProperty("assetType", out var assetType) &&
                assetType.ValueKind == JsonValueKind.String &&
                !string.Equals(assetType.GetString(), "runtime", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!asset.Name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                continue;

            var assemblyName = Path.GetFileNameWithoutExtension(asset.Name);
            if (string.IsNullOrWhiteSpace(assemblyName))
                continue;

            var candidates = GetDepsAssetCandidates(depsDirectory, packageRoots, libraryName, libraryVersion, asset.Name);
            foreach (var candidate in candidates)
                AddDesignTimeDependencyCandidate(pathsByAssembly, assemblyName, candidate);
        }
    }

    static IEnumerable<string> GetDepsAssetCandidates(
        string depsDirectory,
        IReadOnlyCollection<string> packageRoots,
        string? libraryName,
        string? libraryVersion,
        string assetPath)
    {
        var normalizedAssetPath = assetPath.Replace('/', Path.DirectorySeparatorChar);
        yield return Path.GetFullPath(Path.Combine(depsDirectory, normalizedAssetPath));

        if (!string.IsNullOrWhiteSpace(libraryName) && !string.IsNullOrWhiteSpace(libraryVersion))
        {
            yield return Path.GetFullPath(Path.Combine(depsDirectory, libraryName, libraryVersion, normalizedAssetPath));

            foreach (var packageRoot in packageRoots)
                yield return Path.GetFullPath(Path.Combine(packageRoot, libraryName.ToLowerInvariant(), libraryVersion.ToLowerInvariant(), normalizedAssetPath));
        }
    }

    static void AddDesignTimeDependencyCandidate(
        Dictionary<string, List<string>> pathsByAssembly,
        string assemblyName,
        string candidate)
    {
        if (!pathsByAssembly.TryGetValue(assemblyName, out var paths))
        {
            paths = new List<string>();
            pathsByAssembly[assemblyName] = paths;
        }

        if (!paths.Contains(candidate, StringComparer.OrdinalIgnoreCase))
            paths.Add(candidate);
    }
}
