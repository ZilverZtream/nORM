using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

partial class Program
{
    static void AddDepsNativeAssetPaths(
        Dictionary<string, List<string>> pathsByNativeLibrary,
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
                !string.Equals(assetType.GetString(), "native", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!IsNativeLibraryAsset(asset.Name))
                continue;

            var candidates = GetDepsAssetCandidates(depsDirectory, packageRoots, libraryName, libraryVersion, asset.Name);
            foreach (var key in GetNativeLibraryLookupKeys(asset.Name))
            {
                foreach (var candidate in candidates)
                    AddDesignTimeDependencyCandidate(pathsByNativeLibrary, key, candidate);
            }
        }
    }

    static bool IsNativeLibraryAsset(string assetPath)
    {
        var extension = Path.GetExtension(assetPath);
        return extension.Equals(".dll", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".so", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".dylib", StringComparison.OrdinalIgnoreCase);
    }

    static IEnumerable<string> GetNativeLibraryLookupKeys(string assetPath)
    {
        var fileName = Path.GetFileName(assetPath);
        if (!string.IsNullOrWhiteSpace(fileName))
            yield return fileName;

        var stem = Path.GetFileNameWithoutExtension(assetPath);
        if (!string.IsNullOrWhiteSpace(stem))
        {
            yield return stem;
            if (stem.StartsWith("lib", StringComparison.OrdinalIgnoreCase) && stem.Length > 3)
                yield return stem[3..];
        }
    }
}
