using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;

partial class Program
{
    static Assembly LoadDesignTimeAssembly(string assemblyPath, string? depsPath = null, string? runtimeConfigPath = null)
    {
        var fullPath = Path.GetFullPath(assemblyPath);
        var loadContext = new DesignTimeAssemblyLoadContext(
            fullPath,
            GetDesignTimeProbeDirectories(fullPath, depsPath, runtimeConfigPath),
            GetDesignTimeDependencyProbePaths(depsPath, runtimeConfigPath));
        return loadContext.LoadFromAssemblyPath(fullPath);
    }

    static IReadOnlyList<string> GetDesignTimeProbeDirectories(string assemblyPath, params string?[] designTimeFiles)
    {
        var directories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddProbeDirectory(Path.GetDirectoryName(assemblyPath));

        foreach (var file in designTimeFiles)
        {
            if (!string.IsNullOrWhiteSpace(file))
                AddProbeDirectory(Path.GetDirectoryName(Path.GetFullPath(file)));
        }

        return directories.ToArray();

        void AddProbeDirectory(string? directory)
        {
            if (!string.IsNullOrWhiteSpace(directory) && Directory.Exists(directory))
                directories.Add(directory);
        }
    }

    static bool TryResolveExistingDesignTimeFile(string? path, string description, out string? fullPath)
    {
        fullPath = null;
        if (string.IsNullOrWhiteSpace(path))
            return true;

        fullPath = Path.GetFullPath(path);
        if (File.Exists(fullPath))
            return true;

        Console.Error.WriteLine($"{description} '{fullPath}' not found.");
        return false;
    }

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

    static string[] BuildDesignTimeArgs(string? environment)
        => string.IsNullOrWhiteSpace(environment)
            ? Array.Empty<string>()
            : new[] { "--environment", environment };

    static SchemaSnapshot BuildMigrationSnapshot(Assembly assembly, bool attributeOnly, string[] designTimeArgs)
    {
        var factory = FindDesignTimeFactory(assembly);
        if (factory != null)
        {
            using var ctx = CreateDesignTimeContext(factory.Value.FactoryType, factory.Value.InterfaceType, designTimeArgs);
            Console.WriteLine($"Using design-time DbContext factory {factory.Value.FactoryType.FullName}.");
            return SchemaSnapshotBuilder.Build(ctx);
        }

        if (attributeOnly)
        {
            Console.WriteLine("Using attribute-only model snapshot.");
            return SchemaSnapshotBuilder.Build(assembly);
        }

        var ctxType = assembly.GetTypes()
            .FirstOrDefault(t => t.IsClass && !t.IsAbstract && typeof(DbContext).IsAssignableFrom(t));
        if (ctxType == null)
        {
            throw new InvalidOperationException(
                "No DbContext type was found. Add an INormDesignTimeDbContextFactory<TContext> implementation " +
                "or re-run with --attribute-only to generate from attributes only.");
        }

        try
        {
            using var modelCn = new SqliteConnection("Data Source=:memory:");
            modelCn.Open();
            var provider = new SqliteProvider();
            using var modelCtx = CreateModelContext(ctxType, modelCn, provider);
            Console.WriteLine($"Using fluent model from {ctxType.Name}.");
            return SchemaSnapshotBuilder.Build(modelCtx);
        }
        catch (Exception ex) when (ex is MissingMethodException or TargetInvocationException or InvalidOperationException or MemberAccessException)
        {
            throw new InvalidOperationException(
                $"Could not instantiate DbContext type '{ctxType.FullName}' for migration generation. " +
                "Add an INormDesignTimeDbContextFactory<TContext> implementation or re-run with --attribute-only " +
                "if you intentionally want to ignore fluent model configuration.",
                ex);
        }
    }

    static DbContext CreateModelContext(Type ctxType, DbConnection connection, DatabaseProvider provider)
    {
        var twoArgConstructor = ctxType.GetConstructor(new[] { typeof(DbConnection), typeof(DatabaseProvider) });
        if (twoArgConstructor != null)
            return (DbContext)twoArgConstructor.Invoke(new object[] { connection, provider });

        var threeArgConstructor = ctxType.GetConstructor(new[] { typeof(DbConnection), typeof(DatabaseProvider), typeof(DbContextOptions) });
        if (threeArgConstructor != null)
            return (DbContext)threeArgConstructor.Invoke(new object?[] { connection, provider, null });

        throw new MissingMethodException(ctxType.FullName, ".ctor(DbConnection, DatabaseProvider[, DbContextOptions])");
    }

    static (Type FactoryType, Type InterfaceType)? FindDesignTimeFactory(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes().Where(static t => t.IsClass && !t.IsAbstract))
        {
            var interfaceType = type.GetInterfaces().FirstOrDefault(static i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(INormDesignTimeDbContextFactory<>));
            if (interfaceType != null)
                return (type, interfaceType);
        }

        return null;
    }

    static DbContext CreateDesignTimeContext(Type factoryType, Type interfaceType, string[] designTimeArgs)
    {
        object factory;
        try
        {
            factory = Activator.CreateInstance(factoryType)
                ?? throw new InvalidOperationException($"Could not create design-time factory '{factoryType.FullName}'.");
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw new InvalidOperationException(
                $"Design-time factory '{factoryType.FullName}' constructor failed: {ex.InnerException.Message}",
                ex.InnerException);
        }

        var method = interfaceType.GetMethod(nameof(INormDesignTimeDbContextFactory<DbContext>.CreateDbContext))
            ?? throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' does not expose CreateDbContext.");
        object? context;
        try
        {
            context = method.Invoke(factory, new object[] { designTimeArgs });
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw new InvalidOperationException(
                $"Design-time factory '{factoryType.FullName}' failed while creating the DbContext: {ex.InnerException.Message}",
                ex.InnerException);
        }

        if (context is null)
            throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' returned null.");

        if (context is not DbContext dbContext)
            throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' did not return a nORM DbContext.");
        return dbContext;
    }

    /// <summary>
    /// Resolves the build output assembly path for a project by running
    /// <c>dotnet msbuild -getProperty:TargetPath</c> on the project file.
    /// Returns null if the path cannot be determined.
    /// </summary>
    static string? ResolveAssemblyFromProject(
        string projectPath,
        string? targetFramework = null,
        string? configuration = null,
        string? runtime = null)
    {
        try
        {
            var startInfo = new System.Diagnostics.ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            startInfo.ArgumentList.Add("msbuild");
            startInfo.ArgumentList.Add(projectPath);
            AddMSBuildProperty(startInfo, "TargetFramework", targetFramework);
            AddMSBuildProperty(startInfo, "Configuration", configuration);
            AddMSBuildProperty(startInfo, "RuntimeIdentifier", runtime);
            startInfo.ArgumentList.Add("-getProperty:TargetPath");
            startInfo.ArgumentList.Add("--verbosity:quiet");

            using var proc = System.Diagnostics.Process.Start(startInfo);
            if (proc == null) return null;
            var output = proc.StandardOutput.ReadToEnd().Trim();
            proc.WaitForExit();
            if (proc.ExitCode == 0 && !string.IsNullOrWhiteSpace(output) && File.Exists(output))
                return output;
            return null;
        }
        catch
        {
            return null;
        }
    }

    static void AddMSBuildProperty(System.Diagnostics.ProcessStartInfo startInfo, string propertyName, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            startInfo.ArgumentList.Add($"-property:{propertyName}={value}");
    }

}
