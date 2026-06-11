using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;

sealed record DesignTimeDependencyProbePaths(
    IReadOnlyDictionary<string, IReadOnlyList<string>> Assemblies,
    IReadOnlyDictionary<string, IReadOnlyList<string>> NativeLibraries)
{
    public static DesignTimeDependencyProbePaths Empty { get; } = new(
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase));
}

sealed class DesignTimeEnvironmentScope : IDisposable
{
    private readonly bool _changed;
    private readonly string? _previousAspNetCoreEnvironment;
    private readonly string? _previousDotnetEnvironment;

    private DesignTimeEnvironmentScope(string? environment)
    {
        if (string.IsNullOrWhiteSpace(environment))
            return;

        _changed = true;
        _previousAspNetCoreEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        _previousDotnetEnvironment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", environment);
        Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", environment);
    }

    public static DesignTimeEnvironmentScope Apply(string? environment) => new(environment);

    public void Dispose()
    {
        if (!_changed)
            return;

        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", _previousAspNetCoreEnvironment);
        Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", _previousDotnetEnvironment);
    }
}

sealed class DesignTimeAssemblyLoadContext : AssemblyLoadContext
{
    private readonly AssemblyDependencyResolver _resolver;
    private readonly IReadOnlyList<string> _probeDirectories;
    private readonly DesignTimeDependencyProbePaths _dependencyProbePaths;

    public DesignTimeAssemblyLoadContext(
        string mainAssemblyPath,
        IReadOnlyList<string> probeDirectories,
        DesignTimeDependencyProbePaths dependencyProbePaths)
    {
        _resolver = new AssemblyDependencyResolver(mainAssemblyPath);
        _probeDirectories = probeDirectories;
        _dependencyProbePaths = dependencyProbePaths;
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var sharedAssembly = AssemblyLoadContext.Default.Assemblies.FirstOrDefault(assembly =>
            AssemblyName.ReferenceMatchesDefinition(assembly.GetName(), assemblyName));
        if (sharedAssembly != null)
            return sharedAssembly;

        var assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
        assemblyPath ??= ResolveAssemblyFromProbeDirectories(assemblyName);
        return assemblyPath == null ? null : LoadFromAssemblyPath(assemblyPath);
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        var libraryPath = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        libraryPath ??= ResolveNativeLibraryFromDeps(unmanagedDllName);
        return libraryPath == null ? IntPtr.Zero : LoadUnmanagedDllFromPath(libraryPath);
    }

    private string? ResolveAssemblyFromProbeDirectories(AssemblyName assemblyName)
    {
        if (string.IsNullOrWhiteSpace(assemblyName.Name))
            return null;

        if (_dependencyProbePaths.Assemblies.TryGetValue(assemblyName.Name, out var dependencyPaths))
        {
            foreach (var dependencyPath in dependencyPaths)
            {
                if (File.Exists(dependencyPath))
                    return dependencyPath;
            }
        }

        foreach (var directory in _probeDirectories)
        {
            var candidate = Path.Combine(directory, assemblyName.Name + ".dll");
            if (File.Exists(candidate))
                return candidate;

            var refsCandidate = Path.Combine(directory, "refs", assemblyName.Name + ".dll");
            if (File.Exists(refsCandidate))
                return refsCandidate;
        }

        return null;
    }

    private string? ResolveNativeLibraryFromDeps(string unmanagedDllName)
    {
        foreach (var key in GetNativeLibraryResolverKeys(unmanagedDllName))
        {
            if (!_dependencyProbePaths.NativeLibraries.TryGetValue(key, out var dependencyPaths))
                continue;

            foreach (var dependencyPath in dependencyPaths)
            {
                if (File.Exists(dependencyPath))
                    return dependencyPath;
            }
        }

        return null;
    }

    private static IEnumerable<string> GetNativeLibraryResolverKeys(string unmanagedDllName)
    {
        yield return unmanagedDllName;

        var fileName = Path.GetFileName(unmanagedDllName);
        if (!string.IsNullOrWhiteSpace(fileName) &&
            !string.Equals(fileName, unmanagedDllName, StringComparison.OrdinalIgnoreCase))
        {
            yield return fileName;
        }

        var stem = Path.GetFileNameWithoutExtension(unmanagedDllName);
        if (!string.IsNullOrWhiteSpace(stem) &&
            !string.Equals(stem, unmanagedDllName, StringComparison.OrdinalIgnoreCase))
        {
            yield return stem;
        }
    }
}