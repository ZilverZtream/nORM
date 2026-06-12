using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

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
}
