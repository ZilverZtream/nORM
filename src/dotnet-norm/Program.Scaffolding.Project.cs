using System;
using System.IO;
using System.Linq;
using nORM.Core;

partial class Program
{
    static ScaffoldProjectInfo? ResolveScaffoldProject(string? projectPath, bool inferCurrentDirectory = false)
    {
        if (string.IsNullOrWhiteSpace(projectPath))
            return inferCurrentDirectory ? TryResolveCurrentDirectoryScaffoldProject() : null;

        var fullPath = Path.GetFullPath(projectPath);
        string projectFile;
        if (Directory.Exists(fullPath))
        {
            var candidates = Directory.GetFiles(fullPath, "*.csproj", SearchOption.TopDirectoryOnly)
                .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            projectFile = candidates.Length switch
            {
                0 => throw new NormConfigurationException($"Scaffold --project directory '{fullPath}' does not contain a .csproj file."),
                1 => candidates[0],
                _ => throw new NormConfigurationException($"Scaffold --project directory '{fullPath}' contains multiple .csproj files. Pass the target project file explicitly.")
            };
        }
        else
        {
            if (!File.Exists(fullPath))
                throw new NormConfigurationException($"Scaffold --project path '{fullPath}' does not exist.");
            if (!string.Equals(Path.GetExtension(fullPath), ".csproj", StringComparison.OrdinalIgnoreCase))
                throw new NormConfigurationException("Scaffold --project must point to a .csproj file or a directory containing exactly one .csproj file.");
            projectFile = fullPath;
        }

        return CreateScaffoldProjectInfo(projectFile);
    }

    static ScaffoldProjectInfo? TryResolveCurrentDirectoryScaffoldProject()
    {
        var currentDirectory = Directory.GetCurrentDirectory();
        var candidates = Directory.GetFiles(currentDirectory, "*.csproj", SearchOption.TopDirectoryOnly)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return candidates.Length == 1
            ? CreateScaffoldProjectInfo(candidates[0])
            : null;
    }

    static ScaffoldProjectInfo CreateScaffoldProjectInfo(string projectFile)
    {
        var projectDirectory = Path.GetDirectoryName(projectFile)!;
        return new ScaffoldProjectInfo(
            projectFile,
            projectDirectory,
            ReadProjectDefaultNamespace(projectFile),
            ReadProjectUserSecretsId(projectFile),
            ReadProjectUseNullableReferenceTypes(projectFile));
    }

    static string ResolveScaffoldOutputPath(string output, ScaffoldProjectInfo? projectInfo)
    {
        if (projectInfo is null || Path.IsPathRooted(output))
            return output;

        return Path.Combine(projectInfo.ProjectDirectory, output);
    }

    sealed record ScaffoldProjectInfo(string ProjectFile, string ProjectDirectory, string? DefaultNamespace, string? UserSecretsId, bool UseNullableReferenceTypes);
}
