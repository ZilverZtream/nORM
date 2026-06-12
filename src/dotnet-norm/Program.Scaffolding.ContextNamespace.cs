using System;
using System.Collections.Generic;
using System.IO;
using nORM.Core;

partial class Program
{
    static string? ResolveScaffoldContextOutputDirectory(string? contextDirectory, ScaffoldProjectInfo? projectInfo)
    {
        if (contextDirectory is null)
            return null;

        if (Path.IsPathRooted(contextDirectory))
            throw new NormConfigurationException("Scaffold --context-dir must be a relative path inside the target project directory or current directory. Use the runtime ScaffoldOptions.ContextOutputDirectory API for explicit absolute context placement.");

        var baseDirectory = projectInfo?.ProjectDirectory ?? Directory.GetCurrentDirectory();
        var fullBase = Path.GetFullPath(baseDirectory);
        var fullTarget = Path.GetFullPath(Path.Combine(fullBase, contextDirectory));
        var relative = Path.GetRelativePath(fullBase, fullTarget);
        if (relative == "." || (!IsParentRelativePath(relative) && !Path.IsPathRooted(relative)))
            return fullTarget;

        throw new NormConfigurationException("Scaffold --context-dir must resolve inside the target project directory or current directory.");
    }

    static (string ContextName, string? ContextNamespace) ResolveScaffoldContextNameAndNamespace(
        string contextName,
        string? explicitContextNamespace,
        string entityNamespace,
        string? contextDirectory,
        string? explicitEntityNamespace,
        ScaffoldProjectInfo? projectInfo)
    {
        var (resolvedContextName, contextNamespaceFromName) = SplitScaffoldContextName(contextName);
        var contextNamespace = NullIfWhiteSpace(explicitContextNamespace)
            ?? contextNamespaceFromName
            ?? ResolveScaffoldContextNamespace(null, entityNamespace, contextDirectory, explicitEntityNamespace, projectInfo);

        return (resolvedContextName, contextNamespace);
    }

    static string? ResolveScaffoldContextNamespace(
        string? explicitContextNamespace,
        string entityNamespace,
        string? contextDirectory,
        string? explicitEntityNamespace,
        ScaffoldProjectInfo? projectInfo)
    {
        var explicitValue = NullIfWhiteSpace(explicitContextNamespace);
        if (explicitValue is not null)
            return explicitValue;

        if (contextDirectory is null)
            return null;

        var explicitEntityValue = NullIfWhiteSpace(explicitEntityNamespace);
        if (explicitEntityValue is not null)
            return explicitEntityValue;

        var baseNamespace = projectInfo is null
            ? entityNamespace
            : FirstNonBlank(projectInfo.DefaultNamespace, "Scaffolded")!;
        return AppendNamespacePath(baseNamespace, GetScaffoldContextNamespaceSegments(contextDirectory, projectInfo));
    }

    static (string ContextName, string? ContextNamespace) SplitScaffoldContextName(string contextName)
    {
        var trimmed = contextName.Trim();
        var splitIndex = trimmed.LastIndexOf('.');
        if (splitIndex < 0)
            return (trimmed, null);

        if (splitIndex == 0 || splitIndex == trimmed.Length - 1)
        {
            throw new NormConfigurationException(
                "Scaffold --context must be a class name or namespace-qualified class name such as 'AppDbContext' or 'MyApp.Data.AppDbContext'.");
        }

        return (trimmed[(splitIndex + 1)..], trimmed[..splitIndex]);
    }

    static IEnumerable<string> GetScaffoldContextNamespaceSegments(string contextDirectory, ScaffoldProjectInfo? projectInfo)
    {
        if (!Path.IsPathRooted(contextDirectory))
            return SplitDirectorySegments(contextDirectory);

        var baseDirectory = projectInfo?.ProjectDirectory ?? Directory.GetCurrentDirectory();
        var fullBase = Path.GetFullPath(baseDirectory);
        var fullContext = Path.GetFullPath(contextDirectory);
        var relative = Path.GetRelativePath(fullBase, fullContext);
        if (relative == "." || IsParentRelativePath(relative) || Path.IsPathRooted(relative))
            return Array.Empty<string>();

        return SplitDirectorySegments(relative);
    }
}
