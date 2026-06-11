using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using nORM.Core;

partial class Program
{
    static string ResolveScaffoldNamespace(string? explicitNamespace, ScaffoldProjectInfo? projectInfo, string outputDirectory)
    {
        var explicitValue = NullIfWhiteSpace(explicitNamespace);
        if (explicitValue is not null)
            return explicitValue;

        if (projectInfo is null)
            return "Scaffolded";

        var baseNamespace = FirstNonBlank(projectInfo.DefaultNamespace, "Scaffolded")!;
        return AppendNamespacePath(baseNamespace, GetRelativeDirectorySegments(projectInfo.ProjectDirectory, outputDirectory));
    }

    static string ValidateScaffoldNamespaceName(string namespaceName, string source)
    {
        if (IsValidScaffoldNamespaceName(namespaceName))
            return namespaceName;

        throw new NormConfigurationException(
            $"Scaffold {source} '{namespaceName}' is not a valid C# namespace. Use a dot-separated namespace such as 'MyApp.Data'.");
    }

    static string ValidateScaffoldContextClassName(string contextClassName)
    {
        if (IsValidScaffoldIdentifier(contextClassName))
            return contextClassName;

        throw new NormConfigurationException(
            $"Scaffold --context class name '{contextClassName}' is not a valid C# type identifier. Use a class name such as 'AppDbContext' or a namespace-qualified value such as 'MyApp.Data.AppDbContext'.");
    }

    static bool IsValidScaffoldNamespaceName(string namespaceName)
    {
        if (string.IsNullOrWhiteSpace(namespaceName))
            return false;

        return namespaceName.Split('.').All(IsValidScaffoldNamespaceSegment);
    }

    static bool IsValidScaffoldNamespaceSegment(string segment)
        => IsValidScaffoldIdentifier(segment);

    static bool IsValidScaffoldIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var start = value[0] == '@' ? 1 : 0;
        if (start == value.Length)
            return false;

        if (!(char.IsLetter(value[start]) || value[start] == '_'))
            return false;

        for (var i = start + 1; i < value.Length; i++)
        {
            if (!(char.IsLetterOrDigit(value[i]) || value[i] == '_'))
                return false;
        }

        return start != 0 || !IsCSharpKeyword(value);
    }

    static bool IsCSharpKeyword(string value)
        => value is
            "abstract" or "as" or "base" or "bool" or "break" or "byte" or "case" or "catch" or "char" or "checked" or
            "class" or "const" or "continue" or "decimal" or "default" or "delegate" or "do" or "double" or "else" or
            "enum" or "event" or "explicit" or "extern" or "false" or "finally" or "fixed" or "float" or "for" or
            "foreach" or "goto" or "if" or "implicit" or "in" or "int" or "interface" or "internal" or "is" or "lock" or
            "long" or "namespace" or "new" or "null" or "object" or "operator" or "out" or "override" or "params" or
            "private" or "protected" or "public" or "readonly" or "ref" or "return" or "sbyte" or "sealed" or "short" or
            "sizeof" or "stackalloc" or "static" or "string" or "struct" or "switch" or "this" or "throw" or "true" or
            "try" or "typeof" or "uint" or "ulong" or "unchecked" or "unsafe" or "ushort" or "using" or "virtual" or
            "void" or "volatile" or "while" or
            "record" or "partial" or "var" or "dynamic" or "async" or "await" or "nameof" or "when" or "and" or "or" or
            "not" or "with" or "init" or "required" or "file" or "scoped" or "global" or "managed" or "unmanaged" or
            "nint" or "nuint" or "value" or "yield";

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

    static string AppendNamespacePath(string baseNamespace, IEnumerable<string> pathSegments)
    {
        var namespaceSegments = NormalizeNamespacePathSegments(pathSegments).ToArray();
        return namespaceSegments.Length == 0
            ? baseNamespace
            : baseNamespace + "." + string.Join(".", namespaceSegments);
    }

    static IEnumerable<string> GetRelativeDirectorySegments(string rootDirectory, string targetDirectory)
    {
        var fullRoot = Path.GetFullPath(rootDirectory);
        var fullTarget = Path.GetFullPath(targetDirectory);
        var relative = Path.GetRelativePath(fullRoot, fullTarget);
        if (relative == "." || IsParentRelativePath(relative) || Path.IsPathRooted(relative))
            return Array.Empty<string>();

        return SplitDirectorySegments(relative);
    }

    static IEnumerable<string> SplitDirectorySegments(string path)
        => path.Split(new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(static segment => segment is not "." and not "..");

    static bool IsParentRelativePath(string relative)
        => relative == ".."
           || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
           || relative.StartsWith(".." + Path.AltDirectorySeparatorChar, StringComparison.Ordinal);

    static IEnumerable<string> NormalizeNamespacePathSegments(IEnumerable<string> segments)
        => segments
            .Select(NormalizeProjectNamespace)
            .Where(static segment => segment is not null)
            .SelectMany(static segment => segment!.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

    static string? NormalizeProjectNamespace(string? value)
    {
        var trimmed = NullIfWhiteSpace(value);
        if (trimmed is null)
            return null;

        var parts = trimmed.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ToCSharpIdentifier)
            .Where(static segment => !string.IsNullOrWhiteSpace(segment))
            .ToArray();
        return parts.Length == 0 ? null : string.Join(".", parts);
    }
}
