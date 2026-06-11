using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
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

    static string InferScaffoldContextName(string connectionString, string connectionReference, string provider)
    {
        var baseName = TryGetNamedConnectionLeaf(connectionReference)
                       ?? TryGetScaffoldDatabaseName(connectionString, provider)
                       ?? "AppDb";
        var identifier = ToPascalIdentifier(baseName);
        return identifier.EndsWith("Context", StringComparison.Ordinal)
            ? identifier
            : identifier + "Context";
    }

    static string? TryGetScaffoldDatabaseName(string connectionString, string provider)
    {
        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            var normalizedProvider = NormalizeProviderName(provider);
            if (normalizedProvider == "sqlite")
            {
                var dataSource = TryGetConnectionStringValue(builder, "Data Source", "DataSource", "Filename");
                if (dataSource is null || dataSource == ":memory:")
                    return null;

                var fileName = Path.GetFileNameWithoutExtension(dataSource);
                return NullIfWhiteSpace(fileName);
            }

            return TryGetConnectionStringValue(builder, "Database", "Initial Catalog");
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or InvalidOperationException)
        {
            return null;
        }
    }

    static string? TryGetConnectionStringValue(DbConnectionStringBuilder builder, params string[] keys)
    {
        foreach (var key in keys)
        {
            foreach (string existingKey in builder.Keys)
            {
                if (existingKey.Equals(key, StringComparison.OrdinalIgnoreCase))
                    return NullIfWhiteSpace(builder[existingKey]?.ToString());
            }
        }

        return null;
    }

    static string? TryGetNamedConnectionLeaf(string connectionReference)
    {
        if (!IsNamedConnectionReference(connectionReference))
            return null;

        var name = NullIfWhiteSpace(connectionReference.Trim()["Name=".Length..]);
        if (name is null)
            return null;

        var leaf = name.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).LastOrDefault();
        return NullIfWhiteSpace(leaf);
    }

    static string ToPascalIdentifier(string value)
    {
        var builder = new StringBuilder(value.Length);
        var nextUpper = true;
        foreach (var ch in value)
        {
            if (!char.IsLetterOrDigit(ch))
            {
                nextUpper = true;
                continue;
            }

            builder.Append(nextUpper ? char.ToUpperInvariant(ch) : ch);
            nextUpper = false;
        }

        return ToCSharpIdentifier(builder.Length == 0 ? "AppDb" : builder.ToString());
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

    static string? ReadProjectDefaultNamespace(string projectFile)
    {
        try
        {
            var inheritedRootNamespace = ReadNearestDirectoryBuildPropsProperty(projectFile, "RootNamespace", NormalizeProjectNamespace);
            var inheritedAssemblyName = ReadNearestDirectoryBuildPropsProperty(projectFile, "AssemblyName", NormalizeProjectNamespace);
            var document = XDocument.Load(projectFile);
            var propertyGroups = GetPropertyGroups(document);
            var rootNamespace = propertyGroups
                .Elements()
                .Where(static element => element.Name.LocalName == "RootNamespace")
                .Select(static element => NormalizeProjectNamespace(element.Value))
                .LastOrDefault(static value => value is not null);
            if (rootNamespace is not null)
                return rootNamespace;
            if (inheritedRootNamespace is not null)
                return inheritedRootNamespace;

            var assemblyName = propertyGroups
                .Elements()
                .Where(static element => element.Name.LocalName == "AssemblyName")
                .Select(static element => NormalizeProjectNamespace(element.Value))
                .LastOrDefault(static value => value is not null);
            if (assemblyName is not null)
                return assemblyName;
            if (inheritedAssemblyName is not null)
                return inheritedAssemblyName;

            return NormalizeProjectNamespace(Path.GetFileNameWithoutExtension(projectFile));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or XmlException)
        {
            throw new NormConfigurationException($"Could not read scaffold --project file '{projectFile}': {ex.Message}", ex);
        }
    }

    static string? ReadProjectUserSecretsId(string projectFile)
    {
        try
        {
            var inheritedUserSecretsId = ReadNearestDirectoryBuildPropsProperty(projectFile, "UserSecretsId", NullIfWhiteSpace);
            var document = XDocument.Load(projectFile);
            return GetPropertyGroups(document)
                .Elements()
                .Where(static element => element.Name.LocalName == "UserSecretsId")
                .Select(static element => NullIfWhiteSpace(element.Value))
                .LastOrDefault(static value => value is not null)
                ?? inheritedUserSecretsId;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or XmlException)
        {
            throw new NormConfigurationException($"Could not read scaffold --project file '{projectFile}': {ex.Message}", ex);
        }
    }

    static bool ReadProjectUseNullableReferenceTypes(string projectFile)
    {
        try
        {
            var inheritedNullable = ReadNearestDirectoryBuildPropsProperty(projectFile, "Nullable", NullIfWhiteSpace);
            var projectNullable = ReadNullableProperty(projectFile);
            return IsNullableReferenceTypesEnabled(projectNullable ?? inheritedNullable);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or XmlException)
        {
            throw new NormConfigurationException($"Could not read scaffold nullable settings for project '{projectFile}': {ex.Message}", ex);
        }
    }

    static string? ReadNearestDirectoryBuildPropsProperty(string projectFile, string propertyName, Func<string?, string?> normalize)
    {
        for (var directory = Path.GetDirectoryName(projectFile); !string.IsNullOrWhiteSpace(directory); directory = Directory.GetParent(directory)?.FullName)
        {
            var propsPath = Path.Combine(directory, "Directory.Build.props");
            if (File.Exists(propsPath))
                return ReadProjectProperty(propsPath, propertyName, normalize);
        }

        return null;
    }

    static string? ReadNullableProperty(string projectOrPropsFile)
        => ReadProjectProperty(projectOrPropsFile, "Nullable", NullIfWhiteSpace);

    static string? ReadProjectProperty(string projectOrPropsFile, string propertyName, Func<string?, string?> normalize)
    {
        var document = XDocument.Load(projectOrPropsFile);
        return GetPropertyGroups(document)
            .Elements()
            .Where(element => element.Name.LocalName == propertyName)
            .Select(element => normalize(element.Value))
            .LastOrDefault(static value => value is not null);
    }

    static IEnumerable<XElement> GetPropertyGroups(XDocument document)
        => document.Root?.Elements()
            .Where(static element => element.Name.LocalName == "PropertyGroup")
            ?? Enumerable.Empty<XElement>();

    static bool IsNullableReferenceTypesEnabled(string? nullableValue)
    {
        var normalized = NullIfWhiteSpace(nullableValue)?.Trim().ToLowerInvariant();
        return normalized is "enable" or "annotations";
    }

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


    sealed record ScaffoldProjectInfo(string ProjectFile, string ProjectDirectory, string? DefaultNamespace, string? UserSecretsId, bool UseNullableReferenceTypes);
}
