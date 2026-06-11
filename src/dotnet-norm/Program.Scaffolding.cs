using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using nORM.Core;

partial class Program
{
    static string? GetOptionalNonBlankScaffoldOption(ParseResult result, Option<string?> option, string optionName)
    {
        var value = result.GetValue(option);
        if (result.GetResult(option) is not null && string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"Scaffold {optionName} must not be blank.");

        return NullIfWhiteSpace(value);
    }

    static string GetRequiredNonBlankScaffoldOption(ParseResult result, Option<string> option, string optionName)
    {
        var value = result.GetValue(option);
        if (string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"Scaffold {optionName} must not be blank.");

        return value;
    }

    static void ValidateScaffoldUnmatchedTokens(ParseResult result)
    {
        if (result.UnmatchedTokens.Count == 0)
            return;

        var commandLineArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
        if (AreEfPassThroughTokens(result.UnmatchedTokens, commandLineArgs))
            return;

        throw new NormConfigurationException(
            "Unrecognized scaffold argument(s): " + string.Join(" ", result.UnmatchedTokens) +
            ". EF-style application arguments are accepted only after '--'; '--environment' is used for named-connection appsettings lookup and other application arguments are ignored because nORM scaffold does not execute startup code.");
    }

    static bool AreEfPassThroughTokens(IReadOnlyList<string> unmatchedTokens, IReadOnlyList<string> commandLineArgs)
    {
        var passThroughTokens = GetEfPassThroughTokens(commandLineArgs);
        return passThroughTokens.Count > 0 &&
            unmatchedTokens.SequenceEqual(passThroughTokens, StringComparer.Ordinal);
    }

    static string? GetScaffoldPassThroughEnvironment()
    {
        var passThroughTokens = GetEfPassThroughTokens(Environment.GetCommandLineArgs().Skip(1).ToArray());
        for (var i = 0; i < passThroughTokens.Count; i++)
        {
            var token = passThroughTokens[i];
            if (string.Equals(token, "--environment", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= passThroughTokens.Count || passThroughTokens[i + 1].StartsWith("--", StringComparison.Ordinal))
                    throw new NormConfigurationException("EF-style application argument '--environment' requires a value.");

                return NullIfWhiteSpace(passThroughTokens[i + 1]);
            }

            const string environmentPrefix = "--environment=";
            if (token.StartsWith(environmentPrefix, StringComparison.OrdinalIgnoreCase))
                return NullIfWhiteSpace(token[environmentPrefix.Length..]);
        }

        return null;
    }

    static List<string> GetEfPassThroughTokens(IReadOnlyList<string> commandLineArgs)
    {
        var separatorIndex = -1;
        for (var i = 0; i < commandLineArgs.Count; i++)
        {
            if (commandLineArgs[i] == "--")
            {
                separatorIndex = i;
                break;
            }
        }

        return separatorIndex < 0
            ? new List<string>()
            : commandLineArgs.Skip(separatorIndex + 1).ToList();
    }

    static EfToolConfig? LoadEfToolConfig()
    {
        var configPath = FindEfToolConfigPath();
        if (configPath is null)
            return null;

        var configDirectory = Path.GetDirectoryName(configPath)!;
        var baseDirectory = Directory.GetParent(configDirectory)?.FullName ?? Directory.GetCurrentDirectory();
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new NormConfigurationException($"EF tool configuration file '{configPath}' must contain a JSON object.");

            return new EfToolConfig(
                ResolveEfToolConfigPath(ReadEfToolConfigString(document.RootElement, "project"), baseDirectory),
                ResolveEfToolConfigPath(ReadEfToolConfigString(document.RootElement, "startupProject"), baseDirectory),
                ReadEfToolConfigString(document.RootElement, "context"),
                ReadEfToolConfigString(document.RootElement, "framework"),
                ReadEfToolConfigString(document.RootElement, "configuration"),
                ReadEfToolConfigString(document.RootElement, "runtime"),
                ReadEfToolConfigBool(document.RootElement, "verbose"),
                ReadEfToolConfigBool(document.RootElement, "noColor"),
                ReadEfToolConfigBool(document.RootElement, "prefixOutput"));
        }
        catch (NormConfigurationException)
        {
            throw;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            throw new NormConfigurationException($"Could not read EF tool configuration file '{configPath}': {ex.Message}", ex);
        }
    }

    static string? FindEfToolConfigPath()
    {
        var directory = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (directory is not null)
        {
            var configPath = Path.Combine(directory.FullName, ".config", "dotnet-ef.json");
            if (File.Exists(configPath))
                return configPath;

            directory = directory.Parent;
        }

        return null;
    }

    static string? ReadEfToolConfigString(JsonElement root, string propertyName)
    {
        if (!TryGetJsonPropertyIgnoreCase(root, propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
            return null;

        if (property.ValueKind != JsonValueKind.String)
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must be a string.");

        return NullIfWhiteSpace(property.GetString());
    }

    static bool? ReadEfToolConfigBool(JsonElement root, string propertyName)
    {
        if (!TryGetJsonPropertyIgnoreCase(root, propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
            return null;

        return property.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must be a boolean.")
        };
    }

    static string? ResolveEfToolConfigPath(string? path, string baseDirectory)
    {
        if (path is null)
            return null;

        return Path.IsPathRooted(path)
            ? path
            : Path.GetFullPath(Path.Combine(baseDirectory, path));
    }

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

    static bool IsNamedConnectionReference(string connection)
        => connection.TrimStart().StartsWith("Name=", StringComparison.OrdinalIgnoreCase);

    static string ResolveScaffoldConnectionString(
        string connectionReference,
        ScaffoldProjectInfo? projectInfo,
        ScaffoldProjectInfo? startupProjectInfo,
        string? scaffoldEnvironment)
    {
        var trimmed = connectionReference.Trim();
        if (!IsNamedConnectionReference(trimmed))
            return connectionReference;

        var name = NullIfWhiteSpace(trimmed["Name=".Length..])
            ?? throw new NormConfigurationException("Scaffold named connection references must use Name=<configuration-key>.");
        if (TryResolveScaffoldNamedConnection(name, projectInfo, startupProjectInfo, scaffoldEnvironment, out var connectionString))
            return connectionString;

        var searchedDirectories = BuildScaffoldConfigurationSources(projectInfo, startupProjectInfo)
            .Select(source => $"'{source.Directory}'")
            .ToArray();
        throw new NormConfigurationException(
            $"Scaffold connection reference 'Name={name}' was not found in environment variables, user secrets, or appsettings JSON under {string.Join(", ", searchedDirectories)}. " +
            "Pass a literal connection string or add the named value without executing startup code.");
    }

    static bool TryResolveScaffoldNamedConnection(
        string name,
        ScaffoldProjectInfo? projectInfo,
        ScaffoldProjectInfo? startupProjectInfo,
        string? scaffoldEnvironment,
        out string connectionString)
    {
        var candidates = BuildNamedConnectionCandidates(name);
        connectionString = "";

        foreach (var candidate in candidates.Select(static key => key.Replace(":", "__", StringComparison.Ordinal)))
        {
            var value = NullIfWhiteSpace(Environment.GetEnvironmentVariable(candidate));
            if (value is not null)
            {
                connectionString = value;
                return true;
            }
        }

        var sources = BuildScaffoldConfigurationSources(projectInfo, startupProjectInfo);
        foreach (var source in sources)
        {
            var sourceValue = "";
            var userSecretsFile = GetUserSecretsFilePath(source.UserSecretsId);
            if (userSecretsFile is not null && File.Exists(userSecretsFile))
            {
                foreach (var candidate in candidates)
                {
                    if (TryReadJsonConfigurationValue(userSecretsFile, candidate, out var value))
                        sourceValue = value;
                }
            }

            if (!string.IsNullOrWhiteSpace(sourceValue))
            {
                connectionString = sourceValue;
                return true;
            }
        }

        foreach (var source in sources)
        {
            var sourceValue = "";
            foreach (var jsonFile in GetScaffoldAppSettingsFiles(source.Directory, scaffoldEnvironment).Where(File.Exists))
            {
                foreach (var candidate in candidates)
                {
                    if (TryReadJsonConfigurationValue(jsonFile, candidate, out var value))
                        sourceValue = value;
                }
            }

            if (!string.IsNullOrWhiteSpace(sourceValue))
            {
                connectionString = sourceValue;
                return true;
            }
        }

        return false;
    }

    static IReadOnlyList<ScaffoldConfigurationSource> BuildScaffoldConfigurationSources(
        ScaffoldProjectInfo? projectInfo,
        ScaffoldProjectInfo? startupProjectInfo)
    {
        var sources = new List<ScaffoldConfigurationSource>();
        AddProjectConfigurationSource(startupProjectInfo);
        AddProjectConfigurationSource(projectInfo);
        AddConfigurationSourceValue(new ScaffoldConfigurationSource(Directory.GetCurrentDirectory(), null));
        return sources;

        void AddProjectConfigurationSource(ScaffoldProjectInfo? info)
        {
            if (info is null)
                return;

            AddConfigurationSourceValue(new ScaffoldConfigurationSource(info.ProjectDirectory, info.UserSecretsId));
        }

        void AddConfigurationSourceValue(ScaffoldConfigurationSource source)
        {
            var fullDirectory = Path.GetFullPath(source.Directory);
            if (sources.Any(existing =>
                string.Equals(existing.Directory, fullDirectory, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(existing.UserSecretsId, source.UserSecretsId, StringComparison.Ordinal)))
                return;

            sources.Add(source with { Directory = fullDirectory });
        }
    }

    static IEnumerable<string> GetScaffoldAppSettingsFiles(string configDirectory, string? scaffoldEnvironment)
    {
        yield return Path.Combine(configDirectory, "appsettings.json");
        var environment = FirstNonBlank(
            scaffoldEnvironment,
            Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"),
            Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT"));
        if (environment is not null)
            yield return Path.Combine(configDirectory, $"appsettings.{environment}.json");
    }

    static string? GetUserSecretsFilePath(string? userSecretsId)
    {
        var id = NullIfWhiteSpace(userSecretsId);
        if (id is null)
            return null;

        if (OperatingSystem.IsWindows())
        {
            var appData = NullIfWhiteSpace(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData));
            return appData is null ? null : Path.Combine(appData, "Microsoft", "UserSecrets", id, "secrets.json");
        }

        var home = NullIfWhiteSpace(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
        return home is null ? null : Path.Combine(home, ".microsoft", "usersecrets", id, "secrets.json");
    }

    static IReadOnlyList<string> BuildNamedConnectionCandidates(string name)
    {
        var trimmed = name.Trim();
        if (trimmed.Contains(':', StringComparison.Ordinal))
            return new[] { trimmed };

        return new[] { trimmed, $"ConnectionStrings:{trimmed}" };
    }

    static bool TryReadJsonConfigurationValue(string jsonFile, string key, out string value)
    {
        value = "";
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(jsonFile));
            var current = document.RootElement;
            foreach (var segment in key.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (current.ValueKind != JsonValueKind.Object || !TryGetJsonPropertyIgnoreCase(current, segment, out current))
                    return false;
            }

            if (current.ValueKind != JsonValueKind.String)
                return false;

            var configured = NullIfWhiteSpace(current.GetString());
            if (configured is null)
                return false;

            value = configured;
            return true;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            throw new NormConfigurationException($"Could not read scaffold configuration file '{jsonFile}': {ex.Message}", ex);
        }
    }

    static bool TryGetJsonPropertyIgnoreCase(JsonElement element, string propertyName, out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (property.NameEquals(propertyName) || property.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    static IReadOnlyCollection<string> ParseTableFilters(string? commaSeparatedTables, IReadOnlyCollection<string>? repeatedTables)
    {
        var filters = new List<string>();
        filters.AddRange(ParseCliCsvList(commaSeparatedTables, "--tables"));
        if (repeatedTables is not null)
        {
            foreach (var item in repeatedTables)
            {
                if (string.IsNullOrWhiteSpace(item))
                    throw new NormConfigurationException("Scaffold --table values must be non-empty.");

                filters.Add(item.Trim());
            }
        }

        return filters
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static IReadOnlyCollection<string> ParseSchemaFilters(string? commaSeparatedSchemas, IReadOnlyCollection<string>? repeatedSchemas)
    {
        var filters = new List<string>();
        filters.AddRange(ParseCliCsvList(commaSeparatedSchemas, "--schemas"));
        if (repeatedSchemas is not null)
        {
            foreach (var item in repeatedSchemas)
            {
                if (string.IsNullOrWhiteSpace(item))
                    throw new NormConfigurationException("Scaffold --schema values must be non-empty.");

                filters.Add(item.Trim());
            }
        }

        return filters
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static IReadOnlyCollection<string> ParseCliCsvList(string? value, string optionName)
    {
        if (value is null)
            return Array.Empty<string>();

        if (string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"Scaffold {optionName} must include at least one non-empty filter.");

        var items = value.Split(',', StringSplitOptions.TrimEntries);
        if (items.Any(string.IsNullOrWhiteSpace))
            throw new NormConfigurationException($"Scaffold {optionName} must not contain empty filter entries.");

        return items;
    }

    static bool ScaffoldWarningsExist(string outputDirectory)
        => File.Exists(Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json"))
           || File.Exists(Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"));

    static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    static bool IsScaffoldWarningsFailure(NormConfigurationException exception, string outputDirectory)
        => exception.Message.Contains("Scaffolding produced warnings", StringComparison.Ordinal)
           && ScaffoldWarningsExist(outputDirectory);

    static void PrintScaffoldWarningSummary(string outputDirectory)
    {
        var summary = ReadScaffoldWarningSummary(outputDirectory);
        if (summary.TotalWarnings == 0)
        {
            if (summary.Error is not null)
                Console.WriteLine($"Scaffolding warning summary unavailable: {summary.Error}");
            return;
        }

        Console.WriteLine($"Scaffolding warning summary: {summary.TotalWarnings} warning(s) across {summary.Sections} section(s).");
        Console.WriteLine("Codes: " + string.Join(", ", summary.Codes.Select(pair => $"{pair.Key}={pair.Value}")));
        Console.WriteLine("Categories: " + string.Join(", ", summary.Categories.Select(pair => $"{pair.Key}={pair.Value}")));
    }

    static ScaffoldWarningSummary ReadScaffoldWarningSummary(string outputDirectory)
    {
        var jsonPath = Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json");
        if (!File.Exists(jsonPath))
            return ScaffoldWarningSummary.Empty;

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(jsonPath));
            var root = document.RootElement;
            var sections = new[]
            {
                "compositeForeignKeys",
                "possibleManyToManyJoinTables",
                "providerOwnedSchemaFeatures",
                "skippedDatabaseObjects"
            };
            var codeCounts = new SortedDictionary<string, int>(StringComparer.Ordinal);
            var categoryCounts = new SortedDictionary<string, int>(StringComparer.Ordinal);
            var total = 0;
            var nonEmptySections = 0;

            foreach (var section in sections)
            {
                if (!root.TryGetProperty(section, out var items) || items.ValueKind != JsonValueKind.Array)
                    continue;

                var sectionCount = 0;
                foreach (var item in items.EnumerateArray())
                {
                    total++;
                    sectionCount++;
                    IncrementDiagnosticCount(codeCounts, ReadScaffoldDiagnosticProperty(item, "code", "unknown"));
                    IncrementDiagnosticCount(categoryCounts, ReadScaffoldDiagnosticProperty(item, "category", "uncategorized"));
                }

                if (sectionCount > 0)
                    nonEmptySections++;
            }

            return new ScaffoldWarningSummary(total, nonEmptySections, codeCounts, categoryCounts, Error: null);
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            return new ScaffoldWarningSummary(0, 0, new SortedDictionary<string, int>(StringComparer.Ordinal), new SortedDictionary<string, int>(StringComparer.Ordinal), RedactConnectionStrings(ex.Message));
        }
    }

    static void WriteScaffoldResultJson(
        string status,
        string outputDirectory,
        bool dryRun,
        bool reportsWritten,
        ScaffoldWarningSummary warningSummary,
        Exception? error = null)
    {
        var payload = new
        {
            status,
            outputDirectory = Path.GetFullPath(outputDirectory),
            dryRun,
            warnings = new
            {
                hasDiagnostics = warningSummary.TotalWarnings > 0,
                reportsWritten,
                totalWarnings = warningSummary.TotalWarnings,
                sections = warningSummary.Sections,
                codes = warningSummary.Codes,
                categories = warningSummary.Categories,
                summaryError = warningSummary.Error
            },
            error = error is null ? null : RedactConnectionStrings(error.Message)
        };

        Console.WriteLine(JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }));
    }

    static void IncrementDiagnosticCount(IDictionary<string, int> counts, string key)
    {
        counts.TryGetValue(key, out var current);
        counts[key] = current + 1;
    }

    static string ReadScaffoldDiagnosticProperty(JsonElement item, string propertyName, string fallback)
        => item.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String
            ? property.GetString() ?? fallback
            : fallback;

    sealed record ScaffoldProjectInfo(string ProjectFile, string ProjectDirectory, string? DefaultNamespace, string? UserSecretsId, bool UseNullableReferenceTypes);

    sealed record EfToolConfig(
        string? Project,
        string? StartupProject,
        string? Context,
        string? Framework,
        string? Configuration,
        string? Runtime,
        bool? Verbose,
        bool? NoColor,
        bool? PrefixOutput);

    sealed record ScaffoldConfigurationSource(string Directory, string? UserSecretsId);

    sealed record ScaffoldWarningSummary(
        int TotalWarnings,
        int Sections,
        IReadOnlyDictionary<string, int> Codes,
        IReadOnlyDictionary<string, int> Categories,
        string? Error)
    {
        public static ScaffoldWarningSummary Empty { get; } = new(
            0,
            0,
            new SortedDictionary<string, int>(StringComparer.Ordinal),
            new SortedDictionary<string, int>(StringComparer.Ordinal),
            Error: null);
    }
}
