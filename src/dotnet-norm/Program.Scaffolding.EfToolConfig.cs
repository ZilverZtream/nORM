using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Core;

partial class Program
{
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
                ReadFirstEfToolConfigString(document.RootElement, "outputDir", "output"),
                ReadEfToolConfigString(document.RootElement, "namespace"),
                ReadEfToolConfigString(document.RootElement, "context"),
                ReadEfToolConfigString(document.RootElement, "contextDir"),
                ReadEfToolConfigString(document.RootElement, "contextNamespace"),
                ReadEfToolConfigStringList(document.RootElement, "schemas", "schema"),
                ReadEfToolConfigStringList(document.RootElement, "tables", "table"),
                ReadFirstEfToolConfigString(document.RootElement, "framework", "targetFramework"),
                ReadEfToolConfigString(document.RootElement, "configuration"),
                ReadEfToolConfigString(document.RootElement, "runtime"),
                ReadEfToolConfigString(document.RootElement, "msbuildProjectExtensionsPath"),
                ReadEfToolConfigBool(document.RootElement, "noBuild"),
                ReadEfToolConfigBool(document.RootElement, "json"),
                ReadEfToolConfigBool(document.RootElement, "verbose"),
                ReadEfToolConfigBool(document.RootElement, "noColor"),
                ReadEfToolConfigBool(document.RootElement, "prefixOutput"),
                ReadEfToolConfigBool(document.RootElement, "noPluralize"),
                ReadEfToolConfigBool(document.RootElement, "useDatabaseNames"),
                ReadEfToolConfigBool(document.RootElement, "noOnConfiguring"),
                ReadEfToolConfigBool(document.RootElement, "dataAnnotations"),
                ReadEfToolConfigBool(document.RootElement, "force"),
                ReadEfToolConfigBool(document.RootElement, "noOverwrite"),
                ReadEfToolConfigBool(document.RootElement, "dryRun"),
                ReadEfToolConfigBool(document.RootElement, "failOnWarnings"),
                ReadEfToolConfigBool(document.RootElement, "emitRoutineStubs"),
                ReadEfToolConfigBool(document.RootElement, "emitSequenceStubs"),
                ReadEfToolConfigBool(document.RootElement, "emitViewEntities"),
                ReadEfToolConfigBool(document.RootElement, "emitQueryArtifacts"));
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

        var value = NullIfWhiteSpace(property.GetString());
        if (value is null)
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must not be blank.");

        return value;
    }

    static string? ReadFirstEfToolConfigString(JsonElement root, params string[] propertyNames)
    {
        foreach (var propertyName in propertyNames)
        {
            var value = ReadEfToolConfigString(root, propertyName);
            if (value is not null)
                return value;
        }

        return null;
    }

    static IReadOnlyList<string> ReadEfToolConfigStringList(JsonElement root, params string[] propertyNames)
    {
        var values = new List<string>();
        foreach (var propertyName in propertyNames)
        {
            if (!TryGetJsonPropertyIgnoreCase(root, propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
                continue;

            if (property.ValueKind == JsonValueKind.String)
            {
                AddEfToolConfigListValue(values, property.GetString(), propertyName);
                continue;
            }

            if (property.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in property.EnumerateArray())
                {
                    if (item.ValueKind != JsonValueKind.String)
                        throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must contain only strings.");

                    AddEfToolConfigListValue(values, item.GetString(), propertyName);
                }

                continue;
            }

            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must be a string or string array.");
        }

        return values
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static void AddEfToolConfigListValue(List<string> values, string? value, string propertyName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must not contain blank values.");

        var parts = value.Split(',', StringSplitOptions.TrimEntries);
        if (parts.Any(string.IsNullOrWhiteSpace))
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must not contain blank values.");

        values.AddRange(parts);
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
}
