using System;
using System.IO;
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
}
