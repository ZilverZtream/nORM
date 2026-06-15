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
                ReadEfToolConfigBool(document.RootElement, "noRelationships"),
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

    static string? ResolveEfToolConfigPath(string? path, string baseDirectory)
    {
        if (path is null)
            return null;

        return Path.IsPathRooted(path)
            ? path
            : Path.GetFullPath(Path.Combine(baseDirectory, path));
    }
}
