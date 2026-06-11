using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Core;

partial class Program
{
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

    sealed record ScaffoldConfigurationSource(string Directory, string? UserSecretsId);
}
