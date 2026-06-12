using System;
using System.IO;
using System.Linq;
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

}
