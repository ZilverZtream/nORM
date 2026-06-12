using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

partial class Program
{
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

    sealed record ScaffoldConfigurationSource(string Directory, string? UserSecretsId);
}
