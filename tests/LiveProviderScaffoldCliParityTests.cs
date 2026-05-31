#nullable enable

using System;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "LiveProvider")]
[Collection("LiveProviderScaffolding")]
public sealed class LiveProviderScaffoldCliParityTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_builds_composite_shared_tenant_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var userTable = "CliLiveUser_" + suffix;
        var tagTable = "CliLiveTag_" + suffix;
        var userTagTable = "CliLiveUserTag_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_scaffold_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeSharedTenantManyToMany(connection, provider, kind, userTable, tagTable, userTagTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCtx " +
                $"--table {Quote(userTable)} " +
                $"--table {Quote(tagTable)} " +
                $"--table {Quote(userTagTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCtx.cs"));
            Assert.Contains(".UsingTable(", contextCode, StringComparison.Ordinal);
            Assert.Contains(userTagTable, contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, userTagTable + ".cs")), "Pure shared-tenant bridge table should scaffold as a many-to-many mapping, not as a payload entity.");

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupCompositeSharedTenantManyToMany(cleanup, provider, userTable, tagTable, userTagTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    private static (DbConnection Connection, DatabaseProvider Provider, string ConnectionString, string CliProvider)? OpenLive(ProviderKind kind, ref string? sqliteFile)
    {
        if (kind == ProviderKind.Sqlite)
        {
            sqliteFile = Path.Combine(Path.GetTempPath(), "norm_live_cli_scaffold_" + Guid.NewGuid().ToString("N") + ".db");
            var sqliteConnectionString = "Data Source=" + sqliteFile;
            var connection = new SqliteConnection(sqliteConnectionString);
            connection.Open();
            return (connection, new SqliteProvider(), sqliteConnectionString, "sqlite");
        }

        var connectionString = kind switch
        {
            ProviderKind.SqlServer => LiveProviderEnvironment.GetConnectionString("sqlserver"),
            ProviderKind.Postgres => LiveProviderEnvironment.GetConnectionString("postgres"),
            ProviderKind.MySql => LiveProviderEnvironment.GetConnectionString("mysql"),
            _ => null
        };
        if (string.IsNullOrEmpty(connectionString))
            return null;

        var live = LiveProviderFactory.OpenLive(kind);
        if (live is null)
            return null;

        return kind switch
        {
            ProviderKind.SqlServer => (live.Value.Connection, live.Value.Provider, connectionString, "sqlserver"),
            ProviderKind.Postgres => (live.Value.Connection, live.Value.Provider, connectionString, "postgres"),
            ProviderKind.MySql => (live.Value.Connection, live.Value.Provider, connectionString, "mysql"),
            _ => null
        };
    }

    private static DbConnection Reopen(ProviderKind kind, string connectionString)
    {
        if (kind == ProviderKind.Sqlite)
        {
            var connection = new SqliteConnection(connectionString);
            connection.Open();
            return connection;
        }

        var live = LiveProviderFactory.OpenLive(kind);
        if (live is not null)
            return live.Value.Connection;

        throw new InvalidOperationException($"Live provider {kind} is no longer available for cleanup.");
    }

    private static void SetupCompositeSharedTenantManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string userTable,
        string tagTable,
        string userTagTable)
    {
        CleanupCompositeSharedTenantManyToMany(connection, provider, userTable, tagTable, userTagTable);

        var tenantId = provider.Escape("TenantId");
        var userId = provider.Escape("UserId");
        var tagId = provider.Escape("TagId");
        var name = provider.Escape("Name");
        var user = provider.Escape(userTable);
        var tag = provider.Escape(tagTable);
        var userTag = provider.Escape(userTagTable);
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {user} ({tenantId} int NOT NULL, {userId} int NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {userId}))",
            $"CREATE TABLE {tag} ({tenantId} int NOT NULL, {tagId} int NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {tagId}))",
            $"CREATE TABLE {userTag} ({tenantId} int NOT NULL, {userId} int NOT NULL, {tagId} int NOT NULL, PRIMARY KEY ({tenantId}, {userId}, {tagId}), " +
            $"FOREIGN KEY ({tenantId}, {userId}) REFERENCES {user} ({tenantId}, {userId}), " +
            $"FOREIGN KEY ({tenantId}, {tagId}) REFERENCES {tag} ({tenantId}, {tagId}))");
    }

    private static void CleanupCompositeSharedTenantManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string userTable,
        string tagTable,
        string userTagTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(userTagTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(tagTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(userTable)}");
    }

    private static void Execute(DbConnection connection, params string[] statements)
    {
        foreach (var statement in statements)
        {
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
    }

    private static void WriteConsumerProject(string root, string output)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(Path.Combine(output, "CliLiveScaffolded.csproj"), $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <ImplicitUsings>enable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static CliResult RunCli(string arguments, string workingDirectory)
    {
        var root = FindRepositoryRoot();
        var toolPath = Path.Combine(root, "src", "dotnet-norm", "bin", "Release", "net8.0", "dotnet-norm.dll");
        Assert.True(File.Exists(toolPath), $"CLI tool was not built at {toolPath}.");

        return RunProcess("dotnet", $"{Quote(toolPath)} {arguments}", workingDirectory);
    }

    private static void RunDotNet(string arguments, string workingDirectory)
    {
        var result = RunProcess("dotnet", arguments, workingDirectory);
        Assert.True(result.ExitCode == 0,
            $"dotnet {arguments} failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
    }

    private static CliResult RunProcess(string fileName, string arguments, string workingDirectory)
    {
        var startInfo = new ProcessStartInfo(fileName, arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(ProcessTimeout))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // The process may exit between timeout detection and Kill.
            }

            process.WaitForExit();
            var timedOutStdout = stdoutTask.GetAwaiter().GetResult();
            var timedOutStderr = stderrTask.GetAwaiter().GetResult();
            throw new TimeoutException(
                $"{fileName} {arguments} did not exit within {ProcessTimeout.TotalSeconds:N0} seconds.{Environment.NewLine}STDOUT:{Environment.NewLine}{timedOutStdout}{Environment.NewLine}STDERR:{Environment.NewLine}{timedOutStderr}");
        }

        process.WaitForExit();
        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();
        return new CliResult(process.ExitCode, stdout, stderr);
    }

    private static string Quote(string value) => "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; failed deletion only leaves a temp directory.
        }
    }

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr);
}
