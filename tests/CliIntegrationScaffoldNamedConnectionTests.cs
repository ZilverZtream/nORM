using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    [Fact]
    public void Scaffold_lowercase_named_connection_shorthand_from_project_appsettings_generates_model()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_connection_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "named.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedApp.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.Connection.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("name=AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.Connection.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_named_connection_environment_variable_overrides_project_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_envvar_" + Guid.NewGuid().ToString("N"));
        var envDbFile = Path.Combine(tempRoot, "env.db");
        var appsettingsDbFile = Path.Combine(tempRoot, "appsettings.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedEnvVarApp.csproj");
        var connectionName = "EnvDb" + Guid.NewGuid().ToString("N");
        var environmentKey = "ConnectionStrings__" + connectionName;

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.EnvVar.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Data Source={{appsettingsDbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={envDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE EnvCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={appsettingsDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE AppsettingsCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Environment.SetEnvironmentVariable(environmentKey, $"Data Source={envDbFile}");
            var result = RunCli(
                $"scaffold {Quote("Name=" + connectionName)} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "EnvCustomer.cs")));
            Assert.False(File.Exists(Path.Combine(output, "AppsettingsCustomer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.EnvVar.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<EnvCustomer> EnvCustomers", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("AppsettingsCustomer", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(environmentKey, null);
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_named_connection_project_user_secrets_override_same_directory_startup_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_secret_precedence_" + Guid.NewGuid().ToString("N"));
        var projectDir = Path.Combine(tempRoot, "App");
        var modelProjectPath = Path.Combine(projectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(projectDir, "StartupApp.csproj");
        var secretDbFile = Path.Combine(tempRoot, "secret.db");
        var appsettingsDbFile = Path.Combine(tempRoot, "appsettings.db");
        var userSecretsId = "norm-test-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForTest(userSecretsId);

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                modelProjectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Secrets.Override.Model</RootNamespace>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Secrets.Override.Startup</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{appsettingsDbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{secretDbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={secretDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE SecretCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={appsettingsDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE AppsettingsCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(modelProjectPath)} --startup-project {Quote(startupProjectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "SecretCustomer.cs")));
            Assert.False(File.Exists(Path.Combine(output, "AppsettingsCustomer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Secrets.Override.Model.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SecretCustomer> SecretCustomers", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("AppsettingsCustomer", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
        }
    }

    [Fact]
    public void Scaffold_named_connection_uses_pass_through_environment_for_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_environment_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "named_env.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedEnvironmentApp.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.Environment.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx -- --environment Production",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.Environment.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_named_connection_uses_ambient_environment_for_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_ambient_environment_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "named_ambient_env.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedAmbientEnvironmentApp.csproj");
        var previousAspNetCoreEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        var previousDotnetEnvironment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.Ambient.Environment.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Staging.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Staging");
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", null);

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.Ambient.Environment.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", previousAspNetCoreEnvironment);
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", previousDotnetEnvironment);
            TryDeleteDirectory(tempRoot);
        }
    }


    [Fact]
    public void Scaffold_named_connection_from_project_user_secrets_generates_model()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_user_secrets_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "secret.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "SecretApp.csproj");
        var userSecretsId = "norm-test-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForTest(userSecretsId);

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                projectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>User.Secrets.App</RootNamespace>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace User.Secrets.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
        }
    }


    [Fact]
    public void Scaffold_startup_project_short_alias_resolves_named_connection()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_startup_short_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "startup.db");
        var projectDir = Path.Combine(tempRoot, "ModelProject");
        var startupDir = Path.Combine(tempRoot, "StartupProject");
        var projectPath = Path.Combine(projectDir, "ModelProject.csproj");
        var startupProjectPath = Path.Combine(startupDir, "StartupProject.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(startupDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Short.Model</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Short.Host</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} -s {Quote(startupProjectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Startup.Short.Model.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }
}
