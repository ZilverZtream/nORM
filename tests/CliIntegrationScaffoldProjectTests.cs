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
    public void Scaffold_project_option_resolves_relative_output_and_project_namespace()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "project.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "ProjectApp.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                """
                <Project>
                  <PropertyGroup>
                    <RootNamespace>Inherited.Project.Namespace</RootNamespace>
                    <AssemblyName>InheritedProject</AssemblyName>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Project.Default.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
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
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --project {Quote(projectPath)} --startup-project {Quote(projectPath)} --no-build --framework net8.0 --configuration Release --runtime win-x64 --output-dir Models --context-dir Data/Contexts --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var entityCode = File.ReadAllText(Path.Combine(output, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(projectDir, "Data", "Contexts", "CliCtx.cs"));
            Assert.Contains("namespace Project.Default.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Project.Default.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Project.Default.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_project_inherits_directory_build_props_metadata()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_props_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "props.db");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "PropsApp.csproj");
        var userSecretsId = "norm-test-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForTest(userSecretsId);

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                $$"""
                <Project>
                  <PropertyGroup>
                    <RootNamespace>Inherited.Project.Namespace</RootNamespace>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
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
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Inherited.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
        }
    }

    [Fact]
    public void Scaffold_project_nullable_disable_generates_nullable_disabled_code()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_nullable_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "nullable.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NullableDisabledProject.csproj");
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                """
                <Project>
                  <PropertyGroup>
                    <Nullable>enable</Nullable>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(
                projectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>disable</Nullable>
                    <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        Notes TEXT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(projectDir, "Models", "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(projectDir, "Models", "CliCtx.cs"));
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("DbContextOptions options = null", contextCode, StringComparison.Ordinal);

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_project_inherits_nullable_enable_from_directory_build_props()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_nullable_props_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "nullable.db");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "PropsEnabledProject.csproj");
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                """
                <Project>
                  <PropertyGroup>
                    <Nullable>enable</Nullable>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(
                projectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        Notes TEXT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(projectDir, "Models", "Customer.cs"));
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string? Notes { get; set; }", entityCode, StringComparison.Ordinal);

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_current_directory_project_is_inferred_when_project_is_omitted()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_current_project_" + Guid.NewGuid().ToString("N"));
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentApp.csproj");
        var dbFile = Path.Combine(projectDir, "current.db");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Current.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
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
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --output-dir Models --context-dir Data/Contexts --context CliCtx",
                projectDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityCode = File.ReadAllText(Path.Combine(output, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(projectDir, "Data", "Contexts", "CliCtx.cs"));
            Assert.Contains("namespace Current.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Current.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Current.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_supplies_project_and_context_defaults()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "ConfiguredApp.csproj");
        var dbFile = Path.Combine(tempRoot, "configured.db");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/App",
                  "context": "Configured.Contexts.ConfiguredCtx",
                  "framework": "net8.0",
                  "configuration": "Release",
                  "runtime": "win-x64",
                  "verbose": true,
                  "noColor": true,
                  "prefixOutput": false
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Configured.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
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
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --output-dir Models",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityCode = File.ReadAllText(Path.Combine(output, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "ConfiguredCtx.cs"));
            Assert.Contains("namespace Configured.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Configured.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class ConfiguredCtx", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_startup_project_resolves_named_connection()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_startup_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var modelProjectDir = Path.Combine(tempRoot, "src", "Model");
        var startupProjectDir = Path.Combine(tempRoot, "src", "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        var dbFile = Path.Combine(tempRoot, "startup-config.db");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/Model",
                  "startupProject": "src/Startup",
                  "context": "ConfiguredCtx"
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                modelProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Configured.Model.Namespace</RootNamespace>
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
                    <RootNamespace>Configured.Startup.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
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
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --output-dir Models",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "ConfiguredCtx.cs"));
            Assert.Contains("namespace Configured.Model.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_rejects_non_boolean_common_flags()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_bool_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "verbose": "true"
                }
                """,
                Encoding.UTF8);

            var result = RunCli(
                $"scaffold {Quote("Data Source=:memory:")} sqlite --output-dir Models",
                workDir);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("EF tool configuration property 'verbose' must be a boolean", result.Stderr, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

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
    public void Scaffold_pass_through_environment_without_value_fails()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite -- --environment",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("EF-style application argument '--environment' requires a value", result.Stderr, StringComparison.Ordinal);
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
    public void Scaffold_ignores_ef_style_application_args_after_double_dash()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_app_args_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(output, "app_args.db");

        try
        {
            Directory.CreateDirectory(output);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} sqlite --output-dir {Quote(output)} -- --environment Production --tenant Contoso",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
        }
        finally
        {
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_unmatched_option_before_double_dash_fails()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite --conection typo",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Unrecognized scaffold argument(s)", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("accepted only after '--'", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("'--environment' is used for named-connection appsettings lookup", result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_unmatched_option_before_double_dash_is_not_masked_by_matching_app_arg()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite --bad -- --bad",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Unrecognized scaffold argument(s)", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("accepted only after '--'", result.Stderr, StringComparison.Ordinal);
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
