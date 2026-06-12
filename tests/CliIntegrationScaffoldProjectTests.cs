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

}
