using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace nORM.Tests;

public class CliIntegrationTests
{
    [Fact]
    public void Database_update_missing_assembly_returns_nonzero_without_leaking_connection_secret()
    {
        var root = FindRepositoryRoot();
        var secret = "TopSecret123!";
        var connection = $"Server=localhost;Database=norm;User ID=sa;Password={secret};Encrypt=True;TrustServerCertificate=True";
        var missingAssembly = Path.Combine(root, "missing-migrations.dll");

        var result = RunCli(
            $"database update --connection {Quote(connection)} --provider sqlserver --assembly {Quote(missingAssembly)}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Migrations_add_generates_compilable_literals_for_special_sql_text()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), WeirdModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add WeirdMigration --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.Equal(0, result.ExitCode);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_WeirdMigration.cs").Single();
            Assert.Contains("\\n", File.ReadAllText(generated), StringComparison.Ordinal);

            RunDotNet("build -c Release --no-restore --nologo", tempRoot);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    private static string ModelProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string WeirdModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Odd\"Table\\Line\nBreak")]
        public sealed class WeirdEntity
        {
            [Key]
            public int Id { get; set; }

            [Column("Value\"Column\\Line\nBreak")]
            public string Value { get; set; } = "";
        }
        """;

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
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
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
