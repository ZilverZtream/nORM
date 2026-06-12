using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using nORM.Security;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for CLI design-time loading improvements (Blocker 33):
/// - Connection string redaction from error messages.
/// - Clear error when --assembly points to a missing file.
/// - Clear error when --project points to a missing file.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class CliDesignTimeTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);

    // ─── Connection string redaction ──────────────────────────────────────

    [Fact]
    public void Connection_string_password_is_redacted_from_error_output()
    {
        var root = FindRepositoryRoot();
        var secret = "S3cr3tPass!";
        var connection = $"Server=localhost;Database=norm;User ID=sa;Password={secret};Encrypt=True;TrustServerCertificate=True";
        var missingAssembly = Path.Combine(root, "missing-migrations.dll");

        var result = RunCli(
            $"database update --connection {Quote(connection)} --provider sqlserver --assembly {Quote(missingAssembly)}",
            root);

        Assert.NotEqual(0, result.ExitCode);
        // Secret must not appear anywhere in output
        Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Connection_string_token_key_is_redacted_from_error_output()
    {
        var root = FindRepositoryRoot();
        var secret = "my-super-secret-token";
        var connection = $"Server=localhost;Database=norm;Token={secret};Encrypt=True;TrustServerCertificate=True";
        var missingAssembly = Path.Combine(root, "missing-migrations.dll");

        var result = RunCli(
            $"database update --connection {Quote(connection)} --provider sqlserver --assembly {Quote(missingAssembly)}",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
    }

    // ─── RedactConnectionStrings logic (unit test of the helper logic) ────

    [Theory]
    [InlineData("password", "hunter2", "password=[REDACTED]")]
    [InlineData("pwd", "abc123", "pwd=[REDACTED]")]
    [InlineData("Password", "Hunter2", "Password=[REDACTED]")]
    [InlineData("token", "tok123", "token=[REDACTED]")]
    [InlineData("secret", "s3cr3t", "secret=[REDACTED]")]
    public void RedactConnectionStrings_replaces_sensitive_values(string key, string value, string expectedFragment)
    {
        var input = $"Server=localhost;{key}={value};Database=test";
        var result = ConnectionStringRedactor.RedactMessage(input);
        Assert.Contains(expectedFragment, result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(value, result, StringComparison.Ordinal);
    }

    [Fact]
    public void RedactConnectionStrings_does_not_touch_non_sensitive_keys()
    {
        var input = "Server=myserver;Database=mydb;Application Name=norm";
        var result = ConnectionStringRedactor.RedactMessage(input);
        Assert.Contains("myserver", result, StringComparison.Ordinal);
        Assert.Contains("mydb", result, StringComparison.Ordinal);
        Assert.Contains("Application Name=norm", result, StringComparison.Ordinal);
    }

    [Fact]
    public void RedactConnectionStrings_handles_null_and_empty()
    {
        Assert.Equal("", ConnectionStringRedactor.RedactMessage(""));
        Assert.Equal("no sensitive info here", ConnectionStringRedactor.RedactMessage("no sensitive info here"));
    }

    // ─── Missing assembly gives clear error ───────────────────────────────

    [Fact]
    public void Migrations_add_missing_assembly_gives_clear_error()
    {
        var root = FindRepositoryRoot();
        var missingAssembly = Path.Combine(Path.GetTempPath(), "does-not-exist-" + Guid.NewGuid() + ".dll");

        var result = RunCli(
            $"migrations add TestMigration --provider sqlite --assembly {Quote(missingAssembly)} --output {Quote(Path.GetTempPath())}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Missing project file gives clear error ────────────────────────────

    [Fact]
    public void Migrations_add_missing_project_file_gives_clear_error()
    {
        var root = FindRepositoryRoot();
        var missingProject = Path.Combine(Path.GetTempPath(), "does-not-exist-" + Guid.NewGuid() + ".csproj");

        var result = RunCli(
            $"migrations add TestMigration --provider sqlite --project {Quote(missingProject)} --output {Quote(Path.GetTempPath())}",
            root);

        // The CLI should exit non-zero and report that the project file was not found.
        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
    }

    // ─── RedactConnectionStrings helper (mirrors the logic in Program.cs) ─

    [Fact]
    public void Migrations_add_help_lists_design_time_project_options()
    {
        var root = FindRepositoryRoot();

        var result = RunCli("migrations add --help", root);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("--project", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--startup-project", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--target-framework", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--framework", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--configuration", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--runtime", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--environment", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--deps", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--runtimeconfig", result.Stdout, StringComparison.Ordinal);
    }

    [Fact]
    public void Migrations_add_missing_deps_file_gives_clear_error()
    {
        var root = FindRepositoryRoot();
        var missingAssembly = Path.Combine(Path.GetTempPath(), "does-not-exist-" + Guid.NewGuid() + ".dll");
        var missingDeps = Path.Combine(Path.GetTempPath(), "does-not-exist-" + Guid.NewGuid() + ".deps.json");

        var result = RunCli(
            $"migrations add TestMigration --provider sqlite --assembly {Quote(missingAssembly)} --deps {Quote(missingDeps)} --output {Quote(Path.GetTempPath())}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("Deps file", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Migrations_add_missing_runtimeconfig_file_gives_clear_error()
    {
        var root = FindRepositoryRoot();
        var missingAssembly = Path.Combine(Path.GetTempPath(), "does-not-exist-" + Guid.NewGuid() + ".dll");
        var missingRuntimeConfig = Path.Combine(Path.GetTempPath(), "does-not-exist-" + Guid.NewGuid() + ".runtimeconfig.json");

        var result = RunCli(
            $"migrations add TestMigration --provider sqlite --assembly {Quote(missingAssembly)} --runtimeconfig {Quote(missingRuntimeConfig)} --output {Quote(Path.GetTempPath())}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("Runtime config file", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
    }

    // ─── CLI invocation helpers ───────────────────────────────────────────

    private static CliResult RunCli(string arguments, string workingDirectory)
    {
        var root = FindRepositoryRoot();
        var toolPath = Path.Combine(root, "src", "dotnet-norm", "bin", "Release", "net8.0", "dotnet-norm.dll");
        Assert.True(File.Exists(toolPath), $"CLI tool was not built at {toolPath}. Run 'dotnet build src/dotnet-norm' first.");
        return RunProcess("dotnet", $"{Quote(toolPath)} {arguments}", workingDirectory);
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
            try { process.Kill(entireProcessTree: true); } catch { }
            process.WaitForExit();
            throw new TimeoutException($"{fileName} {arguments} timed out.");
        }
        process.WaitForExit();
        return new CliResult(process.ExitCode, stdoutTask.GetAwaiter().GetResult(), stderrTask.GetAwaiter().GetResult());
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

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr);
}
