using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for CLI design-time loading improvements (Blocker 28):
/// - Connection string redaction from error messages.
/// - Clear error when --assembly points to a missing file.
/// - Clear error when --project points to a missing file.
/// </summary>
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
        var result = RedactConnectionStrings(input);
        Assert.Contains(expectedFragment, result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(value, result, StringComparison.Ordinal);
    }

    [Fact]
    public void RedactConnectionStrings_does_not_touch_non_sensitive_keys()
    {
        var input = "Server=myserver;Database=mydb;User ID=sa";
        var result = RedactConnectionStrings(input);
        Assert.Contains("myserver", result, StringComparison.Ordinal);
        Assert.Contains("mydb", result, StringComparison.Ordinal);
    }

    [Fact]
    public void RedactConnectionStrings_handles_null_and_empty()
    {
        Assert.Equal("", RedactConnectionStrings(""));
        Assert.Equal("no sensitive info here", RedactConnectionStrings("no sensitive info here"));
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

    private static string RedactConnectionStrings(string message)
    {
        if (string.IsNullOrEmpty(message))
            return message;

        var sensitiveKeys = new[] { "password", "pwd", "user password", "access token", "accesstoken", "token", "secret" };
        var result = message;
        foreach (var key in sensitiveKeys)
        {
            var pattern = key + "=";
            int idx = 0;
            while (true)
            {
                var pos = result.IndexOf(pattern, idx, StringComparison.OrdinalIgnoreCase);
                if (pos < 0) break;
                var valStart = pos + pattern.Length;
                var valEnd = result.IndexOf(';', valStart);
                if (valEnd < 0) valEnd = result.Length;
                result = result.Substring(0, valStart) + "[REDACTED]" + result.Substring(valEnd);
                idx = valStart + "[REDACTED]".Length;
            }
        }
        return result;
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
