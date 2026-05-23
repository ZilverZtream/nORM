using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>docs/interceptors.md</c> and <c>docs/logging-redaction.md</c>. Third-party
/// interceptor authors depend on the documented hook ordering and mutation rules; downstream
/// operators depend on the default redaction policy. Fail loudly if either doc drops a section
/// that callers integrate against.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class InterceptorAndLoggingDocContractTests
{
    private static string ReadDoc(string fileName)
    {
        var asmDir = Path.GetDirectoryName(typeof(InterceptorAndLoggingDocContractTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", fileName);
        Assert.True(File.Exists(path), $"docs/{fileName} not found at {path}");
        return File.ReadAllText(path);
    }

    // Interceptors doc

    [Theory]
    [InlineData("Command Interceptors")]
    [InlineData("SaveChanges Interceptors")]
    [InlineData("Threading Rules")]
    public void Interceptor_doc_covers_section(string heading)
    {
        var doc = ReadDoc("interceptors.md");
        Assert.Contains(heading, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Interceptor_doc_documents_suppression_path()
    {
        var doc = ReadDoc("interceptors.md");
        Assert.Contains("Suppression", doc, StringComparison.Ordinal);
        Assert.Contains("SuppressWithResult", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Interceptor_doc_documents_failure_path()
    {
        var doc = ReadDoc("interceptors.md");
        Assert.Contains("CommandFailed", doc, StringComparison.Ordinal);
        Assert.Contains("rethrown", doc, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Interceptor_doc_states_sync_and_async_separation()
    {
        var doc = ReadDoc("interceptors.md");
        Assert.Contains("Sync APIs", doc, StringComparison.Ordinal);
        Assert.Contains("not block on async hooks", doc, StringComparison.OrdinalIgnoreCase);
    }

    // Logging-redaction doc

    [Theory]
    [InlineData("Default Policy")]
    [InlineData("Privileged Extensibility")]
    [InlineData("Raw SQL")]
    [InlineData("Release Gate")]
    public void Logging_doc_covers_section(string heading)
    {
        var doc = ReadDoc("logging-redaction.md");
        Assert.Contains(heading, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Logging_doc_states_default_sql_literal_redaction()
    {
        var doc = ReadDoc("logging-redaction.md");
        Assert.Contains("SQL string literals are redacted", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Logging_doc_documents_release_artifact_redaction()
    {
        var doc = ReadDoc("logging-redaction.md");
        Assert.Contains("Benchmark and release artifacts", doc, StringComparison.Ordinal);
        Assert.Contains("NORM_TEST_", doc, StringComparison.Ordinal);
    }
}
