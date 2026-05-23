using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the cancellation cleanup contract in <c>docs/exception-taxonomy.md</c>. Downstream
/// applications rely on each per-resource behavior promise documented there (transactions,
/// readers, temp tables, command pools, etc.), so the doc must not silently lose those rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CancellationContractDocTests
{
    private static string ReadDoc()
    {
        var asmDir = Path.GetDirectoryName(typeof(CancellationContractDocTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", "exception-taxonomy.md");
        Assert.True(File.Exists(path));
        return File.ReadAllText(path);
    }

    [Fact]
    public void Doc_includes_cancellation_cleanup_contract_section()
    {
        var doc = ReadDoc();
        Assert.Contains("Cancellation Cleanup Contract", doc, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Owned transactions")]
    [InlineData("Ambient transactions")]
    [InlineData("Temp tables")]
    [InlineData("Command pools")]
    [InlineData("Open data readers")]
    [InlineData("Migrations")]
    [InlineData("Temporal bootstrap")]
    [InlineData("Interceptors")]
    public void Doc_covers_resource(string label)
    {
        var doc = ReadDoc();
        Assert.Contains(label, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_states_caller_cancellation_is_not_rewrapped()
    {
        var doc = ReadDoc();
        // "Caller cancellation is not wrapped" / "is never re-wrapped" - either form is fine,
        // as long as the doc explicitly states the don't-wrap rule for caller cancellation.
        Assert.True(
            doc.Contains("not wrapped", StringComparison.OrdinalIgnoreCase) ||
            doc.Contains("re-wrapped", StringComparison.OrdinalIgnoreCase),
            "exception-taxonomy.md must explicitly state that caller cancellation is not wrapped.");
    }

    [Fact]
    public void Doc_states_timeout_versus_cancellation_distinction()
    {
        var doc = ReadDoc();
        Assert.Contains("NormTimeoutException", doc, StringComparison.Ordinal);
        Assert.Contains("OperationCanceledException", doc, StringComparison.Ordinal);
    }
}
