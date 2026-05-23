using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>docs/raw-sql-security.md</c> as the privileged-boundary contract. The runtime gate
/// (<c>NormValidator.ValidateRawQuerySql</c>) and the multi-tenancy doc must remain referenced
/// from this page so the raw-SQL API cannot be mistaken for an automatically tenant-safe
/// ORM-generated query.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class RawSqlSecurityDocContractTests
{
    private static string ReadDoc()
    {
        var asmDir = Path.GetDirectoryName(typeof(RawSqlSecurityDocContractTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", "raw-sql-security.md");
        Assert.True(File.Exists(path), $"docs/raw-sql-security.md not found at {path}");
        return File.ReadAllText(path);
    }

    [Fact]
    public void Doc_marks_raw_sql_as_read_only()
    {
        var doc = ReadDoc();
        Assert.Contains("read-only", doc, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Doc_names_the_runtime_validator()
    {
        var doc = ReadDoc();
        Assert.Contains("NormValidator", doc, StringComparison.Ordinal);
        Assert.Contains("ValidateRawQuerySql", doc, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SQL Server")]
    [InlineData("PostgreSQL")]
    [InlineData("MySQL")]
    [InlineData("SQLite")]
    public void Doc_describes_provider_gating(string label)
    {
        var doc = ReadDoc();
        Assert.Contains(label, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_calls_out_privileged_escape_hatches_explicitly()
    {
        var doc = ReadDoc();
        Assert.Contains("Privileged Escape Hatches", doc, StringComparison.Ordinal);
        Assert.Contains("Stored procedures", doc, StringComparison.Ordinal);
        Assert.Contains("not automatically tenant-filtered", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_states_tenant_boundary_is_not_automatic()
    {
        var doc = ReadDoc();
        Assert.Contains("Raw SQL does not automatically inject tenant predicates", doc, StringComparison.Ordinal);
        Assert.Contains("multi-tenancy-security.md", doc, StringComparison.Ordinal);
    }
}
