#nullable enable

using System;
using System.IO;
using System.Text;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void ScaffoldContext_WithSqlServerSequence_EmitsNextValueWrapper()
    {
        var code = InvokeScaffoldContextWithSequence(
            "dbo",
            "OrderNo",
            "SQL Server sequence; dataType=bigint");

        Assert.Contains("private sealed class OrderNoSequenceValue", code);
        Assert.Contains("public long Value { get; set; }", code);
        Assert.Contains("public async Task<long> NextOrderNoValueAsync(CancellationToken ct = default)", code);
        Assert.Contains("SELECT NEXT VALUE FOR ", code);
        Assert.Contains("Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"OrderNo\")", code);
        Assert.Contains("QueryUnchangedAsync<OrderNoSequenceValue>", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_sequence_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithSequenceStubOnly_EmitsWrapperWithoutEntitySets()
    {
        var code = InvokeScaffoldContextWithSequenceOnly();

        Assert.Contains("public partial class AppDbContext", code, StringComparison.Ordinal);
        Assert.Contains("private sealed class OrderNoSequenceValue", code, StringComparison.Ordinal);
        Assert.Contains("public async Task<long> NextOrderNoValueAsync", code, StringComparison.Ordinal);
        Assert.DoesNotContain("IQueryable<User>", code, StringComparison.Ordinal);
    }

    [Fact]
    public void ScaffoldContext_WithSqlServerTinyIntSequence_EmitsByteWrapper()
    {
        var code = InvokeScaffoldContextWithSequence(
            "dbo",
            "TinyOrderNo",
            "SQL Server sequence; dataType=tinyint");

        Assert.Contains("private sealed class TinyOrderNoSequenceValue", code);
        Assert.Contains("public byte Value { get; set; }", code);
        Assert.Contains("public async Task<byte> NextTinyOrderNoValueAsync(CancellationToken ct = default)", code);
        Assert.Contains("QueryUnchangedAsync<TinyOrderNoSequenceValue>", code);
    }

    [Fact]
    public void ScaffoldContext_WithPostgresSequence_EmitsRegclassWrapper()
    {
        var code = InvokeScaffoldContextWithSequence(
            "public",
            "invoice_no",
            "PostgreSQL sequence; dataType=integer");

        Assert.Contains("private sealed class InvoiceNoSequenceValue", code);
        Assert.Contains("public int Value { get; set; }", code);
        Assert.Contains("public async Task<int> NextInvoiceNoValueAsync(CancellationToken ct = default)", code);
        Assert.Contains("SELECT nextval('", code);
        Assert.Contains("::regclass) AS ", code);
        Assert.Contains("(Provider.Escape(\"public\") + \".\" + Provider.Escape(\"invoice_no\")).Replace(\"'\", \"''\")", code);
    }

    [Fact]
    public void ScaffoldContext_WithDuplicateSequenceNamesAcrossSchemas_UsesSchemaQualifiedMemberNames()
    {
        var code = WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            sequenceStubs: new[]
            {
                new DatabaseScaffolder.ScaffoldSkippedObject(
                    "billing",
                    "OrderNo",
                    "Sequence",
                    "SQL Server sequence; dataType=bigint",
                    null),
                new DatabaseScaffolder.ScaffoldSkippedObject(
                    "audit",
                    "OrderNo",
                    "Sequence",
                    "SQL Server sequence; dataType=bigint",
                    null)
            });

        Assert.Contains("private sealed class AuditOrderNoSequenceValue", code, StringComparison.Ordinal);
        Assert.Contains("private sealed class BillingOrderNoSequenceValue", code, StringComparison.Ordinal);
        Assert.Contains("public async Task<long> NextAuditOrderNoValueAsync", code, StringComparison.Ordinal);
        Assert.Contains("public async Task<long> NextBillingOrderNoValueAsync", code, StringComparison.Ordinal);
        Assert.DoesNotContain("OrderNoSequenceValue2", code, StringComparison.Ordinal);
        Assert.DoesNotContain("NextOrderNoValueAsync2", code, StringComparison.Ordinal);
        Assert.Contains("Provider.Escape(\"audit\") + \".\" + Provider.Escape(\"OrderNo\")", code, StringComparison.Ordinal);
        Assert.Contains("Provider.Escape(\"billing\") + \".\" + Provider.Escape(\"OrderNo\")", code, StringComparison.Ordinal);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_duplicate_sequences_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithUseDatabaseNames_SequenceStubPreservesLegalSequenceName()
    {
        var code = InvokeScaffoldContextWithSequence(
            "public",
            "invoice_no",
            "PostgreSQL sequence; dataType=integer",
            useDatabaseNames: true);

        Assert.Contains("private sealed class invoice_noSequenceValue", code);
        Assert.Contains("public async Task<int> Nextinvoice_noValueAsync(CancellationToken ct = default)", code);
        Assert.Contains("QueryUnchangedAsync<invoice_noSequenceValue>", code);
        Assert.DoesNotContain("InvoiceNoSequenceValue", code, StringComparison.Ordinal);
        Assert.DoesNotContain("NextInvoiceNoValueAsync", code, StringComparison.Ordinal);
    }
}
