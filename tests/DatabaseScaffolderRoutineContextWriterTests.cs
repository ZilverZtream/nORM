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
    public void ScaffoldContext_WithRoutineStub_EmitsProviderBoundWrapperMethods()
    {
        var code = InvokeScaffoldContextWithRoutineStub();

        Assert.Contains("using System.Threading;", code);
        Assert.Contains("using System.Threading.Tasks;", code);
        Assert.Contains("Executes provider-bound stored procedure `dbo.GetRevenue`", code);
        Assert.Contains("Parameters discovered at scaffold time: @tenantId IN int, @total OUT decimal", code);
        Assert.Contains("public sealed class GetRevenueParameters", code);
        Assert.Contains("public int? tenantId { get; init; }", code);
        Assert.Contains("public string? message { get; init; }", code);
        Assert.DoesNotContain("public decimal? total { get; init; }", code);
        Assert.Contains("public sealed class GetRevenueResult", code);
        Assert.Contains("public int Id { get; set; }", code);
        Assert.Contains("public string Name { get; set; } = default!;", code);
        Assert.Contains("Task<List<TResult>> GetRevenueAsync<TResult>(GetRevenueParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("Task<List<GetRevenueResult>> GetRevenueAsync(GetRevenueParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\")))", code);
        Assert.Contains("ExecuteStoredProcedureAsync<GetRevenueResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\")))", code);
        Assert.Contains("IAsyncEnumerable<TResult> StreamGetRevenueAsync<TResult>(GetRevenueParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("IAsyncEnumerable<GetRevenueResult> StreamGetRevenueAsync(GetRevenueParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureAsAsyncEnumerable<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\")))", code);
        Assert.Contains("ExecuteStoredProcedureAsAsyncEnumerable<GetRevenueResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\")))", code);
        Assert.Contains("without buffering the full result set", code);
        Assert.Contains("Task<StoredProcedureResult<TResult>> GetRevenueWithOutputAsync<TResult>", code);
        Assert.Contains("Task<StoredProcedureResult<GetRevenueResult>> GetRevenueWithOutputAsync", code);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\")), outputParameters)", code);
        Assert.Contains("output parameters discovered at scaffold time", code);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\")), CreateGetRevenueOutputParameters())", code);
        Assert.Contains("public static OutputParameter[] CreateGetRevenueOutputParameters()", code);
        Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal, (byte)18, (byte)2)", code);
        Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32, System.Data.ParameterDirection.InputOutput)", code);
        Assert.Contains("private static object? RequireScaffoldedRoutineParameters", code);
        Assert.Contains("Routine bodies are provider-owned and are not translated by nORM", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_routine_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithRoutineStubOnly_EmitsWrapperWithoutEntitySets()
    {
        var code = InvokeScaffoldContextWithRoutineStubOnly();

        Assert.Contains("public partial class AppDbContext", code, StringComparison.Ordinal);
        Assert.Contains("public sealed class GetRevenueParameters", code, StringComparison.Ordinal);
        Assert.Contains("Task<List<TResult>> GetRevenueAsync<TResult>", code, StringComparison.Ordinal);
        Assert.DoesNotContain("IQueryable<User>", code, StringComparison.Ordinal);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_routine_only_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithDuplicateRoutineNamesAcrossSchemas_UsesSchemaQualifiedMemberNames()
    {
        var code = WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            routineStubs: new[]
            {
                new ScaffoldSkippedObject(
                    "billing",
                    "SyncLedger",
                    "Routine",
                    "SQL Server stored procedure; parameters=0",
                    null),
                new ScaffoldSkippedObject(
                    "audit",
                    "SyncLedger",
                    "Routine",
                    "SQL Server stored procedure; parameters=0",
                    null)
            });

        Assert.Contains("Task<List<TResult>> AuditSyncLedgerAsync<TResult>", code, StringComparison.Ordinal);
        Assert.Contains("Task<List<TResult>> BillingSyncLedgerAsync<TResult>", code, StringComparison.Ordinal);
        Assert.Contains("StreamAuditSyncLedgerAsync<TResult>", code, StringComparison.Ordinal);
        Assert.Contains("StreamBillingSyncLedgerAsync<TResult>", code, StringComparison.Ordinal);
        Assert.DoesNotContain("SyncLedgerAsync2", code, StringComparison.Ordinal);
        Assert.Contains("Provider.Escape(\"audit\") + \".\" + Provider.Escape(\"SyncLedger\")", code, StringComparison.Ordinal);
        Assert.Contains("Provider.Escape(\"billing\") + \".\" + Provider.Escape(\"SyncLedger\")", code, StringComparison.Ordinal);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_duplicate_routines_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithSameSchemaRoutineOverloads_UsesSignatureAwareNames()
    {
        var code = WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            routineStubs: new[]
            {
                new ScaffoldSkippedObject(
                    "public",
                    "CalculateScore",
                    "Routine",
                    "PostgreSQL function; parameters=1; parameterModes=value:IN:integer; callShape=scalar-function; dataType=integer",
                    null),
                new ScaffoldSkippedObject(
                    "public",
                    "CalculateScore",
                    "Routine",
                    "PostgreSQL function; parameters=1; parameterModes=value:IN:text; callShape=scalar-function; dataType=integer",
                    null)
            });

        Assert.Contains("public sealed class CalculateScoreIntegerParameters", code, StringComparison.Ordinal);
        Assert.Contains("public sealed class CalculateScoreTextParameters", code, StringComparison.Ordinal);
        Assert.Contains("Task<List<TResult>> CalculateScoreIntegerAsync<TResult>", code, StringComparison.Ordinal);
        Assert.Contains("Task<List<TResult>> CalculateScoreTextAsync<TResult>", code, StringComparison.Ordinal);
        Assert.DoesNotContain("PublicCalculateScore", code, StringComparison.Ordinal);
        Assert.DoesNotContain("CalculateScoreAsync2", code, StringComparison.Ordinal);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_same_schema_overloaded_routines_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithUseDatabaseNames_RoutineStubPreservesLegalRoutineAndResultColumnNames()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "calculate_risk",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@customer_id:IN:int; resultColumns=total_value:decimal(18,2):0",
            useDatabaseNames: true);

        Assert.Contains("public sealed class calculate_riskParameters", code);
        Assert.Contains("public int? customer_id { get; init; }", code);
        Assert.Contains("public sealed class calculate_riskResult", code);
        Assert.Contains("public decimal total_value { get; set; }", code);
        Assert.Contains("Task<List<TResult>> calculate_riskAsync<TResult>(calculate_riskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("Task<List<calculate_riskResult>> calculate_riskAsync(calculate_riskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("IAsyncEnumerable<calculate_riskResult> Streamcalculate_riskAsync(calculate_riskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.DoesNotContain("CalculateRisk", code, StringComparison.Ordinal);
        Assert.DoesNotContain("TotalValue", code, StringComparison.Ordinal);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_database_names_routine_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithKnownNoResultProcedure_EmitsNonQueryWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "RecalculateLedger",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenantId:IN:int; resultColumns=");

        Assert.Contains("Task<int> RecalculateLedgerAsync(RecalculateLedgerParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureNonQueryAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"RecalculateLedger\"), ct, RequireScaffoldedRoutineParameters(parameters, 1, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"RecalculateLedger\")))", code);
        Assert.DoesNotContain("RecalculateLedgerAsync<TResult>", code);
        Assert.DoesNotContain("StreamRecalculateLedgerAsync", code);
    }

    [Fact]
    public void ScaffoldContext_WithKnownNoResultOutputProcedure_EmitsNonQueryOutputWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "FinalizeLedger",
            "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@tenantId:IN:int,@status:OUT:nvarchar(32); resultColumns=");

        Assert.Contains("Task<int> FinalizeLedgerAsync(FinalizeLedgerParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("Task<StoredProcedureNonQueryResult> FinalizeLedgerWithOutputAsync(FinalizeLedgerParameters? parameters = null, CancellationToken ct = default, params OutputParameter[] outputParameters)", code);
        Assert.Contains("ExecuteStoredProcedureNonQueryWithOutputAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"FinalizeLedger\"), ct, RequireScaffoldedRoutineParameters(parameters, 1, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"FinalizeLedger\")), outputParameters)", code);
        Assert.Contains("Task<StoredProcedureNonQueryResult> FinalizeLedgerWithOutputAsync(FinalizeLedgerParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureNonQueryWithOutputAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"FinalizeLedger\"), ct, RequireScaffoldedRoutineParameters(parameters, 1, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"FinalizeLedger\")), CreateFinalizeLedgerOutputParameters())", code);
        Assert.DoesNotContain("StoredProcedureResult<TResult> FinalizeLedgerWithOutputAsync", code);
        Assert.DoesNotContain("FinalizeLedgerAsync<TResult>", code);
        Assert.DoesNotContain("StreamFinalizeLedgerAsync", code);
    }

    [Fact]
    public void ScaffoldContext_WithUnscaffoldableRoutineOutputName_EmitsExplicitOutputOverloadOnly()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "WeirdOutput",
            "SQL Server stored procedure; parameters=1; outputParameters=1; parameterModes=@bad name:OUT:int; resultColumns=");

        Assert.Contains("Task<int> WeirdOutputAsync(object? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("Task<StoredProcedureNonQueryResult> WeirdOutputWithOutputAsync(object? parameters = null, CancellationToken ct = default, params OutputParameter[] outputParameters)", code);
        Assert.Contains("ExecuteStoredProcedureNonQueryWithOutputAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"WeirdOutput\"), ct, parameters, outputParameters)", code);
        Assert.DoesNotContain("CreateWeirdOutputOutputParameters", code);
        Assert.DoesNotContain("output parameters discovered at scaffold time", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_unscaffoldable_output_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithPrecisionOnlyRoutineOutput_EmitsNullableScaleOutputParameter()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "GetLedgerTotal",
            "SQL Server stored procedure; parameters=1; outputParameters=1; parameterModes=@total:OUT:decimal(18); resultColumns=");

        Assert.Contains("public static OutputParameter[] CreateGetLedgerTotalOutputParameters()", code);
        Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal, null, (byte)18, null, System.Data.ParameterDirection.Output, null)", code);
    }

    [Fact]
    public void ScaffoldContext_WithRoutineReturnValue_EmitsReturnOutputParameter()
    {
        var code = InvokeScaffoldContextWithRoutineReturnStub();

        Assert.Contains("Parameters discovered at scaffold time: @orderId IN int, return RETURN int", code);
        Assert.Contains("public int? orderId { get; init; }", code);
        Assert.DoesNotContain("public int? @return { get; init; }", code);
        Assert.Contains("Task<StoredProcedureResult<TResult>> ApplyDiscountWithOutputAsync<TResult>", code);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"ApplyDiscount\"), ct, RequireScaffoldedRoutineParameters(parameters, 1, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"ApplyDiscount\")), CreateApplyDiscountOutputParameters())", code);
        Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_routine_return_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithSqlServerAliasAndTableValuedRoutineParameters_EmitsBestEffortTypes()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "ImportLines",
            "SQL Server stored procedure; parameters=4; outputParameters=0; parameterModes=@email:IN:nvarchar(320),@amount:IN:decimal(18,4),@login:IN:sysname,@items:IN:table type (dbo.LineItemList)");

        Assert.Contains("public string? email { get; init; }", code);
        Assert.Contains("public decimal? amount { get; init; }", code);
        Assert.Contains("public string? login { get; init; }", code);
        Assert.Contains("public DbParameter? items { get; init; }", code);
        Assert.Contains("Parameters discovered at scaffold time: @email IN nvarchar(320), @amount IN decimal(18,4), @login IN sysname, @items IN table type (dbo.LineItemList)", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_sqlserver_alias_routine_" + Guid.NewGuid().ToString("N"));
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
    public void ScaffoldContext_WithQuotedStoredProcedureName_UsesProviderEscapedInvocationName()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "sales ops",
            "Get Revenue",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenantId:IN:int");

        Assert.Contains("Provider.Escape(\"sales ops\") + \".\" + Provider.Escape(\"Get Revenue\")", code);
        Assert.DoesNotContain("\"sales ops.Get Revenue\"", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_quoted_routine_" + Guid.NewGuid().ToString("N"));
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

}
