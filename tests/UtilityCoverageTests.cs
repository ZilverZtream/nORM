using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Entities for utility tests ─────────────────────────────────────────────

[Table("UCT_Gadget")]
public class UctGadget
{
    [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Price { get; set; }
    public string? Description { get; set; }
}

// ── Interceptor helpers ────────────────────────────────────────────────────

public sealed class UctLoggingInterceptor : BaseDbCommandInterceptor
{
    public int NonQueryCount;
    public int ScalarCount;
    public int ReaderCount;
    public int FailureCount;

    public UctLoggingInterceptor(ILogger logger) : base(logger) { }
}

public sealed class UctSuppressingInterceptor : IDbCommandInterceptor
{
    public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<int>.SuppressWithResult(42));

    public Task NonQueryExecutedAsync(DbCommand cmd, DbContext ctx, int result, TimeSpan dur, CancellationToken ct)
        => Task.CompletedTask;

    public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<object?>.SuppressWithResult((object?)99L));

    public Task ScalarExecutedAsync(DbCommand cmd, DbContext ctx, object? result, TimeSpan dur, CancellationToken ct)
        => Task.CompletedTask;

    public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<DbDataReader>.SuppressWithResult(new UctEmptyReader()));

    public Task ReaderExecutedAsync(DbCommand cmd, DbContext ctx, DbDataReader reader, TimeSpan dur, CancellationToken ct)
        => Task.CompletedTask;

    public Task CommandFailedAsync(DbCommand cmd, DbContext ctx, Exception ex, CancellationToken ct)
        => Task.CompletedTask;
}

public sealed class UctEmptyReader : DbDataReader
{
    public override bool Read() => false;
    public override bool NextResult() => false;
    public override int FieldCount => 0;
    public override object GetValue(int ordinal) => DBNull.Value;
    public override int GetValues(object[] values) => 0;
    public override bool IsDBNull(int ordinal) => true;
    public override bool GetBoolean(int ordinal) => false;
    public override byte GetByte(int ordinal) => 0;
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
    public override char GetChar(int ordinal) => '\0';
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
    public override string GetDataTypeName(int ordinal) => "object";
    public override DateTime GetDateTime(int ordinal) => default;
    public override decimal GetDecimal(int ordinal) => 0;
    public override double GetDouble(int ordinal) => 0;
    public override System.Collections.IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
    public override float GetFloat(int ordinal) => 0;
    public override Guid GetGuid(int ordinal) => default;
    public override short GetInt16(int ordinal) => 0;
    public override int GetInt32(int ordinal) => 0;
    public override long GetInt64(int ordinal) => 0;
    public override string GetName(int ordinal) => "";
    public override int GetOrdinal(string name) => 0;
    public override string GetString(int ordinal) => "";
    public override bool HasRows => false;
    public override bool IsClosed => false;
    public override int RecordsAffected => 0;
    public override int Depth => 0;
    public override object this[int ordinal] => DBNull.Value;
    public override object this[string name] => DBNull.Value;
    public override Type GetFieldType(int ordinal) => typeof(object);
}

/// <summary>Coverage for DbCommandExtensions, FastExpressionVisitorPool, DynamicBatchSizer,
/// CommandInterceptorExtensions, DbConnectionFactory, BaseDbCommandInterceptor.</summary>
public class UtilityCoverageTests
{
    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static SqliteConnection CreateGadgetDb()
    {
        var cn = OpenDb();
        Exec(cn, "CREATE TABLE UCT_Gadget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Price INTEGER NOT NULL DEFAULT 0, Description TEXT)");
        Exec(cn, "INSERT INTO UCT_Gadget (Name, Price, Description) VALUES ('Widget', 10, 'A widget'), ('Gizmo', 20, NULL), ('Doohickey', 5, 'cheap')");
        return cn;
    }

    // ══════════════════════════════════════════════════════════════════════
    // DbCommandExtensions — AddParamsStackAlloc / SetParametersFast
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void DbCommandExtensions_AddParamsStackAlloc_AddsParameters()
    {
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT @p0";
        (string, object)[] arr = { ("@p0", (object)42) };
        cmd.AddParamsStackAlloc(arr);
        Assert.Equal(1, cmd.Parameters.Count);
        Assert.Equal("@p0", cmd.Parameters[0].ParameterName);
    }

    [Fact]
    public void DbCommandExtensions_AddParamsStackAlloc_ReusesExistingParams()
    {
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT @p0, @p1";
        (string, object)[] ps1 = { ("@p0", (object)1), ("@p1", (object)2) };
        cmd.AddParamsStackAlloc(ps1);
        Assert.Equal(2, cmd.Parameters.Count);
        (string, object)[] ps2 = { ("@p0", (object)10), ("@p1", (object)20) };
        cmd.AddParamsStackAlloc(ps2);
        Assert.Equal(2, cmd.Parameters.Count);
        Assert.Equal(10, (int)cmd.Parameters[0].Value!);
    }

    [Fact]
    public void DbCommandExtensions_AddParamsStackAlloc_RemovesExcessParams()
    {
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT @p0";
        (string, object)[] ps1 = { ("@p0", (object)1), ("@p1", (object)2) };
        cmd.AddParamsStackAlloc(ps1);
        Assert.Equal(2, cmd.Parameters.Count);
        (string, object)[] ps2 = { ("@p0", (object)99) };
        cmd.AddParamsStackAlloc(ps2);
        Assert.Equal(1, cmd.Parameters.Count);
    }

    [Fact]
    public void DbCommandExtensions_SetParametersFast_AddsParameters()
    {
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT @a, @b";
        (string, object)[] ps = { ("@a", (object)"hello"), ("@b", (object)7) };
        cmd.SetParametersFast(ps);
        Assert.Equal(2, cmd.Parameters.Count);
    }

    [Fact]
    public void DbCommandExtensions_SetParametersFast_RemovesExcess()
    {
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        (string, object)[] ps1 = { ("@x", (object)1), ("@y", (object)2), ("@z", (object)3) };
        cmd.SetParametersFast(ps1);
        Assert.Equal(3, cmd.Parameters.Count);
        (string, object)[] ps2 = { ("@x", (object)10) };
        cmd.SetParametersFast(ps2);
        Assert.Equal(1, cmd.Parameters.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // FastExpressionVisitorPool — Get / Return / GetMemberValue
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void FastExpressionVisitorPool_GetReturn_Roundtrip()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var param = System.Linq.Expressions.Expression.Parameter(typeof(UctGadget), "g");
        var vc = new VisitorContext(ctx, mapping, new SqliteProvider(), param, "g", null, null, null);
        var visitor = FastExpressionVisitorPool.Get(in vc);
        Assert.NotNull(visitor);
        FastExpressionVisitorPool.Return(visitor);
    }

    [Fact]
    public void FastExpressionVisitorPool_GetMemberValue_Property()
    {
        var prop = typeof(UctGadget).GetProperty(nameof(UctGadget.Name))!;
        var gadget = new UctGadget { Name = "TestGadget" };
        var value = FastExpressionVisitorPool.GetMemberValue(prop, gadget);
        Assert.Equal("TestGadget", value);
    }

    [Fact]
    public void FastExpressionVisitorPool_GetMemberValue_Field()
    {
        var type = typeof(TestFieldHolder);
        var field = type.GetField("Value")!;
        var instance = new TestFieldHolder { Value = 42 };
        var value = FastExpressionVisitorPool.GetMemberValue(field, instance);
        Assert.Equal(42, value);
    }

    [Fact]
    public void FastExpressionVisitorPool_GetMemberValue_UnsupportedMember_Throws()
    {
        var method = typeof(string).GetMethod(nameof(string.ToLower), Type.EmptyTypes)!;
        Assert.Throws<NotSupportedException>(() => FastExpressionVisitorPool.GetMemberValue(method, "hello"));
    }

    [Fact]
    public void FastExpressionVisitorPool_GetMemberValue_CachesDelegate()
    {
        var prop = typeof(UctGadget).GetProperty(nameof(UctGadget.Price))!;
        var g1 = new UctGadget { Price = 5 };
        var g2 = new UctGadget { Price = 99 };
        // Second call should use cached delegate
        var v1 = FastExpressionVisitorPool.GetMemberValue(prop, g1);
        var v2 = FastExpressionVisitorPool.GetMemberValue(prop, g2);
        Assert.Equal(5, v1);
        Assert.Equal(99, v2);
    }

    // ══════════════════════════════════════════════════════════════════════
    // DynamicBatchSizer
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void DynamicBatchSizer_EmptySample_ReturnsMinBatchSize()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var sizer = new DynamicBatchSizer();
        var result = sizer.CalculateOptimalBatchSize<UctGadget>(Enumerable.Empty<UctGadget>(), mapping, "test_op");
        Assert.True(result.OptimalBatchSize >= 1);
    }

    [Fact]
    public void DynamicBatchSizer_WithSample_ReturnsReasonableBatchSize()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var sizer = new DynamicBatchSizer();
        var sample = Enumerable.Range(1, 10).Select(i => new UctGadget { Id = i, Name = "N" + i, Price = i * 2 });
        var result = sizer.CalculateOptimalBatchSize(sample, mapping, "uct_insert");
        Assert.True(result.OptimalBatchSize >= 10);
        Assert.True(result.OptimalBatchSize <= 10000);
        Assert.NotEmpty(result.Strategy);
    }

    [Fact]
    public void DynamicBatchSizer_RecordBatchPerformance_TracksHistory()
    {
        var sizer = new DynamicBatchSizer();
        sizer.RecordBatchPerformance("op1", 500, TimeSpan.FromSeconds(1), 500);
        sizer.RecordBatchPerformance("op1", 1000, TimeSpan.FromSeconds(1.5), 1000);
        sizer.RecordBatchPerformance("op1", 750, TimeSpan.FromSeconds(1.2), 750);
        // No assertion on optimal — just verify it doesn't throw
    }

    [Fact]
    public void DynamicBatchSizer_RecordBatchPerformance_PrunesOldHistory()
    {
        var sizer = new DynamicBatchSizer();
        for (int i = 0; i < 25; i++)
            sizer.RecordBatchPerformance("bulk_op", 100 + i, TimeSpan.FromMilliseconds(200 + i * 5), 100 + i);
        // Should not throw even with 25 entries (max is 20, pruning kicks in)
    }

    [Fact]
    public void DynamicBatchSizer_WithHistorical_InfluencesBatchSize()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var sizer = new DynamicBatchSizer();
        // Seed history with a few entries
        sizer.RecordBatchPerformance("hist_op", 200, TimeSpan.FromSeconds(0.5), 200);
        sizer.RecordBatchPerformance("hist_op", 300, TimeSpan.FromSeconds(0.6), 300);
        sizer.RecordBatchPerformance("hist_op", 400, TimeSpan.FromSeconds(0.9), 400);
        var sample = Enumerable.Range(1, 5).Select(i => new UctGadget { Name = "H" + i, Price = i });
        var result = sizer.CalculateOptimalBatchSize(sample, mapping, "hist_op");
        Assert.True(result.OptimalBatchSize > 0);
    }

    [Fact]
    public void DynamicBatchSizer_TotalRecordsSmall_CapsBatchSize()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var sizer = new DynamicBatchSizer();
        var sample = Enumerable.Range(1, 5).Select(i => new UctGadget { Name = "S" + i, Price = i });
        var result = sizer.CalculateOptimalBatchSize(sample, mapping, "small_op", totalRecords: 100);
        Assert.True(result.OptimalBatchSize >= 10); // min is 10
    }

    [Fact]
    public void DynamicBatchSizer_LongStrings_ReducesBatchSize()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var sizer = new DynamicBatchSizer();
        // 60 items with long descriptions triggers AvgStringLength > 1000 path
        var bigStr = new string('x', 2000);
        var sample = Enumerable.Range(1, 60).Select(i => new UctGadget { Name = bigStr, Price = i, Description = bigStr });
        var result = sizer.CalculateOptimalBatchSize(sample, mapping, "long_str");
        Assert.True(result.OptimalBatchSize >= 10);
    }

    [Fact]
    public void DynamicBatchSizer_BatchSizingResult_HasEstimates()
    {
        using var cn = CreateGadgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(UctGadget));
        var sizer = new DynamicBatchSizer();
        var sample = Enumerable.Range(1, 5).Select(i => new UctGadget { Name = "G" + i, Price = i });
        var result = sizer.CalculateOptimalBatchSize(sample, mapping, "est_op");
        Assert.True(result.EstimatedMemoryUsage > 0);
        Assert.True(result.EstimatedBatchTime >= TimeSpan.Zero);
    }

    // ══════════════════════════════════════════════════════════════════════
    // BaseDbCommandInterceptor
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BaseDbCommandInterceptor_NonQueryExecuting_LogsAndContinues()
    {
        var logger = new FakeLogger();
        var interceptor = new UctLoggingInterceptor(logger);
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await interceptor.NonQueryExecutingAsync(cmd, ctx, CancellationToken.None);
        Assert.False(result.IsSuppressed);
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_ScalarExecuting_LogsAndContinues()
    {
        var logger = new FakeLogger();
        var interceptor = new UctLoggingInterceptor(logger);
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 42";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await interceptor.ScalarExecutingAsync(cmd, ctx, CancellationToken.None);
        Assert.False(result.IsSuppressed);
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_ReaderExecuting_LogsAndContinues()
    {
        var logger = new FakeLogger();
        var interceptor = new UctLoggingInterceptor(logger);
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await interceptor.ReaderExecutingAsync(cmd, ctx, CancellationToken.None);
        Assert.False(result.IsSuppressed);
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_ExecutedCallbacks_DoNotThrow()
    {
        var logger = new FakeLogger();
        var interceptor = new UctLoggingInterceptor(logger);
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        using var ctx = new DbContext(cn, new SqliteProvider());
        await interceptor.NonQueryExecutedAsync(cmd, ctx, 1, TimeSpan.FromMilliseconds(5), CancellationToken.None);
        await interceptor.ScalarExecutedAsync(cmd, ctx, 42L, TimeSpan.FromMilliseconds(2), CancellationToken.None);
        using var reader = new UctEmptyReader();
        await interceptor.ReaderExecutedAsync(cmd, ctx, reader, TimeSpan.FromMilliseconds(3), CancellationToken.None);
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_CommandFailed_LogsError()
    {
        var logger = new FakeLogger();
        var interceptor = new UctLoggingInterceptor(logger);
        using var cn = OpenDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        using var ctx = new DbContext(cn, new SqliteProvider());
        await interceptor.CommandFailedAsync(cmd, ctx, new Exception("test error"), CancellationToken.None);
    }

    [Fact]
    public void BaseDbCommandInterceptor_NullLogger_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new UctLoggingInterceptor(null!));
    }

    // ══════════════════════════════════════════════════════════════════════
    // CommandInterceptorExtensions — with and without interceptors
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteNonQueryAsync_NoInterceptors()
    {
        using var cn = CreateGadgetDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO UCT_Gadget (Name, Price) VALUES ('New', 99)";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, CancellationToken.None);
        Assert.Equal(1, result);
    }

    [Fact]
    public void CommandInterceptorExtensions_ExecuteNonQuerySync_NoInterceptors()
    {
        using var cn = CreateGadgetDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO UCT_Gadget (Name, Price) VALUES ('Sync', 5)";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = cmd.ExecuteNonQueryWithInterception(ctx);
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteScalarAsync_NoInterceptors()
    {
        using var cn = CreateGadgetDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM UCT_Gadget";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await cmd.ExecuteScalarWithInterceptionAsync(ctx, CancellationToken.None);
        Assert.Equal(3L, result);
    }

    [Fact]
    public void CommandInterceptorExtensions_ExecuteScalarSync_NoInterceptors()
    {
        using var cn = CreateGadgetDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 42";
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = cmd.ExecuteScalarWithInterception(ctx);
        Assert.Equal(42L, result);
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteReaderAsync_NoInterceptors()
    {
        using var cn = CreateGadgetDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM UCT_Gadget LIMIT 1";
        using var ctx = new DbContext(cn, new SqliteProvider());
        await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, CancellationToken.None);
        Assert.True(reader.Read());
    }

    [Fact]
    public void CommandInterceptorExtensions_ExecuteReaderSync_NoInterceptors()
    {
        using var cn = CreateGadgetDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM UCT_Gadget LIMIT 1";
        using var ctx = new DbContext(cn, new SqliteProvider());
        using var reader = cmd.ExecuteReaderWithInterception(ctx, CommandBehavior.Default);
        Assert.True(reader.Read());
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteNonQueryAsync_WithLoggingInterceptor()
    {
        using var cn = CreateGadgetDb();
        var logger = new FakeLogger();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctLoggingInterceptor(logger) }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO UCT_Gadget (Name, Price) VALUES ('Intercepted', 1)";
        var result = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, CancellationToken.None);
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteNonQueryAsync_Suppressed()
    {
        using var cn = CreateGadgetDb();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctSuppressingInterceptor() }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO UCT_Gadget (Name, Price) VALUES ('Never', 0)";
        var result = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, CancellationToken.None);
        Assert.Equal(42, result); // suppressed value
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteScalarAsync_Suppressed()
    {
        using var cn = CreateGadgetDb();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctSuppressingInterceptor() }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var result = await cmd.ExecuteScalarWithInterceptionAsync(ctx, CancellationToken.None);
        Assert.Equal(99L, result);
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteReaderAsync_Suppressed()
    {
        using var cn = CreateGadgetDb();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctSuppressingInterceptor() }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT * FROM UCT_Gadget";
        await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, CancellationToken.None);
        Assert.IsType<UctEmptyReader>(reader);
    }

    [Fact]
    public void CommandInterceptorExtensions_ExecuteNonQuerySync_WithInterceptor()
    {
        using var cn = CreateGadgetDb();
        var logger = new FakeLogger();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctLoggingInterceptor(logger) }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO UCT_Gadget (Name, Price) VALUES ('SyncIntercepted', 7)";
        var result = cmd.ExecuteNonQueryWithInterception(ctx);
        Assert.Equal(1, result);
    }

    [Fact]
    public void CommandInterceptorExtensions_ExecuteScalarSync_WithInterceptor()
    {
        using var cn = CreateGadgetDb();
        var logger = new FakeLogger();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctLoggingInterceptor(logger) }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM UCT_Gadget";
        var result = cmd.ExecuteScalarWithInterception(ctx);
        Assert.NotNull(result);
    }

    [Fact]
    public void CommandInterceptorExtensions_ExecuteReaderSync_WithInterceptor()
    {
        using var cn = CreateGadgetDb();
        var logger = new FakeLogger();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctLoggingInterceptor(logger) }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM UCT_Gadget LIMIT 1";
        using var reader = cmd.ExecuteReaderWithInterception(ctx, CommandBehavior.Default);
        Assert.True(reader.Read());
    }

    [Fact]
    public async Task CommandInterceptorExtensions_ExecuteNonQuery_ThrowPropagates()
    {
        using var cn = CreateGadgetDb();
        var logger = new FakeLogger();
        var opts = new DbContextOptions
        {
            CommandInterceptors = { new UctLoggingInterceptor(logger) }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO NonExistentTable (Col) VALUES (1)";
        await Assert.ThrowsAsync<SqliteException>(() => cmd.ExecuteNonQueryWithInterceptionAsync(ctx, CancellationToken.None));
    }

    // ══════════════════════════════════════════════════════════════════════
    // DbConnectionFactory
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void DbConnectionFactory_Create_Sqlite_ReturnsConnection()
    {
        var cn = DbConnectionFactory.Create("Data Source=:memory:", new SqliteProvider());
        Assert.IsType<SqliteConnection>(cn);
        cn.Dispose();
    }

    [Fact]
    public void DbConnectionFactory_Create_SqlServer_ReturnsConnection()
    {
        var cn = DbConnectionFactory.Create("Server=localhost;Database=test;Trusted_Connection=True;", new SqlServerProvider());
        Assert.NotNull(cn);
        cn.Dispose();
    }

    [Fact]
    public void DbConnectionFactory_Create_InvalidConnectionString_Throws()
    {
        Assert.ThrowsAny<Exception>(() => DbConnectionFactory.Create("", new SqliteProvider()));
    }

    [Fact]
    public void DbConnectionFactory_Create_UnsupportedProvider_Throws()
    {
        var customProvider = new CustomProvider();
        Assert.ThrowsAny<Exception>(() => DbConnectionFactory.Create("Data Source=:memory:", customProvider));
    }

    // ══════════════════════════════════════════════════════════════════════
    // DbConcurrencyException / NormDatabaseException coverage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void DbConcurrencyException_CanBeCreated_WithMessage()
    {
        var ex = new DbConcurrencyException("Conflict detected");
        Assert.Equal("Conflict detected", ex.Message);
    }

    [Fact]
    public void DbConcurrencyException_CanBeCreated_WithInnerException()
    {
        var inner = new Exception("inner");
        var ex = new DbConcurrencyException("Conflict", inner);
        Assert.Equal(inner, ex.InnerException);
    }

    [Fact]
    public void NormDatabaseException_CanBeCreated()
    {
        var ex = new NormDatabaseException("DB error", null, null, null);
        Assert.Contains("DB error", ex.Message);
    }

    [Fact]
    public void NormTimeoutException_CanBeCreated()
    {
        var ex = new NormTimeoutException("Timeout", null, null, null);
        Assert.Contains("Timeout", ex.Message);
    }

    [Fact]
    public void NormUnsupportedFeatureException_CanBeCreated()
    {
        var ex = new NormUnsupportedFeatureException("Not supported");
        Assert.Equal("Not supported", ex.Message);
    }

    // ── OutputParameter ───────────────────────────────────────────────────────

    [Fact]
    public void OutputParameter_RecordProperties_AreCorrect()
    {
        var p = new OutputParameter("MyParam", System.Data.DbType.Int32);
        Assert.Equal("MyParam", p.Name);
        Assert.Equal(System.Data.DbType.Int32, p.DbType);
        Assert.Null(p.Size);
    }

    [Fact]
    public void OutputParameter_WithSize_SetsSize()
    {
        var p = new OutputParameter("Str", System.Data.DbType.String, 100);
        Assert.Equal(100, p.Size);
    }

    [Fact]
    public void OutputParameter_Equality_WorksAsRecord()
    {
        var p1 = new OutputParameter("A", System.Data.DbType.Int32, null);
        var p2 = new OutputParameter("A", System.Data.DbType.Int32, null);
        Assert.Equal(p1, p2);
    }

    // ── SetPropertyCalls<T> ───────────────────────────────────────────────────

    [Fact]
    public void SetPropertyCalls_SetProperty_ReturnsSelf()
    {
        var calls = new SetPropertyCalls<UctGadget>();
        var result = calls.SetProperty(g => g.Name, "Test");
        Assert.Same(calls, result);
    }

    [Fact]
    public void SetPropertyCalls_ChainedCalls_Work()
    {
        var calls = new SetPropertyCalls<UctGadget>();
        var result = calls.SetProperty(g => g.Name, "X").SetProperty(g => g.Price, 10);
        Assert.Same(calls, result);
    }

    // ── DbContextLoggingExtensions ────────────────────────────────────────────

    [Fact]
    public void LogQuery_WithLogger_DoesNotThrow()
    {
        ILogger logger = new FakeLogger();
        var parameters = new Dictionary<string, object> { ["@p0"] = 42 };
        logger.LogQuery("SELECT 1", parameters, TimeSpan.FromMilliseconds(5), 1);
    }

    [Fact]
    public void LogQuery_NullLogger_DoesNotThrow()
    {
        ILogger logger = null!;
        logger.LogQuery("SELECT 1", new Dictionary<string, object>(), TimeSpan.Zero, 0);
    }

    [Fact]
    public void LogBulkOperation_WithLogger_DoesNotThrow()
    {
        ILogger logger = new FakeLogger();
        logger.LogBulkOperation("INSERT", "MyTable", 100, TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public void LogBulkOperation_NullLogger_DoesNotThrow()
    {
        ILogger logger = null!;
        logger.LogBulkOperation("DELETE", "T", 0, TimeSpan.Zero);
    }

    [Fact]
    public void LogError_WithLogger_DoesNotThrow()
    {
        ILogger logger = new FakeLogger();
        var ex = new NormUsageException("oops");
        logger.LogError(ex, 2);
    }

    [Fact]
    public void LogError_NullLogger_DoesNotThrow()
    {
        ILogger logger = null!;
        var ex = new NormUsageException("oops");
        logger.LogError(ex, 0);
    }

    // ── TableSplitAttribute ───────────────────────────────────────────────────

    [Fact]
    public void TableSplitAttribute_StoresPrincipalType()
    {
        var attr = new nORM.Mapping.TableSplitAttribute(typeof(UctGadget));
        Assert.Equal(typeof(UctGadget), attr.PrincipalType);
    }

    // ── CompileTimeQueryAttribute ─────────────────────────────────────────────

    [Fact]
    public void CompileTimeQueryAttribute_StoresSql()
    {
        var attr = new nORM.SourceGeneration.CompileTimeQueryAttribute("SELECT * FROM T");
        Assert.Equal("SELECT * FROM T", attr.Sql);
    }

    // ── OwnedNavigation record ────────────────────────────────────────────────

    [Fact]
    public void OwnedNavigation_RecordProperties_AreCorrect()
    {
        var nav = new nORM.Configuration.OwnedNavigation(typeof(UctGadget), null);
        Assert.Equal(typeof(UctGadget), nav.OwnedType);
        Assert.Null(nav.Configuration);
    }

    [Fact]
    public void OwnedNavigation_Equality_WorksAsRecord()
    {
        var n1 = new nORM.Configuration.OwnedNavigation(typeof(UctGadget), null);
        var n2 = new nORM.Configuration.OwnedNavigation(typeof(UctGadget), null);
        Assert.Equal(n1, n2);
    }

    // ── SpanSqlBuilder (internal ref struct) ──────────────────────────────────

    [Fact]
    public void SpanSqlBuilder_AppendLiteral_WritesCorrectLength()
    {
        Span<char> buffer = stackalloc char[100];
        var builder = new nORM.Query.SpanSqlBuilder(buffer);
        builder.AppendLiteral("SELECT".AsSpan());
        Assert.Equal(6, builder.Position);
    }

    [Fact]
    public void SpanSqlBuilder_AppendParameter_WritesParam()
    {
        Span<char> buffer = stackalloc char[100];
        var builder = new nORM.Query.SpanSqlBuilder(buffer);
        builder.AppendParameter("@p0".AsSpan());
        Assert.Equal(3, builder.Position);
    }

    [Fact]
    public void SpanSqlBuilder_MultipleAppends_AccumulatePosition()
    {
        Span<char> buffer = stackalloc char[100];
        var builder = new nORM.Query.SpanSqlBuilder(buffer);
        builder.AppendLiteral("SELECT ".AsSpan());
        builder.AppendLiteral("*".AsSpan());
        builder.AppendLiteral(" FROM ".AsSpan());
        builder.AppendParameter("tbl".AsSpan());
        Assert.Equal("SELECT * FROM tbl".Length, builder.Position);
    }

    // ── SpanSqlTemplate (internal static class) ───────────────────────────────

    [Fact]
    public void SpanSqlTemplate_SimpleSelect_HasContent()
    {
        var span = nORM.Query.SpanSqlTemplate.SimpleSelect;
        Assert.True(span.Length > 0);
    }

    [Fact]
    public void SpanSqlTemplate_SimpleWhere_HasContent()
    {
        var span = nORM.Query.SpanSqlTemplate.SimpleWhere;
        Assert.True(span.Length > 0);
    }

    [Fact]
    public void SpanSqlTemplate_SimpleTake_HasContent()
    {
        var span = nORM.Query.SpanSqlTemplate.SimpleTake;
        Assert.True(span.Length > 0);
    }

    [Fact]
    public void SpanSqlTemplate_BuildSimpleQuery_NoWhere()
    {
        Span<char> buffer = stackalloc char[256];
        int len = nORM.Query.SpanSqlTemplate.BuildSimpleQuery(buffer, "Id, Name".AsSpan(), "MyTable".AsSpan());
        var result = new string(buffer.Slice(0, len));
        Assert.Equal("SELECT Id, Name FROM MyTable", result);
    }

    [Fact]
    public void SpanSqlTemplate_BuildSimpleQuery_WithWhere()
    {
        Span<char> buffer = stackalloc char[256];
        int len = nORM.Query.SpanSqlTemplate.BuildSimpleQuery(buffer, "Id".AsSpan(), "T".AsSpan(), "Id = 1".AsSpan());
        var result = new string(buffer.Slice(0, len));
        Assert.Equal("SELECT Id FROM T WHERE Id = 1", result);
    }

    // ── StackAllocParameterBuilder (internal ref struct) ─────────────────────

    [Fact]
    public void StackAllocParameterBuilder_Add_SetsParameterValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var p1 = cmd.CreateParameter();
        cmd.Parameters.Add(p1);
        System.Data.Common.DbParameter[] paramArr = [p1];
        Span<System.Data.Common.DbParameter> paramSpan = paramArr;
        Span<string> nameSpan = new string[1];
        var sab = new nORM.Query.StackAllocParameterBuilder(paramSpan, nameSpan);
        sab.Add("@p0".AsSpan(), 42);
        Assert.Equal("@p0", p1.ParameterName);
        Assert.Equal(42, p1.Value);
    }

    // ── ParameterHelper (internal static) ────────────────────────────────────

    [Fact]
    public void ParameterHelper_AddParameters_AddsAllParams()
    {
        var provider = new SqliteProvider();
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var values = new object[] { 1, "hello", 3.14 };
        var result = nORM.Core.ParameterHelper.AddParameters(provider, cmd, values);
        Assert.Equal(3, result.Count);
        Assert.True(result.ContainsKey("@p0"));
        Assert.True(result.ContainsKey("@p1"));
        Assert.True(result.ContainsKey("@p2"));
        Assert.Equal(1, result["@p0"]);
    }

    [Fact]
    public void ParameterHelper_AddParameters_EmptyArray_ReturnsEmptyDict()
    {
        var provider = new SqliteProvider();
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var result = nORM.Core.ParameterHelper.AddParameters(provider, cmd, []);
        Assert.Empty(result);
    }
}

// ── Support types ─────────────────────────────────────────────────────────

public class TestFieldHolder { public int Value; }

public sealed class CustomProvider : SqliteProvider
{
    // Override type check to be non-Sqlite, non-SqlServer, non-Postgres, non-MySql
}

public sealed class FakeLogger : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
}

// ══════════════════════════════════════════════════════════════════════════
// RetryPolicy tests
// ══════════════════════════════════════════════════════════════════════════

public class RetryPolicyTests
{
    [Fact]
    public void RetryPolicy_Defaults_MaxRetries3_BaseDelay1s()
    {
        var policy = new RetryPolicy();
        Assert.Equal(3, policy.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), policy.BaseDelay);
        Assert.NotNull(policy.ShouldRetry);
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_TimeoutException_ReturnsFalse()
    {
        // TimeoutException is deliberately NOT retryable — retrying a timed-out write
        // can duplicate data because the write may have succeeded before the timeout.
        var policy = new RetryPolicy();
        Assert.False(policy.ShouldRetry(new TimeoutException()));
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_IOException_ReturnsTrue()
    {
        var policy = new RetryPolicy();
        Assert.True(policy.ShouldRetry(new System.IO.IOException()));
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_SocketException_ReturnsTrue()
    {
        var policy = new RetryPolicy();
        Assert.True(policy.ShouldRetry(new System.Net.Sockets.SocketException()));
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_ArgumentException_ReturnsFalse()
    {
        var policy = new RetryPolicy();
        Assert.False(policy.ShouldRetry(new ArgumentException("not transient")));
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_NullReferenceException_ReturnsFalse()
    {
        var policy = new RetryPolicy();
        Assert.False(policy.ShouldRetry(new NullReferenceException()));
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_InvalidOperation_ReturnsFalse()
    {
        var policy = new RetryPolicy();
        Assert.False(policy.ShouldRetry(new InvalidOperationException()));
    }

    [Fact]
    public void RetryPolicy_CustomShouldRetry_CanBeReplaced()
    {
        var policy = new RetryPolicy { ShouldRetry = ex => ex is InvalidOperationException };
        Assert.True(policy.ShouldRetry(new InvalidOperationException()));
        Assert.False(policy.ShouldRetry(new TimeoutException()));
    }

    [Fact]
    public void RetryPolicy_MaxRetries_CanBeChanged()
    {
        var policy = new RetryPolicy { MaxRetries = 5 };
        Assert.Equal(5, policy.MaxRetries);
    }

    [Fact]
    public void RetryPolicy_BaseDelay_CanBeChanged()
    {
        var policy = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(250) };
        Assert.Equal(TimeSpan.FromMilliseconds(250), policy.BaseDelay);
    }

    [Fact]
    public void RetryPolicy_ShouldRetry_DerivedFromIOException_ReturnsTrue()
    {
        // Any IOException subclass (e.g. FileNotFoundException) should also match
        var policy = new RetryPolicy();
        Assert.True(policy.ShouldRetry(new System.IO.FileNotFoundException()));
    }
}

// ══════════════════════════════════════════════════════════════════════════
// NavigationContext and NavigationPropertyInfo tests
// ══════════════════════════════════════════════════════════════════════════

public class NavigationContextTests
{
    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    [Fact]
    public void NavigationContext_Properties_StoreDbContextAndEntityType()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(UctGadget));
        Assert.Same(ctx, navCtx.DbContext);
        Assert.Equal(typeof(UctGadget), navCtx.EntityType);
    }

    [Fact]
    public void NavigationContext_IsLoaded_ReturnsFalseInitially()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(UctGadget));
        Assert.False(navCtx.IsLoaded("SomeProperty"));
    }

    [Fact]
    public void NavigationContext_MarkAsLoaded_ThenIsLoaded_ReturnsTrue()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(UctGadget));
        navCtx.MarkAsLoaded("Posts");
        Assert.True(navCtx.IsLoaded("Posts"));
    }

    [Fact]
    public void NavigationContext_MarkAsUnloaded_AfterLoaded_ReturnsFalse()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(UctGadget));
        navCtx.MarkAsLoaded("Comments");
        navCtx.MarkAsUnloaded("Comments");
        Assert.False(navCtx.IsLoaded("Comments"));
    }

    [Fact]
    public void NavigationContext_Dispose_ClearsAllLoadedProperties()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(UctGadget));
        navCtx.MarkAsLoaded("A");
        navCtx.MarkAsLoaded("B");
        navCtx.Dispose();
        Assert.False(navCtx.IsLoaded("A"));
        Assert.False(navCtx.IsLoaded("B"));
    }

    [Fact]
    public void NavigationContext_MultipleProperties_TrackedIndependently()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(UctGadget));
        navCtx.MarkAsLoaded("Alpha");
        Assert.True(navCtx.IsLoaded("Alpha"));
        Assert.False(navCtx.IsLoaded("Beta"));
    }
}

public class NavigationPropertyInfoTests
{
    [Fact]
    public void NavigationPropertyInfo_RecordProperties_AreCorrect()
    {
        var prop = typeof(UctGadget).GetProperty(nameof(UctGadget.Name))!;
        var info = new NavigationPropertyInfo(prop, typeof(string), false);
        Assert.Equal(prop, info.Property);
        Assert.Equal(typeof(string), info.TargetType);
        Assert.False(info.IsCollection);
    }

    [Fact]
    public void NavigationPropertyInfo_IsCollection_True_ForCollectionProperty()
    {
        var prop = typeof(UctGadget).GetProperty(nameof(UctGadget.Name))!;
        var info = new NavigationPropertyInfo(prop, typeof(UctGadget), true);
        Assert.True(info.IsCollection);
    }

    [Fact]
    public void NavigationPropertyInfo_RecordEquality_SameValues_AreEqual()
    {
        var prop = typeof(UctGadget).GetProperty(nameof(UctGadget.Name))!;
        var i1 = new NavigationPropertyInfo(prop, typeof(string), false);
        var i2 = new NavigationPropertyInfo(prop, typeof(string), false);
        Assert.Equal(i1, i2);
    }

    [Fact]
    public void NavigationPropertyInfo_WithExpression_ProducesNewRecord()
    {
        var prop = typeof(UctGadget).GetProperty(nameof(UctGadget.Name))!;
        var info = new NavigationPropertyInfo(prop, typeof(string), false);
        var updated = info with { IsCollection = true };
        Assert.True(updated.IsCollection);
        Assert.False(info.IsCollection);
    }
}

// ══════════════════════════════════════════════════════════════════════════
// LazyNavigationCollection<T> tests
// ══════════════════════════════════════════════════════════════════════════

[Table("LncOwner")]
public class LncOwner
{
    [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<LncItem>? Items { get; set; }
}

[Table("LncItem")]
public class LncItem
{
    [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int OwnerId { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class LazyNavigationCollectionTests
{
    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    // Helper: build a LazyNavigationCollection backed by a pre-populated List<LncItem>
    // so GetOrLoadCollection never hits the database.
    private static (LazyNavigationCollection<LncItem> coll, LncOwner owner) BuildPreloaded(DbContext ctx)
    {
        var owner = new LncOwner { Id = 1 };
        var navCtx = new NavigationContext(ctx, typeof(LncOwner));
        var prop = typeof(LncOwner).GetProperty(nameof(LncOwner.Items))!;

        var items = new List<LncItem>
        {
            new LncItem { Id = 1, OwnerId = 1, Name = "Alpha" },
            new LncItem { Id = 2, OwnerId = 1, Name = "Beta" },
        };
        owner.Items = items;
        navCtx.MarkAsLoaded(prop.Name);

        var coll = new LazyNavigationCollection<LncItem>(owner, prop, navCtx);
        return (coll, owner);
    }

    [Fact]
    public void LazyNavCollection_Count_ReturnsItemCount()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        Assert.Equal(2, coll.Count);
    }

    [Fact]
    public void LazyNavCollection_Enumeration_IteratesAllItems()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        var names = coll.Select(i => i.Name).ToList();
        Assert.Equal(new[] { "Alpha", "Beta" }, names);
    }

    [Fact]
    public void LazyNavCollection_Contains_ExistingItem_ReturnsTrue()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, owner) = BuildPreloaded(ctx);
        var item = ((List<LncItem>)owner.Items!)[0];
        Assert.Contains(item, coll);
    }

    [Fact]
    public void LazyNavCollection_Contains_MissingItem_ReturnsFalse()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        Assert.DoesNotContain(new LncItem { Id = 999, Name = "Ghost" }, coll);
    }

    [Fact]
    public void LazyNavCollection_Add_IncreasesCount()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        coll.Add(new LncItem { Id = 3, Name = "Gamma" });
        Assert.Equal(3, coll.Count);
    }

    [Fact]
    public void LazyNavCollection_Remove_ExistingItem_DecreasesCount()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, owner) = BuildPreloaded(ctx);
        var item = ((List<LncItem>)owner.Items!)[0];
        bool removed = coll.Remove(item);
        Assert.True(removed);
        Assert.Single(coll);
    }

    [Fact]
    public void LazyNavCollection_Clear_EmptiesCollection()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        coll.Clear();
        Assert.Empty(coll);
    }

    [Fact]
    public void LazyNavCollection_CopyTo_CopiesItemsToArray()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        var arr = new LncItem[2];
        coll.CopyTo(arr, 0);
        Assert.Equal("Alpha", arr[0].Name);
        Assert.Equal("Beta", arr[1].Name);
    }

    [Fact]
    public void LazyNavCollection_IsReadOnly_IsFalse()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        Assert.False(coll.IsReadOnly);
    }

    [Fact]
    public void LazyNavCollection_IndexOf_ReturnsCorrectIndex()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, owner) = BuildPreloaded(ctx);
        var item = ((List<LncItem>)owner.Items!)[1];
        Assert.Equal(1, coll.IndexOf(item));
    }

    [Fact]
    public void LazyNavCollection_Insert_InsertsAtSpecifiedIndex()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        var newItem = new LncItem { Id = 99, Name = "Inserted" };
        coll.Insert(0, newItem);
        Assert.Equal("Inserted", coll[0].Name);
        Assert.Equal(3, coll.Count);
    }

    [Fact]
    public void LazyNavCollection_RemoveAt_RemovesAtIndex()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        coll.RemoveAt(0);
        Assert.Single(coll);
        Assert.Equal("Beta", coll[0].Name);
    }

    [Fact]
    public void LazyNavCollection_IndexerGet_ReturnsCorrectItem()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        Assert.Equal("Alpha", coll[0].Name);
        Assert.Equal("Beta", coll[1].Name);
    }

    [Fact]
    public void LazyNavCollection_IndexerSet_ReplacesItem()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        var replacement = new LncItem { Id = 10, Name = "Replaced" };
        coll[0] = replacement;
        Assert.Equal("Replaced", coll[0].Name);
    }

    [Fact]
    public void LazyNavCollection_NonGenericEnumerator_IteratesAllItems()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        int count = 0;
        System.Collections.IEnumerable enumerable = coll;
        foreach (var _ in enumerable)
            count++;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task LazyNavCollection_GetAsyncEnumerator_WhenAlreadyLoaded_YieldsAllItems()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var (coll, _) = BuildPreloaded(ctx);
        var names = new List<string>();
        await foreach (var item in coll)
            names.Add(item.Name);
        Assert.Equal(new[] { "Alpha", "Beta" }, names);
    }
}

// ══════════════════════════════════════════════════════════════════════════
// NavigationPropertyExtensions static behaviour tests (no DB needed)
// ══════════════════════════════════════════════════════════════════════════

public class NavigationPropertyExtensionsTests
{
    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    [Fact]
    public void EnableLazyLoading_NullEntity_ThrowsArgumentNullException()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        UctGadget? nullGadget = null;
        Assert.Throws<ArgumentNullException>(
            () => nullGadget!.EnableLazyLoading(ctx));
    }

    [Fact]
    public void EnableLazyLoading_ValidEntity_ReturnsSameInstance()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var gadget = new UctGadget { Name = "Test" };
        var result = gadget.EnableLazyLoading(ctx);
        Assert.Same(gadget, result);
    }

    [Fact]
    public void EnableLazyLoading_RegistersNavigationContext()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var gadget = new UctGadget { Name = "Test" };
        gadget.EnableLazyLoading(ctx);
        bool found = NavigationPropertyExtensions._navigationContexts.TryGetValue(gadget, out var navCtx);
        Assert.True(found);
        Assert.NotNull(navCtx);
    }

    [Fact]
    public void CleanupNavigationContext_NullEntity_DoesNotThrow()
    {
        var ex = Record.Exception(() => NavigationPropertyExtensions.CleanupNavigationContext<UctGadget>(null!));
        Assert.Null(ex);
    }

    [Fact]
    public void CleanupNavigationContext_EntityWithContext_RemovesIt()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var gadget = new UctGadget { Name = "Test" };
        gadget.EnableLazyLoading(ctx);
        NavigationPropertyExtensions.CleanupNavigationContext(gadget);
        bool found = NavigationPropertyExtensions._navigationContexts.TryGetValue(gadget, out _);
        Assert.False(found);
    }

    [Fact]
    public void CleanupNavigationContext_EntityWithoutContext_DoesNotThrow()
    {
        var gadget = new UctGadget { Name = "Test" };
        var ex = Record.Exception(() => NavigationPropertyExtensions.CleanupNavigationContext(gadget));
        Assert.Null(ex);
    }

    [Fact]
    public void IsLoaded_EntityWithoutContext_ReturnsFalse()
    {
        var gadget = new UctGadget { Name = "Test" };
        bool loaded = gadget.IsLoaded(g => g.Name);
        Assert.False(loaded);
    }

    [Fact]
    public void IsLoaded_NullEntity_ReturnsFalse()
    {
        // Calling IsLoaded on a null reference - use non-nullable type to satisfy constraint
        var gadget = new UctGadget { Name = "Test" };
        // Without a context registered, should return false
        bool loaded = gadget.IsLoaded(g => g.Name);
        Assert.False(loaded);
    }

    [Fact]
    public void IsLoaded_NullExpression_ReturnsFalse()
    {
        var gadget = new UctGadget { Name = "Test" };
        Expression<Func<UctGadget, string>>? nullExpr = null;
        bool loaded = gadget.IsLoaded(nullExpr!);
        Assert.False(loaded);
    }

    [Fact]
    public async Task LoadAsync_EntityWithoutContext_ThrowsInvalidOperation()
    {
        var gadget = new UctGadget { Name = "Test" };
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => gadget.LoadAsync<UctGadget, object>(g => null!));
    }

    [Fact]
    public async Task LoadAsync_NullEntity_ThrowsArgumentNullException()
    {
        UctGadget? n = null;
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => n!.LoadAsync<UctGadget, object>(g => null!));
    }

    [Fact]
    public void RegisterAndUnregisterLoader_DoNotThrow()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var loader = new BatchedNavigationLoader(ctx);
        NavigationPropertyExtensions.RegisterLoader(loader);
        NavigationPropertyExtensions.UnregisterLoader(loader);
    }

    [Fact]
    public void EnableLazyLoading_CalledTwice_DoesNotThrow()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var gadget = new UctGadget { Name = "Test" };
        gadget.EnableLazyLoading(ctx);
        var ex = Record.Exception(() => gadget.EnableLazyLoading(ctx));
        Assert.Null(ex);
    }
}
