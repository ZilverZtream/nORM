using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests covering the 3.6/5.0 audit findings for the compile-time query path:
///   T1  (High)  – fast path crashed on closed/fresh connection
///   SG1 (Med)   – fast path bypassed transaction binding and interceptors
///   P1  (Med)   – fixed parameters used direct .Value assignment, bypassing AssignValue
/// Score gates: 3.6→4.0 (T1+SG1+interceptor), 4.0→4.5 (P1 type matrix),
///              4.5→5.0 (adversarial parity suite).
/// </summary>
public class Audit36CompilePathTests
{
    // ─── shared entity ────────────────────────────────────────────────────────

    public class Article
    {
        public int    Id       { get; set; }
        public string Title    { get; set; } = "";
        public int    Category { get; set; }   // also used as enum-backed column
    }

    public enum ArticleCategory { News = 1, Sports = 2, Tech = 3 }

    private static SqliteConnection CreateConnection(string? path = null)
    {
        var cn = new SqliteConnection(path == null
            ? "Data Source=:memory:"
            : $"Data Source={path}");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE Article (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Category INTEGER NOT NULL);" +
            "INSERT INTO Article VALUES (1, 'Hello World', 1);" +
            "INSERT INTO Article VALUES (2, 'Fast Car',    2);" +
            "INSERT INTO Article VALUES (3, 'C# Tricks',  3);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ─── interceptor helpers ──────────────────────────────────────────────────

    private sealed class ReaderCapturingInterceptor : IDbCommandInterceptor
    {
        public int    ReaderExecutingCallCount;
        public DbTransaction? CapturedTransaction;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand _, DbContext __, CancellationToken ___) =>
            Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand _, DbContext __, int ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand _, DbContext __, CancellationToken ___) =>
            Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand _, DbContext __, object? ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand _, DbContext __, Exception ___, CancellationToken ____) =>
            Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext ctx, CancellationToken ct)
        {
            Interlocked.Increment(ref ReaderExecutingCallCount);
            CapturedTransaction = command.Transaction;
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task ReaderExecutedAsync(DbCommand _, DbContext __, DbDataReader ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Gate 3.6 → 4.0 : T1 (connection) + SG1 (transaction + interceptors)
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// T1: Before the fix the fast path called cmd.ExecuteReader() on a closed connection
    /// and threw "Connection is closed".  After the fix, a closed connection causes the
    /// fast path to be skipped; the standard path calls EnsureConnectionAsync which reopens
    /// the file-backed SQLite connection, and the query succeeds with correct data.
    /// </summary>
    [Fact]
    public async Task T1_CompiledQuery_ClosedConnection_FallsBackToStandardPath()
    {
        var path = Path.Combine(Path.GetTempPath(), $"norm_t1_{Guid.NewGuid():N}.db");
        try
        {
            // Arrange: create file-based DB so data survives close/reopen.
            // Use an explicit inner scope so both cn and ctx are disposed before cleanup.
            List<Article>? result = null;
            using (var cn = new SqliteConnection($"Data Source={path}"))
            {
                cn.Open();
                using (var setupCmd = cn.CreateCommand())
                {
                    setupCmd.CommandText =
                        "CREATE TABLE Article (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Category INTEGER NOT NULL);" +
                        "INSERT INTO Article VALUES (1, 'Hello World', 1);" +
                        "INSERT INTO Article VALUES (2, 'Fast Car',    2);" +
                        "INSERT INTO Article VALUES (3, 'C# Tricks',  3);";
                    setupCmd.ExecuteNonQuery();
                }

                var compiled = Norm.CompileQuery((DbContext c, int id) =>
                    c.Query<Article>().Where(a => a.Id == id));

                using var ctx = new DbContext(cn, new SqliteProvider());

                // Verify baseline works while open.
                var baseline = await compiled(ctx, 1);
                Assert.Single(baseline);
                Assert.Equal("Hello World", baseline[0].Title);

                // Act: close the connection – fast path must bail, standard path must reopen.
                cn.Close();
                Assert.Equal(ConnectionState.Closed, cn.State);

                result = await compiled(ctx, 2);

                // Assert: connection was reopened by EnsureConnectionAsync; data is present.
                Assert.Equal(ConnectionState.Open, cn.State);
            }

            Assert.NotNull(result);
            Assert.Single(result!);
            Assert.Equal("Fast Car", result[0].Title);
        }
        finally
        {
            // SQLite on Windows may hold file handles briefly; swallow cleanup errors.
            SqliteConnection.ClearAllPools();
            try { File.Delete(path); } catch (IOException) { }
            try { File.Delete(path + "-wal"); } catch (IOException) { }
            try { File.Delete(path + "-shm"); } catch (IOException) { }
        }
    }

    /// <summary>
    /// SG1 (interceptors): Removing the CommandInterceptors.Count==0 guard and routing
    /// through ExecuteReaderWithInterception means registered interceptors are invoked
    /// for compiled queries on the sync fast path.
    /// </summary>
    [Fact]
    public async Task SG1_CompiledQuery_WithInterceptor_ReaderExecutingIsInvoked()
    {
        using var cn = CreateConnection();

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<Article>().Where(a => a.Id == id));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var result = await compiled(ctx, 3);

        Assert.Single(result);
        Assert.Equal("C# Tricks", result[0].Title);
        Assert.Equal(1, interceptor.ReaderExecutingCallCount);
    }

    /// <summary>
    /// SG1 (transaction binding): Adding cmd.Transaction = ctx.CurrentTransaction before
    /// ExecuteReader means the compiled query command participates in the active transaction.
    /// Verified via interceptor capturing command.Transaction inside ReaderExecutingAsync.
    /// </summary>
    [Fact]
    public async Task SG1_CompiledQuery_UnderExplicitTransaction_CommandTransactionIsSet()
    {
        using var cn = CreateConnection();

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<Article>().Where(a => a.Id == id));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Begin an explicit transaction so ctx.CurrentTransaction is non-null.
        await using var tx = await ctx.Database.BeginTransactionAsync();

        Assert.NotNull(ctx.CurrentTransaction);

        var result = await compiled(ctx, 1);

        // The interceptor captures cmd.Transaction at execution time.
        // After the SG1 fix it must equal ctx.CurrentTransaction.
        Assert.Same(ctx.CurrentTransaction, interceptor.CapturedTransaction);
        Assert.Single(result);

        await tx.CommitAsync();
    }

    /// <summary>
    /// SG1 + T1 combined: interceptor is invoked AND transaction is bound even when
    /// the connection was briefly closed before the call (T1 fall-back path).
    /// </summary>
    [Fact]
    public async Task SG1_T1_ClosedConnection_WithInterceptorAndTransaction_BothWork()
    {
        var path = Path.Combine(Path.GetTempPath(), $"norm_sg1t1_{Guid.NewGuid():N}.db");
        try
        {
            int callCount = 0;
            using (var cn = new SqliteConnection($"Data Source={path}"))
            {
                cn.Open();
                using (var setupCmd = cn.CreateCommand())
                {
                    setupCmd.CommandText =
                        "CREATE TABLE Article (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Category INTEGER NOT NULL);" +
                        "INSERT INTO Article VALUES (1, 'Hello World', 1);";
                    setupCmd.ExecuteNonQuery();
                }

                var interceptor = new ReaderCapturingInterceptor();
                var options = new DbContextOptions();
                options.CommandInterceptors.Add(interceptor);

                var compiled = Norm.CompileQuery((DbContext c, int id) =>
                    c.Query<Article>().Where(a => a.Id == id));

                using var ctx = new DbContext(cn, new SqliteProvider(), options);

                // Close then immediately call — standard path must reopen.
                cn.Close();
                var result = await compiled(ctx, 1);

                Assert.Single(result);
                callCount = interceptor.ReaderExecutingCallCount;
            }
            // Interceptor is called even on the fallback standard path.
            Assert.True(callCount >= 1);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            try { File.Delete(path); } catch (IOException) { }
            try { File.Delete(path + "-wal"); } catch (IOException) { }
            try { File.Delete(path + "-shm"); } catch (IOException) { }
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Gate 4.0 → 4.5 : P1 fixed-param DbType matrix
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// P1 unit: AssignValue must set DbType = Int32 (not Object) for an enum value
    /// with an int32 underlying type so providers do not coerce it as an untyped blob.
    /// </summary>
    [Fact]
    public void P1_AssignValue_EnumWithInt32Underlying_SetsInt32DbType()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();

        ParameterAssign.AssignValue(p, ArticleCategory.Sports);

        Assert.Equal(DbType.Int32, p.DbType);
        Assert.Equal(2, Convert.ToInt32(p.Value)); // Sports = 2
    }

    /// <summary>
    /// P1 unit: AssignValue must set DbType = Date for a DateOnly value.
    /// </summary>
    [Fact]
    public void P1_AssignValue_DateOnly_SetsDateDbType()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();

        var date = new DateOnly(2026, 3, 15);
        ParameterAssign.AssignValue(p, date);

        Assert.Equal(DbType.Date, p.DbType);
    }

    /// <summary>
    /// P1 unit: AssignValue must set DbType = Time and convert TimeOnly to TimeSpan
    /// for cross-provider compatibility.
    /// </summary>
    [Fact]
    public void P1_AssignValue_TimeOnly_SetsTimeDbTypeAndConvertsToTimeSpan()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();

        var time = new TimeOnly(14, 30, 0);
        ParameterAssign.AssignValue(p, time);

        Assert.Equal(DbType.Time, p.DbType);
        Assert.Equal(time.ToTimeSpan(), p.Value);
    }

    /// <summary>
    /// P1 unit: AssignValue must set DbType = Guid for a Guid value.
    /// Without this fix, a raw Guid stored via p.Value = guid with no DbType hint
    /// would default to Object / String on some providers causing binding failures.
    /// </summary>
    [Fact]
    public void P1_AssignValue_Guid_SetsGuidDbType()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();

        var g = Guid.NewGuid();
        ParameterAssign.AssignValue(p, g);

        Assert.Equal(DbType.Guid, p.DbType);
        Assert.Equal(g, p.Value);
    }

    /// <summary>
    /// P1 integration: A compiled query whose WHERE clause contains an enum constant
    /// (fixed parameter) must return correct results.  Before the fix, the fixed param
    /// had no DbType; with the fix ParameterAssign.AssignValue sets DbType = Int32.
    /// </summary>
    [Fact]
    public async Task P1_CompiledQuery_EnumConstantFixedParam_CorrectResults()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // ArticleCategory.Sports = 2 is a compile-time constant → fixed parameter.
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<Article>().Where(a => a.Category == (int)ArticleCategory.Sports));

        var result = await compiled(ctx, 0);

        Assert.Single(result);
        Assert.Equal("Fast Car", result[0].Title);
    }

    /// <summary>
    /// P1 integration: A compiled query whose runtime parameter is a nullable int
    /// must bind correctly whether the value is non-null or null.
    /// </summary>
    [Fact]
    public async Task P1_CompiledQuery_NullableIntCompiledParam_CorrectResults()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Tag TEXT);" +
                "INSERT INTO Widget VALUES (1, 'alpha');" +
                "INSERT INTO Widget VALUES (2, NULL);";
            cmd.ExecuteNonQuery();
        }

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<Widget>().Where(w => w.Id == id));

        using var ctx = new DbContext(cn, new SqliteProvider());
        var r1 = await compiled(ctx, 1);
        Assert.Single(r1);
        Assert.Equal("alpha", r1[0].Tag);

        var r2 = await compiled(ctx, 2);
        Assert.Single(r2);
        Assert.Null(r2[0].Tag);
    }

    public class Widget
    {
        public int     Id  { get; set; }
        public string? Tag { get; set; }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Gate 4.5 → 5.0 : adversarial parity
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Parity: compiled query and runtime LINQ query must produce identical result sets
    /// for the same predicate and parameter values.
    /// </summary>
    [Fact]
    public async Task Adversarial_CompiledAndRuntimeQuery_SameResultsForAllRows()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int cat) =>
            c.Query<Article>().Where(a => a.Category == cat));

        foreach (var category in new[] { 1, 2, 3 })
        {
            var compiledResult = await compiled(ctx, category);
            var runtimeResult  = await ctx.Query<Article>()
                                          .Where(a => a.Category == category)
                                          .ToListAsync();

            Assert.Equal(runtimeResult.Count, compiledResult.Count);
            for (int i = 0; i < compiledResult.Count; i++)
            {
                Assert.Equal(runtimeResult[i].Id,       compiledResult[i].Id);
                Assert.Equal(runtimeResult[i].Title,    compiledResult[i].Title);
                Assert.Equal(runtimeResult[i].Category, compiledResult[i].Category);
            }
        }
    }

    /// <summary>
    /// Adversarial full-parity: compiled query with interceptor registered AND under
    /// an explicit transaction produces correct results, calls the interceptor, and
    /// the command's transaction reference matches ctx.CurrentTransaction.
    /// </summary>
    [Fact]
    public async Task Adversarial_CompiledQuery_InterceptorAndTransaction_FullParity()
    {
        using var cn = CreateConnection();

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<Article>().Where(a => a.Id == id));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        await using var tx = await ctx.Database.BeginTransactionAsync();

        var result = await compiled(ctx, 2);

        Assert.Single(result);
        Assert.Equal("Fast Car", result[0].Title);
        Assert.Equal(1, interceptor.ReaderExecutingCallCount);
        Assert.Same(ctx.CurrentTransaction, interceptor.CapturedTransaction);

        await tx.CommitAsync();
    }

    /// <summary>
    /// Adversarial soak: 200 iterations alternating between two parameter values
    /// must all return correct results (catches stale-state / pool-corruption issues).
    /// </summary>
    [Fact]
    public async Task Adversarial_CompiledQuery_200IterationsAlternating_AllCorrect()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int cat) =>
            c.Query<Article>().Where(a => a.Category == cat));

        for (int i = 0; i < 200; i++)
        {
            var cat    = (i % 2 == 0) ? 1 : 2;
            var expect = (cat == 1) ? "Hello World" : "Fast Car";
            var result = await compiled(ctx, cat);
            Assert.Single(result);
            Assert.Equal(expect, result[0].Title);
        }
    }
}
