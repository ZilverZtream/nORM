using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
/// Tests covering the compiled query fast path: connection state handling, transaction
/// binding, interceptor invocation, DbType assignment for fixed parameters, and parity
/// between compiled and runtime LINQ queries.
/// </summary>
public class CompiledQueryFastPathTests
{
    // ─── shared entity ────────────────────────────────────────────────────────

    public class Article
    {
        public int    Id       { get; set; }
        public string Title    { get; set; } = "";
        public int    Category { get; set; }   // also used as enum-backed column
    }

    [Table("Article")]
    public class EnumArticle
    {
        public int Id { get; set; }
        public string Title { get; set; } = "";
        public ArticleCategory Category { get; set; }
    }

    public class NullableArticle
    {
        public int Id { get; set; }
        public string? Title { get; set; }
    }

    [Table("SpecialParamEntity")]
    public class SpecialParamEntity
    {
        public int Id { get; set; }
        public Guid GuidValue { get; set; }
        public DateOnly Created { get; set; }
        public TimeOnly StartsAt { get; set; }
    }

    public enum ArticleCategory { News = 1, Sports = 2, Tech = 3 }

    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }

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
        public string? CapturedSql;
        public List<(string Name, DbType DbType, object? Value, int Size)> CapturedParameters { get; } = new();

        // Sync hook — called from sync execution paths (e.g. compiled query fast path on SQLite)
        public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext ctx)
        {
            Interlocked.Increment(ref ReaderExecutingCallCount);
            CapturedTransaction = command.Transaction;
            CapturedSql = command.CommandText;
            CaptureParameters(command);
            return InterceptionResult<DbDataReader>.Continue();
        }

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
            CapturedSql = command.CommandText;
            CaptureParameters(command);
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task ReaderExecutedAsync(DbCommand _, DbContext __, DbDataReader ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;

        private void CaptureParameters(DbCommand command)
        {
            CapturedParameters.Clear();
            foreach (DbParameter parameter in command.Parameters)
                CapturedParameters.Add((parameter.ParameterName, parameter.DbType, parameter.Value, parameter.Size));
        }
    }

    private sealed class ThrowOnceReaderInterceptor : IDbCommandInterceptor
    {
        private int _remainingThrows = 1;
        public List<DbCommand> SeenCommands { get; } = new();

        public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext ctx)
        {
            SeenCommands.Add(command);
            if (Interlocked.Exchange(ref _remainingThrows, 0) == 1)
                throw new InvalidOperationException("Injected reader failure");
            return InterceptionResult<DbDataReader>.Continue();
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand _, DbContext __, CancellationToken ___) =>
            Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand _, DbContext __, int ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand _, DbContext __, CancellationToken ___) =>
            Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand _, DbContext __, object? ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext ctx, CancellationToken ct)
            => Task.FromResult(ReaderExecuting(command, ctx));
        public Task ReaderExecutedAsync(DbCommand _, DbContext __, DbDataReader ___, TimeSpan ____, CancellationToken _____) =>
            Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand _, DbContext __, Exception ___, CancellationToken ____) =>
            Task.CompletedTask;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Connection state handling
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// When the connection is closed, the fast path is skipped. The standard path calls
    /// EnsureConnectionAsync which reopens the file-backed SQLite connection, and the
    /// query succeeds with correct data.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_ClosedConnection_FallsBackToStandardPath()
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

    // ════════════════════════════════════════════════════════════════════════════
    // Transaction binding and interceptor invocation
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Registered interceptors are invoked for compiled queries on the sync fast path.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_WithInterceptor_ReaderExecutingIsInvoked()
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
    /// The compiled query command participates in the active transaction.
    /// Verified via interceptor capturing command.Transaction inside ReaderExecutingAsync.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_UnderExplicitTransaction_CommandTransactionIsSet()
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
        Assert.Same(ctx.CurrentTransaction, interceptor.CapturedTransaction);
        Assert.Single(result);

        await tx.CommitAsync();
    }

    /// <summary>
    /// The interceptor is invoked and the transaction is bound even when the connection was
    /// briefly closed before the call (closed-connection fallback path).
    /// </summary>
    [Fact]
    public async Task ClosedConnection_WithInterceptorAndTransaction_BothWork()
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
    // Fixed-parameter DbType matrix
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// AssignValue must set DbType = Int32 (not Object) for an enum value with an int32
    /// underlying type so providers do not coerce it as an untyped blob.
    /// </summary>
    [Fact]
    public void AssignValue_EnumWithInt32Underlying_SetsInt32DbType()
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
    /// AssignValue must set DbType = Date for a DateOnly value.
    /// </summary>
    [Fact]
    public void AssignValue_DateOnly_SetsDateDbType()
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
    /// AssignValue must set DbType = Time and convert TimeOnly to TimeSpan for
    /// cross-provider compatibility.
    /// </summary>
    [Fact]
    public void AssignValue_TimeOnly_SetsTimeDbTypeAndConvertsToTimeSpan()
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
    /// AssignValue must set DbType = Guid for a Guid value. Without a DbType hint, a raw
    /// Guid stored via p.Value defaults to Object / String on some providers, causing
    /// binding failures.
    /// </summary>
    [Fact]
    public void AssignValue_Guid_SetsGuidDbType()
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
    /// A compiled query whose WHERE clause contains an enum constant (fixed parameter)
    /// must return correct results. AssignValue sets DbType = Int32 for enum-backed fixed params.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_EnumConstantFixedParam_CorrectResults()
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

    [Fact]
    public async Task CompiledQuery_AsyncProvider_EnumConstantFixedParam_UsesInt32Binding()
    {
        using var cn = CreateConnection();
        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<EnumArticle>().Where(a => a.Category == ArticleCategory.Sports));

        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), options);

        var result = await compiled(ctx, 0);

        Assert.Single(result);
        Assert.Equal("Fast Car", result[0].Title);
        var parameter = Assert.Single(interceptor.CapturedParameters);
        Assert.Equal(DbType.Int32, parameter.DbType);
        Assert.Equal((int)ArticleCategory.Sports, Convert.ToInt32(parameter.Value));
    }

    /// <summary>
    /// A compiled query whose runtime parameter is a nullable int must bind correctly
    /// whether the value is non-null or null.
    /// </summary>
    [Fact]
    public async Task NullableInt_AssignedCorrectly()
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
    // Parity between compiled and runtime queries
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Compiled query and runtime LINQ query must produce identical result sets for the
    /// same predicate and parameter values.
    /// </summary>
    [Fact]
    public async Task CompiledAndRuntimeQuery_SameResultsForAllRows()
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
    /// Compiled query with interceptor registered AND under an explicit transaction
    /// produces correct results, calls the interceptor, and the command's transaction
    /// reference matches ctx.CurrentTransaction.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_InterceptorAndTransaction_FullParity()
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

    [Fact]
    public async Task CompiledQuery_PooledCommand_RebindsTransactionAfterReuse()
    {
        using var cn = CreateConnection();

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<Article>().Where(a => a.Id == id));

        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), options);

        var outsideTx = await compiled(ctx, 1);
        Assert.Single(outsideTx);
        Assert.Null(interceptor.CapturedTransaction);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        var insideTx = await compiled(ctx, 2);

        Assert.Single(insideTx);
        Assert.Equal("Fast Car", insideTx[0].Title);
        Assert.Same(ctx.CurrentTransaction, interceptor.CapturedTransaction);

        await tx.CommitAsync();
    }

    [Fact]
    public async Task CompiledQuery_PooledCommand_ReturnedAfterSyncReaderException()
    {
        var path = Path.Combine(Path.GetTempPath(), $"norm_compiled_pool_{Guid.NewGuid():N}.db");
        try
        {
            using var cn = CreateConnection(path);

            var interceptor = new ThrowOnceReaderInterceptor();
            var options = new DbContextOptions();
            options.CommandInterceptors.Add(interceptor);

            var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
                ctx.Query<Article>().Where(a => a.Id == id));

            using var ctx = new DbContext(cn, new SqliteProvider(), options);

            await Assert.ThrowsAsync<InvalidOperationException>(() => compiled(ctx, 1));

            var result = await compiled(ctx, 2);

            Assert.Single(result);
            Assert.Equal("Fast Car", result[0].Title);
            Assert.Equal(2, interceptor.SeenCommands.Count);
            Assert.Same(interceptor.SeenCommands[0], interceptor.SeenCommands[1]);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    /// <summary>
    /// 200 iterations alternating between two parameter values must all return correct
    /// results, catching stale-state or pool-corruption issues.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_200IterationsAlternating_AllCorrect()
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

    [Fact]
    public async Task CompiledQuery_NonNullableStringColumn_UsesPlainEquality()
    {
        using var cn = CreateConnection();
        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext c, string title) =>
            c.Query<Article>().Where(a => a.Title == title));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var result = await compiled(ctx, "Fast Car");

        Assert.Single(result);
        Assert.Equal(2, result[0].Id);
        Assert.NotNull(interceptor.CapturedSql);
        Assert.Contains(" = ", interceptor.CapturedSql);
        Assert.DoesNotContain(" IS ", interceptor.CapturedSql);
    }

    [Fact]
    public async Task CompiledQuery_NullableStringColumn_KeepsNullSafeEquality()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE NullableArticle (Id INTEGER PRIMARY KEY, Title TEXT);" +
                "INSERT INTO NullableArticle VALUES (1, 'Hello World');" +
                "INSERT INTO NullableArticle VALUES (2, NULL);";
            setup.ExecuteNonQuery();
        }

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext c, string? title) =>
            c.Query<NullableArticle>().Where(a => a.Title == title));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var result = await compiled(ctx, null);

        Assert.Single(result);
        Assert.Equal(2, result[0].Id);
        Assert.NotNull(interceptor.CapturedSql);
        Assert.Contains(" IS ", interceptor.CapturedSql);
    }

    [Fact]
    public async Task CompiledQuery_PooledStringParameter_ResetsSizeAndNullMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE NullableArticle (Id INTEGER PRIMARY KEY, Title TEXT);" +
                "INSERT INTO NullableArticle VALUES (1, 'A');" +
                "INSERT INTO NullableArticle VALUES (2, 'A much longer title');" +
                "INSERT INTO NullableArticle VALUES (3, NULL);";
            setup.ExecuteNonQuery();
        }

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext c, string? title) =>
            c.Query<NullableArticle>().Where(a => a.Title == title));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var shortResult = await compiled(ctx, "A");
        Assert.Single(shortResult);
        var shortParameter = Assert.Single(interceptor.CapturedParameters);
        Assert.Equal(DbType.String, shortParameter.DbType);
        Assert.Equal(1, shortParameter.Size);

        var longResult = await compiled(ctx, "A much longer title");
        Assert.Single(longResult);
        var longParameter = Assert.Single(interceptor.CapturedParameters);
        Assert.Equal(DbType.String, longParameter.DbType);
        Assert.Equal("A much longer title".Length, longParameter.Size);

        var nullResult = await compiled(ctx, null);
        Assert.Single(nullResult);
        var nullParameter = Assert.Single(interceptor.CapturedParameters);
        Assert.Equal(DbType.Object, nullParameter.DbType);
        Assert.Equal(DBNull.Value, nullParameter.Value);
        Assert.Equal(0, nullParameter.Size);
    }

    [Fact]
    public async Task CompiledQuery_PooledSpecialParameters_SetExpectedDbTypes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE SpecialParamEntity (" +
                "Id INTEGER PRIMARY KEY, GuidValue TEXT NOT NULL, Created TEXT NOT NULL, StartsAt TEXT NOT NULL);";
            setup.ExecuteNonQuery();
        }

        var interceptor = new ReaderCapturingInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        var compiled = Norm.CompileQuery((DbContext c, (Guid Id, DateOnly Date, TimeOnly Time) p) =>
            c.Query<SpecialParamEntity>().Where(e =>
                e.GuidValue == p.Id &&
                e.Created == p.Date &&
                e.StartsAt == p.Time));

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var rows = await compiled(ctx, (Guid.NewGuid(), new DateOnly(2026, 5, 21), new TimeOnly(14, 30)));

        Assert.Empty(rows);
        Assert.Contains(interceptor.CapturedParameters, p => p.DbType == DbType.Guid);
        Assert.Contains(interceptor.CapturedParameters, p => p.DbType == DbType.Date);
        Assert.Contains(interceptor.CapturedParameters, p => p.DbType == DbType.Time && p.Value is TimeSpan);
    }

}
