using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CountCancellationAndConcurrencyTests
{
    [Table("CancelCountItem")]
    private sealed class CancelCountItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── A1: Cancellation token honored on sync-execution providers ────────

    /// <summary>
    /// A pre-canceled token must cause CountAsync to throw OperationCanceledException
    /// even when the connection is already open (bypassing EnsureConnectionAsync token check).
    /// Covers the sync parameterless pooled-command branch on SQLite.
    /// </summary>
    [Fact]
    public async Task CountAsync_PreCanceledToken_PooledSyncPath_ThrowsOperationCanceled()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Warm up the pool so the pooled-command branch is taken
        _ = await ctx.Query<CancelCountItem>().CountAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<CancelCountItem>().CountAsync(cts.Token));
    }

    /// <summary>
    /// A pre-canceled token must cause CountAsync to throw on the non-pooled sync path
    /// (parameterized count query, SQLite provider).
    /// </summary>
    [Fact]
    public async Task CountAsync_PreCanceledToken_NonPooledSyncPath_ThrowsOperationCanceled()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Use a predicate so this takes the non-pooled parameterized path
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<CancelCountItem>().Where(x => x.Id == 1).CountAsync(cts.Token));
    }

    /// <summary>
    /// On an async-execution provider, a command interceptor that cancels the token during
    /// ScalarExecutingAsync must cause CountAsync to throw OperationCanceledException because
    /// the subsequent ExecuteScalarAsync(ct) call sees the now-canceled token.
    /// </summary>
    [Fact]
    public async Task CountAsync_TokenCanceledDuringInterception_AsyncPath_ThrowsOperationCanceled()
    {
        using var cn = CreateConnection();
        using var cts = new CancellationTokenSource();
        var interceptor = new CancelOnScalarInterceptor(cts);
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);
        // AsyncSqliteProvider has PrefersSyncExecution=false, so interception goes through
        // ExecuteScalarWithInterceptionAsync which passes ct to ExecuteScalarAsync.
        await using var ctx = new DbContext(cn, new AsyncSqliteProvider(), options);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<CancelCountItem>().CountAsync(cts.Token));
    }

    // ── C1/X1: Pooled count command serialized per SQL key ────────────────

    /// <summary>
    /// Multiple concurrent CountAsync calls on the same DbContext instance must all return
    /// correct results without corrupting each other's transaction binding or command state.
    /// </summary>
    [Fact]
    public async Task CountAsync_ConcurrentCallsSameContext_AllReturnCorrectResult()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        var tasks = Enumerable.Range(0, 20).Select(_ =>
            ctx.Query<CancelCountItem>().CountAsync());

        var results = await Task.WhenAll(tasks);

        Assert.All(results, r => Assert.Equal(3, r));
    }

    /// <summary>
    /// Parallel CountAsync calls with a changing transaction (null transaction between calls)
    /// must not cause nondeterministic errors or wrong row counts.
    /// </summary>
    [Fact]
    public async Task CountAsync_ConcurrentCallsNoTransaction_NoDeterministicErrors()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        var tasks = Enumerable.Range(0, 10).Select(async _ =>
        {
            try
            {
                var count = await ctx.Query<CancelCountItem>().CountAsync();
                Assert.Equal(3, count);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        });

        await Task.WhenAll(tasks);
        Assert.Empty(exceptions);
    }

    // ── S1: NormalizeSql single-pass whitespace collapse ──────────────────

    /// <summary>
    /// NormalizeSql must complete in bounded time on a MaxSqlLength-sized input
    /// whose content is all spaces. The previous quadratic algorithm would take
    /// O(n²) time; the single-pass algorithm completes in O(n).
    /// </summary>
    [Fact]
    public void NormalizeSql_AdversarialAllSpaces_CompletesQuickly()
    {
        // 100k spaces — worst case for the old quadratic loop
        var adversarialInput = new string(' ', 100_000);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = NormValidator.NormalizeSql(adversarialInput);
        sw.Stop();

        // After trimming/collapsing, all-whitespace input → empty string
        Assert.Equal(string.Empty, result);
        // O(n) should finish in well under 1 second even on slow CI hardware
        Assert.True(sw.ElapsedMilliseconds < 1000,
            $"NormalizeSql took {sw.ElapsedMilliseconds}ms on 100k-space input (expected <1000ms)");
    }

    /// <summary>
    /// NormalizeSql must produce correct single-space separation for normal SQL
    /// even after the single-pass whitespace-collapse change.
    /// </summary>
    [Fact]
    public void NormalizeSql_MultipleSpacesBetweenTokens_CollapsedToOne()
    {
        var result = NormValidator.NormalizeSql("SELECT   *   FROM   users");
        Assert.Equal("select * from users", result);
    }

    /// <summary>
    /// NormalizeSql must collapse mixed whitespace characters (tabs, newlines, Unicode spaces)
    /// into single spaces between tokens.
    /// </summary>
    [Fact]
    public void NormalizeSql_MixedWhitespaceVariants_CollapsedToSingleSpaces()
    {
        var result = NormValidator.NormalizeSql("SELECT\t\r\n*\u00A0FROM\u2003users");
        Assert.Equal("select * from users", result);
    }

    /// <summary>
    /// NormalizeSql must trim leading and trailing whitespace.
    /// </summary>
    [Fact]
    public void NormalizeSql_LeadingAndTrailingWhitespace_Trimmed()
    {
        var result = NormValidator.NormalizeSql("   SELECT 1   ");
        Assert.Equal("select 1", result);
    }

    // ── 4.0→4.5: Cancellation across providers (combinatorial) ───────────

    /// <summary>
    /// CountAsync with a pre-canceled token must throw on the async path (non-sync provider).
    /// </summary>
    [Fact]
    public async Task CountAsync_AsyncProvider_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Use a CustomSqliteProvider that does NOT set PrefersSyncExecution=true
        // to exercise the async code path through ExecuteScalarWithInterceptionAsync.
        using var cn = CreateConnection();
        using var cts = new CancellationTokenSource();

        // Cancel before any execution
        cts.Cancel();

        await using var ctx = new DbContext(cn, new AsyncSqliteProvider());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<CancelCountItem>().CountAsync(cts.Token));
    }

    // ── 4.5→5.0: Adversarial NormalizeSql coverage ───────────────────────

    /// <summary>
    /// NormalizeSql must handle 100k-character SQL with alternating word/space tokens
    /// without quadratic blowup.
    /// </summary>
    [Fact]
    public void NormalizeSql_AdversarialAlternatingTokens_CompletesQuickly()
    {
        var sb = new System.Text.StringBuilder(100_000);
        for (int i = 0; i < 10_000; i++)
            sb.Append("a    ");
        var input = sb.ToString();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = NormValidator.NormalizeSql(input);
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds < 1000,
            $"NormalizeSql took {sw.ElapsedMilliseconds}ms on adversarial alternating input (expected <1000ms)");
        // Each "a    " should collapse to "a " → "a a a..."
        Assert.DoesNotContain("  ", result);
    }

    /// <summary>
    /// NormalizeSql with deeply nested block comments does not introduce extra spaces.
    /// </summary>
    [Fact]
    public void NormalizeSql_DeepNestedBlockComment_NoExtraSpaces()
    {
        var result = NormValidator.NormalizeSql("SELECT /* /* nested */ */ 1");
        Assert.Equal("select 1", result);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CancelCountItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "INSERT INTO CancelCountItem VALUES (1, 'Alice');" +
            "INSERT INTO CancelCountItem VALUES (2, 'Bob');" +
            "INSERT INTO CancelCountItem VALUES (3, 'Carol');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    /// <summary>Cancels the provided CancellationTokenSource when a scalar command is about to execute.</summary>
    private sealed class CancelOnScalarInterceptor : nORM.Enterprise.IDbCommandInterceptor
    {
        private readonly CancellationTokenSource _cts;
        public CancelOnScalarInterceptor(CancellationTokenSource cts) => _cts = cts;

        public Task<nORM.Enterprise.InterceptionResult<int>> NonQueryExecutingAsync(
            System.Data.Common.DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(nORM.Enterprise.InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(System.Data.Common.DbCommand command, DbContext context,
            int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<nORM.Enterprise.InterceptionResult<object?>> ScalarExecutingAsync(
            System.Data.Common.DbCommand command, DbContext context, CancellationToken ct)
        {
            _cts.Cancel();
            return Task.FromResult(nORM.Enterprise.InterceptionResult<object?>.Continue());
        }

        public Task ScalarExecutedAsync(System.Data.Common.DbCommand command, DbContext context,
            object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<nORM.Enterprise.InterceptionResult<System.Data.Common.DbDataReader>> ReaderExecutingAsync(
            System.Data.Common.DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(nORM.Enterprise.InterceptionResult<System.Data.Common.DbDataReader>.Continue());

        public Task ReaderExecutedAsync(System.Data.Common.DbCommand command, DbContext context,
            System.Data.Common.DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(System.Data.Common.DbCommand command, DbContext context,
            Exception exception, CancellationToken ct)
            => Task.CompletedTask;
    }

    /// <summary>SQLite provider variant that uses async execution paths.</summary>
    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }
}
