using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

// X1 regression: [CompileTimeQuery]-generated methods must:
//  1. Use CreateCompiledQueryCommandAsync — opens connection, binds active transaction.
//  2. Use ExecuteCompiledQueryListAsync  — routes through command interceptors.
// These tests validate the two new DbContext public APIs directly.

#nullable enable

namespace nORM.Tests;

public class CompileTimeQueryLifecycleTests
{
    // ── Domain model ──────────────────────────────────────────────────────────

    [Table("ctq_items")]
    private class CtqItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateCtx(
        IDbCommandInterceptor? interceptor = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var opts = new DbContextOptions();
        if (interceptor != null)
            opts.CommandInterceptors.Add(interceptor);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx);
    }

    private static void CreateTable(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS ctq_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL DEFAULT '')";
        cmd.ExecuteNonQuery();
    }

    private static Func<DbDataReader, CancellationToken, Task<CtqItem>> RowMat =>
        (reader, ct) => Task.FromResult(new CtqItem
        {
            Id = reader.GetInt32(reader.GetOrdinal("Id")),
            Label = reader.GetString(reader.GetOrdinal("Label"))
        });

    // ── X1-1: CreateCompiledQueryCommandAsync opens a closed connection ───────

    [Fact]
    public async Task CreateCompiledQueryCommandAsync_opens_closed_connection()
    {
        // Provide an explicitly-closed connection.
        var cn = new SqliteConnection("Data Source=:memory:");
        Assert.Equal(ConnectionState.Closed, cn.State);
        var ctx = new DbContext(cn, new SqliteProvider());

        var cmd = await ctx.CreateCompiledQueryCommandAsync();

        // Connection must have been opened.
        Assert.Equal(ConnectionState.Open, cn.State);
        Assert.NotNull(cmd);

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    [Fact]
    public async Task CreateCompiledQueryCommandAsync_with_already_open_connection()
    {
        var (cn, ctx) = CreateCtx();

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        Assert.Equal(ConnectionState.Open, cn.State);
        Assert.NotNull(cmd);

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-2: CreateCompiledQueryCommandAsync binds active transaction ────────

    [Fact]
    public async Task CreateCompiledQueryCommandAsync_binds_active_transaction()
    {
        var (cn, ctx) = CreateCtx();
        CreateTable(cn);

        // Begin an explicit transaction via the DatabaseFacade.
        var tx = await ctx.Database.BeginTransactionAsync();

        var cmd = await ctx.CreateCompiledQueryCommandAsync();

        // Command must carry the transaction.
        Assert.NotNull(cmd.Transaction);
        Assert.Same(tx.Transaction, cmd.Transaction);

        await tx.RollbackAsync();
        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    [Fact]
    public async Task CreateCompiledQueryCommandAsync_no_transaction_gives_null_transaction()
    {
        var (cn, ctx) = CreateCtx();

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        // No active transaction — command.Transaction should be null.
        Assert.Null(cmd.Transaction);

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-3: ExecuteCompiledQueryListAsync invokes reader interceptors ───────

    private sealed class ReaderInterceptor : IDbCommandInterceptor
    {
        public int ExecutingCount { get; private set; }
        public int ExecutedCount { get; private set; }
        public string? LastSql { get; private set; }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct)
            => Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand c, DbContext ctx, int r, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct)
            => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand c, DbContext ctx, object? r, TimeSpan d, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct)
        {
            ExecutingCount++;
            LastSql = c.CommandText;
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }
        public Task ReaderExecutedAsync(DbCommand c, DbContext ctx, DbDataReader r, TimeSpan d, CancellationToken ct)
        {
            ExecutedCount++;
            return Task.CompletedTask;
        }
        public Task CommandFailedAsync(DbCommand c, DbContext ctx, Exception e, CancellationToken ct)
            => Task.CompletedTask;
    }

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_fires_reader_interceptor()
    {
        var interceptor = new ReaderInterceptor();
        var (cn, ctx) = CreateCtx(interceptor);
        CreateTable(cn);

        // Insert a row via direct SQL.
        using (var ins = cn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO ctq_items (Label) VALUES ('hello')";
            ins.ExecuteNonQuery();
        }

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items";

        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, RowMat, CancellationToken.None);

        Assert.Single(results);
        Assert.Equal("hello", results[0].Label);
        Assert.Equal(1, interceptor.ExecutingCount);
        Assert.Equal(1, interceptor.ExecutedCount);
        Assert.Contains("ctq_items", interceptor.LastSql);

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_returns_empty_list_for_no_rows()
    {
        var (cn, ctx) = CreateCtx();
        CreateTable(cn);

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items";

        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, RowMat, CancellationToken.None);

        Assert.Empty(results);

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-4: ExecuteCompiledQueryListAsync materializes multiple rows ─────────

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_materializes_all_rows()
    {
        var (cn, ctx) = CreateCtx();
        CreateTable(cn);

        using (var ins = cn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO ctq_items (Label) VALUES ('a'), ('b'), ('c')";
            ins.ExecuteNonQuery();
        }

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items ORDER BY Id";

        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, RowMat, CancellationToken.None);

        Assert.Equal(3, results.Count);
        Assert.Equal(new[] { "a", "b", "c" }, new[] { results[0].Label, results[1].Label, results[2].Label });

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-5: CancellationToken propagates correctly ───────────────────────────

    [Fact]
    public async Task CreateCompiledQueryCommandAsync_respects_cancelled_token()
    {
        // Provide closed connection so EnsureConnectionAsync is required.
        var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.CreateCompiledQueryCommandAsync(cts.Token));

        ctx.Dispose();
        cn.Dispose();
    }

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_respects_cancelled_token_in_materializer()
    {
        var (cn, ctx) = CreateCtx();
        CreateTable(cn);

        using (var ins = cn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO ctq_items (Label) VALUES ('x')";
            ins.ExecuteNonQuery();
        }

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items";

        Func<DbDataReader, CancellationToken, Task<CtqItem>> cancelMat = (reader, ct) =>
        {
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(new CtqItem { Id = reader.GetInt32(0), Label = reader.GetString(1) });
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.ExecuteCompiledQueryListAsync(cmd, cancelMat, cts.Token));

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-6: reads inside transaction see uncommitted writes ─────────────────

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_sees_uncommitted_writes_in_same_transaction()
    {
        var (cn, ctx) = CreateCtx();
        CreateTable(cn);

        var tx = await ctx.Database.BeginTransactionAsync();

        // Write inside transaction without committing.
        using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction?)tx.Transaction;
            ins.CommandText = "INSERT INTO ctq_items (Label) VALUES ('uncommitted')";
            ins.ExecuteNonQuery();
        }

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items";

        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, RowMat, CancellationToken.None);

        // Same transaction — must see the uncommitted row.
        Assert.Single(results);
        Assert.Equal("uncommitted", results[0].Label);

        await tx.RollbackAsync();
        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-7: multiple interceptors all fire ──────────────────────────────────

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_fires_all_interceptors()
    {
        var i1 = new ReaderInterceptor();
        var i2 = new ReaderInterceptor();

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(i1);
        opts.CommandInterceptors.Add(i2);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        CreateTable(cn);

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items";

        await ctx.ExecuteCompiledQueryListAsync(cmd, RowMat, CancellationToken.None);

        Assert.Equal(1, i1.ExecutingCount);
        Assert.Equal(1, i2.ExecutingCount);

        cmd.Dispose();
        ctx.Dispose();
        cn.Dispose();
    }

    // ── X1-8: disposed context throws ─────────────────────────────────────────

    [Fact]
    public async Task CreateCompiledQueryCommandAsync_throws_when_disposed()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Dispose();

        await Assert.ThrowsAnyAsync<Exception>(
            () => ctx.CreateCompiledQueryCommandAsync());

        cn.Dispose();
    }

    [Fact]
    public async Task ExecuteCompiledQueryListAsync_throws_when_disposed()
    {
        var (cn, ctx) = CreateCtx();
        CreateTable(cn);

        var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Label FROM ctq_items";

        ctx.Dispose();

        await Assert.ThrowsAnyAsync<Exception>(
            () => ctx.ExecuteCompiledQueryListAsync(cmd, RowMat, CancellationToken.None));

        cmd.Dispose();
        cn.Dispose();
    }
}
