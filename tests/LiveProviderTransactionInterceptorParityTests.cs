using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for explicit transaction semantics and command interceptor
/// behavior within active transactions.
///
/// Evidence gap closed: ExplicitTransactionTests verifies these contracts only on
/// SQLite in-memory. This class re-runs the same contracts against SQL Server, MySQL,
/// and PostgreSQL live connections, plus two interceptor contracts that require a
/// real execution path (interceptor fires with non-null Transaction on the command;
/// suppressed command inside a transaction does not abort the transaction).
///
/// Schema: TxpRow (Id INT PK, Label VARCHAR — no identity column so all providers
/// accept explicit key values without special configuration).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderTransactionInterceptorParityTests
{
    private const string Table = "TxpRow";

    [Table(Table)]
    private sealed class TxpRow
    {
        [Key]
        public int    Id    { get; set; }
        public string Label { get; set; } = "";
    }

    // ── DDL helpers ───────────────────────────────────────────────────────────

    private static string IntCol(ProviderKind kind) =>
        kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropDdl(ProviderKind kind, string esc) =>
        kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
            : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc  = ctx.Provider.Escape(Table);
        var eId  = ctx.Provider.Escape("Id");
        var eLbl = ctx.Provider.Escape("Label");
        await ExecAsync(ctx, DropDdl(kind, esc));
        await ExecAsync(ctx,
            $"CREATE TABLE {esc} ({eId} {IntCol(kind)} PRIMARY KEY, {eLbl} {VarCol(kind, 100)} NOT NULL)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    private static async Task ExecAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountRowsAsync(DbContext ctx)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {ctx.Provider.Escape(Table)}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // ── 1: Commit makes rows visible ──────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExplicitTransaction_Commit_RowsAreVisible(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var ctx = new DbContext(cn, prov))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                await ctx.InsertAsync(new TxpRow { Id = 1, Label = "committed" });
                await tx.CommitAsync();

                Assert.Equal(1L, await CountRowsAsync(ctx));
                var rows = ctx.Query<TxpRow>().ToList();
                Assert.Single(rows);
                Assert.Equal("committed", rows[0].Label);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: Rollback hides inserted rows ──────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExplicitTransaction_Rollback_RowsAbsent(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var ctx = new DbContext(cn, prov))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                await ctx.InsertAsync(new TxpRow { Id = 1, Label = "should-vanish" });
                await tx.RollbackAsync();

                Assert.Equal(0L, await CountRowsAsync(ctx));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: Opening a second transaction while one is active throws ────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task BeginTransaction_WhileActiveTransaction_ThrowsNormUsageException(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var ctx = new DbContext(cn, prov))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                await Assert.ThrowsAsync<NormUsageException>(() =>
                    ctx.Database.BeginTransactionAsync());
                await tx.RollbackAsync();
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: SaveChanges participates in the enclosing explicit transaction ──────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task SaveChanges_InsideExplicitTransaction_RollsBackWithTransaction(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var ctx = new DbContext(cn, prov))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                ctx.Add(new TxpRow { Id = 1, Label = "tracked-1" });
                ctx.Add(new TxpRow { Id = 2, Label = "tracked-2" });
                await ctx.SaveChangesAsync();
                await tx.RollbackAsync();

                Assert.Equal(0L, await CountRowsAsync(ctx));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: Command interceptor fires with non-null Transaction on the command ──

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Interceptor_NonQueryExecutingAsync_SeesTransactionOnCommand(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;

        var capturedTransactions = new List<DbTransaction?>();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(new TransactionCapturingInterceptor(capturedTransactions));

        var (cn, prov) = live!.Value;
        await using (cn)
        using (var ctx = new DbContext(cn, prov, opts))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                await ctx.InsertAsync(new TxpRow { Id = 1, Label = "via-interceptor" });
                await tx.CommitAsync();

                Assert.Contains(capturedTransactions, t => t != null);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 6: Suppressed command inside transaction leaves transaction usable ─────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Interceptor_SuppressedInsert_TransactionStillCommitsSubsequentInserts(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;

        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(new SuppressFirstNonQueryInterceptor());

        var (cn, prov) = live!.Value;
        await using (cn)
        using (var ctx = new DbContext(cn, prov, opts))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                // First insert: suppressed — Id=1 never reaches the database.
                await ctx.InsertAsync(new TxpRow { Id = 1, Label = "suppressed" });
                // Second insert: allowed — Id=2 reaches the database.
                await ctx.InsertAsync(new TxpRow { Id = 2, Label = "real" });
                await tx.CommitAsync();

                Assert.Equal(1L, await CountRowsAsync(ctx));
                var rows = ctx.Query<TxpRow>().ToList();
                Assert.Single(rows);
                Assert.Equal("real", rows[0].Label);
                Assert.Equal(2, rows[0].Id);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── Interceptor helpers ───────────────────────────────────────────────────

    private sealed class TransactionCapturingInterceptor : IDbCommandInterceptor
    {
        private readonly List<DbTransaction?> _captured;
        public TransactionCapturingInterceptor(List<DbTransaction?> captured) => _captured = captured;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            _captured.Add(command.Transaction);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct)
            => Task.CompletedTask;
    }

    private sealed class SuppressFirstNonQueryInterceptor : IDbCommandInterceptor
    {
        private int _callCount;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            if (Interlocked.Increment(ref _callCount) == 1)
                return Task.FromResult(InterceptionResult<int>.SuppressWithResult(0));
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct)
            => Task.CompletedTask;
    }
}
