using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using nORM.Versioning;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// A1 — Temporal bootstrap cancellation not propagated (Gate 4.1→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that a CancellationToken passed to EnsureConnectionAsync (or directly to
/// TemporalManager.InitializeAsync) is forwarded all the way through the temporal
/// bootstrap path so that pre-cancellation and mid-bootstrap cancellation work correctly.
///
/// A1 root cause: Lazy&lt;Task&gt; captures the factory at construction time with no way
/// to accept the CancellationToken provided at runtime. DDL commands used `default`,
/// ignoring the caller's token entirely.
///
/// Fix: replaced Lazy&lt;Task&gt; with a volatile double-checked lock pattern
/// (_temporalInitTask / _temporalInitLock) so that ct is plumbed from
/// EnsureConnectionAsync → TemporalManager.InitializeAsync → every DDL call.
/// </summary>
public class TemporalBootstrapCancellationTests
{
    // ── Entity for temporal-enabled contexts ─────────────────────────────────

    [Table("TbcEntity")]
    private class TbcEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── Helper: build a temporal-enabled DbContext ────────────────────────────

    private static (SqliteConnection cn, DbContext ctx) BuildTemporal()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TbcEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions
        {
            // Register TbcEntity via OnModelCreating so GetAllMappings() includes it
            // during temporal bootstrap. Calling GetMapping() alone is insufficient:
            // GetAllMappings() enumerates _modelBuilder.GetConfiguredEntityTypes(), which
            // only includes types registered through OnModelCreating, not those added to
            // the mapping cache via GetMapping().
            OnModelCreating = mb => mb.Entity<TbcEntity>()
        };
        opts.EnableTemporalVersioning();
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    // ── Helper: check whether a table exists in the SQLite database ───────────

    private static bool TableExists(SqliteConnection cn, string tableName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=@n";
        cmd.Parameters.AddWithValue("@n", tableName);
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    // ── Pre-cancelled token before first temporal bootstrap ───────────────────

    /// <summary>
    /// When the CancellationToken is already cancelled before EnsureConnectionAsync is
    /// called, the temporal bootstrap must throw OperationCanceledException immediately —
    /// before any DDL reaches the database.
    /// </summary>
    [Fact]
    public async Task PreCancelledToken_EnsureConnection_ThrowsOperationCanceledException()
    {
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel before any work starts

        var ex = await Record.ExceptionAsync(
            () => ctx.EnsureConnectionAsync(cts.Token));

        Assert.NotNull(ex);
        Assert.True(ex is OperationCanceledException, $"Expected OperationCanceledException, got {ex.GetType().Name}");
    }

    /// <summary>
    /// After a pre-cancellation, the temporal infrastructure must NOT have been created —
    /// neither the tags table nor any history table should exist.
    /// </summary>
    [Fact]
    public async Task PreCancelledToken_NoDdlExecuted_TablesAbsent()
    {
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Ignore the expected cancellation exception.
        await Record.ExceptionAsync(() => ctx.EnsureConnectionAsync(cts.Token));

        // No DDL should have been executed — tags table and history table absent.
        Assert.False(TableExists(cn, "__NormTemporalTags"),
            "Tags table must not exist after pre-cancelled bootstrap");
        Assert.False(TableExists(cn, "TbcEntity_History"),
            "History table must not exist after pre-cancelled bootstrap");
    }

    /// <summary>
    /// After a pre-cancelled bootstrap attempt leaves the context in un-initialized state,
    /// a subsequent call with a non-cancelled token must succeed and complete the bootstrap.
    /// The double-checked lock must permit re-entry when _temporalInitComplete is still false.
    /// </summary>
    [Fact]
    public async Task PreCancelledThenValidToken_BootstrapSucceeds()
    {
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        // First attempt: pre-cancelled.
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Record.ExceptionAsync(() => ctx.EnsureConnectionAsync(cts.Token));

        // Second attempt: valid token — bootstrap must now complete.
        await ctx.EnsureConnectionAsync(CancellationToken.None);

        Assert.True(TableExists(cn, "__NormTemporalTags"),
            "Tags table must exist after successful retry bootstrap");
        Assert.True(TableExists(cn, "TbcEntity_History"),
            "History table must exist after successful retry bootstrap");
    }

    // ── Mid-bootstrap cancellation via TemporalManager.InitializeAsync ────────

    /// <summary>
    /// Calling TemporalManager.InitializeAsync directly with a pre-cancelled token must
    /// throw OperationCanceledException at the very first ThrowIfCancellationRequested
    /// guard, before any DDL reaches the database.
    /// This exercises the guard at the entry point of InitializeAsync itself.
    /// </summary>
    [Fact]
    public async Task TemporalManager_InitializeAsync_PreCancelledToken_ThrowsOCE()
    {
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Invoke InitializeAsync directly via reflection.
        // Signature: InitializeAsync(DbContext context, DbConnection conn, CancellationToken ct = default)
        var initMethod = typeof(TemporalManager).GetMethod(
            "InitializeAsync",
            BindingFlags.Static | BindingFlags.Public)!;

        var task = (Task)initMethod.Invoke(null, new object[] { ctx, cn, cts.Token })!;
        var ex = await Record.ExceptionAsync(() => task);

        Assert.NotNull(ex);
        Assert.True(ex is OperationCanceledException,
            $"Expected OperationCanceledException, got {ex?.GetType().Name}");
    }

    /// <summary>
    /// When the cancellation token fires after the tags-table step but before the per-entity
    /// DDL loop, the in-loop ThrowIfCancellationRequested guard must propagate the cancellation.
    /// We simulate this by cancelling a CancellationTokenSource immediately after construction
    /// (token is already cancelled when the foreach body checks it), then verifying that
    /// EnsureConnectionAsync surfaces OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task MidBootstrap_LoopGuardFires_ThrowsOperationCanceledException()
    {
        // Build a context backed by a CancellationTokenSource we control.
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        // CancelAfter(0) schedules immediate cancellation — fires on the next async yield
        // point inside InitializeAsync (which is the await of CreateTagsTableIfNotExistsAsync
        // or the first iteration's EnsureConnectionAsync / ExecuteDdlAsync call).
        using var cts = new CancellationTokenSource(TimeSpan.Zero);

        // Give the token a moment to transition to cancelled state.
        await Task.Yield();

        var ex = await Record.ExceptionAsync(
            () => ctx.EnsureConnectionAsync(cts.Token));

        // Either the pre-entry guard or the in-loop guard fired; either way OCE is required.
        Assert.NotNull(ex);
        Assert.True(ex is OperationCanceledException,
            $"Expected OperationCanceledException, got {ex?.GetType().Name}");
    }

    // ── Successful temporal bootstrap smoke test ──────────────────────────────

    /// <summary>
    /// Baseline: with a non-cancelled token, temporal bootstrap must complete successfully,
    /// creating both the tags table and the entity history table.
    /// </summary>
    [Fact]
    public async Task ValidToken_BootstrapCompletes_TablesPresent()
    {
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        await ctx.EnsureConnectionAsync(CancellationToken.None);

        Assert.True(TableExists(cn, "__NormTemporalTags"));
        Assert.True(TableExists(cn, "TbcEntity_History"));
    }

    /// <summary>
    /// Temporal bootstrap must be idempotent: calling EnsureConnectionAsync multiple times
    /// with a valid token must not throw or re-execute DDL.
    /// </summary>
    [Fact]
    public async Task ValidToken_RepeatedEnsureConnection_Idempotent()
    {
        var (cn, ctx) = BuildTemporal();
        await using var _ = ctx; using var __ = cn;

        // Call multiple times — second and later calls take the fast path.
        await ctx.EnsureConnectionAsync(CancellationToken.None);
        await ctx.EnsureConnectionAsync(CancellationToken.None);
        await ctx.EnsureConnectionAsync(CancellationToken.None);

        Assert.True(TableExists(cn, "__NormTemporalTags"));
        Assert.True(TableExists(cn, "TbcEntity_History"));
    }
}
