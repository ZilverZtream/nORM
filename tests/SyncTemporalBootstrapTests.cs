using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// A1/X1 — Sync-first temporal bootstrap parity (Gate 4.2→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that temporal versioning infrastructure is bootstrapped correctly when the
/// FIRST call into the DbContext is a synchronous operation (Count, ToList, First, etc.).
///
/// A1/X1 root cause: EnsureConnection() (sync) opened the connection and initialized
/// the provider but did NOT invoke TemporalManager.InitializeAsync, so sync-first
/// workloads with temporal versioning enabled would fail with missing-object errors
/// (no history table, no tags table) while async-first workloads succeeded.
///
/// Fix: EnsureConnection() now runs the same double-checked lock pattern as
/// EnsureConnectionSlowAsync(), using _temporalInitLock.Wait() +
/// TemporalManager.InitializeAsync(...).GetAwaiter().GetResult() so that temporal
/// bootstrap runs exactly once regardless of which entry path fires first.
///
/// Tests:
///   SB-1  Sync Count() creates history and tags tables.
///   SB-2  Sync ToList() creates history and tags tables.
///   SB-3  Sync Count() idempotent — second call does not re-run bootstrap.
///   SB-4  Sync-first then async: bootstrap already complete, async no-ops cleanly.
///   SB-5  Async-first then sync: bootstrap already complete, sync no-ops cleanly.
///   SB-6  Sync Count() with temporal disabled: no bootstrap tables created.
/// </summary>
public class SyncTemporalBootstrapTests
{
    // ── Entity ────────────────────────────────────────────────────────────────

    [Table("SbEntity")]
    private class SbEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) BuildTemporal()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions
        {
            // OnModelCreating required: GetAllMappings() only returns types registered
            // through the model builder, not types accessed via GetMapping() alone.
            OnModelCreating = mb => mb.Entity<SbEntity>()
        };
        opts.EnableTemporalVersioning();
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static (SqliteConnection Cn, DbContext Ctx) BuildNoTemporal()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static bool TableExists(SqliteConnection cn, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=@n";
        cmd.Parameters.AddWithValue("@n", name);
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    // ── SB-1: Sync Count() bootstraps history and tags tables ─────────────────

    /// <summary>
    /// The very first call into a temporal-enabled DbContext is the synchronous
    /// Count() method. After it returns, the temporal infrastructure (tags table +
    /// history table for SbEntity) must exist.
    /// </summary>
    [Fact]
    public void SyncCount_TemporalEnabled_BootstrapsHistoryAndTagsTables()
    {
        var (cn, ctx) = BuildTemporal();
        using var _ = cn;
        ctx.Dispose();
        // Rebuild so nothing async has run first.
        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using var cmd = cn2.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<SbEntity>() };
        opts.EnableTemporalVersioning();
        using var ctx2 = new DbContext(cn2, new SqliteProvider(), opts);

        // Sync entry path — no prior async call.
        var count = ctx2.Query<SbEntity>().Count();

        Assert.Equal(0, count);
        Assert.True(TableExists(cn2, "__NormTemporalTags"),
            "Tags table must exist after sync bootstrap");
        Assert.True(TableExists(cn2, "SbEntity_History"),
            "History table must exist after sync bootstrap");

        cn2.Dispose();
    }

    // ── SB-2: Sync ToList() bootstraps history and tags tables ────────────────

    [Fact]
    public void SyncToList_TemporalEnabled_BootstrapsHistoryAndTagsTables()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmdSetup = cn.CreateCommand();
        cmdSetup.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmdSetup.ExecuteNonQuery();

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<SbEntity>() };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Sync enumeration — triggers EnsureConnection().
        var rows = ctx.Query<SbEntity>().ToList();

        Assert.Empty(rows);
        Assert.True(TableExists(cn, "__NormTemporalTags"),
            "Tags table must exist after sync ToList bootstrap");
        Assert.True(TableExists(cn, "SbEntity_History"),
            "History table must exist after sync ToList bootstrap");

        cn.Dispose();
    }

    // ── SB-3: Sync Count() is idempotent — bootstrap runs exactly once ─────────

    [Fact]
    public void SyncCount_CalledTwice_BootstrapRunsOnce()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmdSetup = cn.CreateCommand();
        cmdSetup.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmdSetup.ExecuteNonQuery();

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<SbEntity>() };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // First call — creates bootstrap tables.
        var c1 = ctx.Query<SbEntity>().Count();
        // Second call — must not throw (e.g., "table already exists").
        var c2 = ctx.Query<SbEntity>().Count();

        Assert.Equal(0, c1);
        Assert.Equal(0, c2);
        Assert.True(TableExists(cn, "SbEntity_History"),
            "History table must still exist after second sync call");

        cn.Dispose();
    }

    // ── SB-4: Sync-first then async — async fast-path observes bootstrap done ──

    [Fact]
    public async Task SyncFirst_ThenAsync_AsyncObservesBootstrapComplete()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmdSetup = cn.CreateCommand();
        cmdSetup.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmdSetup.ExecuteNonQuery();

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<SbEntity>() };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Sync call first.
        _ = ctx.Query<SbEntity>().Count();
        Assert.True(TableExists(cn, "SbEntity_History"),
            "History table must exist after sync bootstrap");

        // Async call second — must succeed and must not re-run bootstrap.
        var asyncCount = await ctx.Query<SbEntity>().CountAsync();

        Assert.Equal(0, asyncCount);
        // Still there (not double-created/dropped).
        Assert.True(TableExists(cn, "SbEntity_History"),
            "History table must still exist after async call post-sync bootstrap");

        cn.Dispose();
    }

    // ── SB-5: Async-first then sync — sync fast-path observes bootstrap done ───

    [Fact]
    public async Task AsyncFirst_ThenSync_SyncObservesBootstrapComplete()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmdSetup = cn.CreateCommand();
        cmdSetup.CommandText =
            "CREATE TABLE SbEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)";
        cmdSetup.ExecuteNonQuery();

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<SbEntity>() };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Async call first.
        _ = await ctx.Query<SbEntity>().CountAsync();
        Assert.True(TableExists(cn, "SbEntity_History"),
            "History table must exist after async bootstrap");

        // Sync call second — must succeed without re-running bootstrap.
        var syncCount = ctx.Query<SbEntity>().Count();

        Assert.Equal(0, syncCount);
        Assert.True(TableExists(cn, "SbEntity_History"),
            "History table must still exist after sync call post-async bootstrap");

        cn.Dispose();
    }

    // ── SB-6: Temporal disabled — no bootstrap tables created ─────────────────

    [Fact]
    public void SyncCount_TemporalDisabled_NoBootstrapTables()
    {
        var (cn, ctx) = BuildNoTemporal();
        using var _ = cn;
        using var __ = ctx;

        var count = ctx.Query<SbEntity>().Count();

        Assert.Equal(0, count);
        Assert.False(TableExists(cn, "__NormTemporalTags"),
            "Tags table must NOT exist when temporal versioning is disabled");
        Assert.False(TableExists(cn, "SbEntity_History"),
            "History table must NOT exist when temporal versioning is disabled");
    }
}
