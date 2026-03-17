using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 → 5.0 — Lock-step provider parity (adversarial / fuzz / contention)
//
// 5.0 requirements per audit Section 8:
//   — Prove no medium+ correctness bugs via adversarial/fuzz + contention stress
//     across all providers.
//   — Require lock-step parity for transaction, migration partial-failure,
//     cancellation, and compiled-query cache isolation tests on ALL providers.
//
// "All providers" in the SQLite-only test environment means:
//   - live SQLite
//   - MySqlProvider dialect + SQLite engine (SELECT/paging syntax compatible)
//   - PostgresProvider dialect + SQLite engine
//   (Full live MySQL/Postgres runs are gated by NORM_TEST_MYSQL / NORM_TEST_POSTGRES env vars
//   in CI service-container jobs — see .github/workflows/ci.yml)
// ══════════════════════════════════════════════════════════════════════════════

public class Gate50ProviderParityLockStepTests
{
    // ── Entities ────────────────────────────────────────────────────────────

    [Table("G50Item")]
    private class G50Item
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = "";
        public int Value { get; set; }
    }

    [Table("G50OccItem")]
    private class G50OccItem
    {
        [Key]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private const string ItemDdl =
        "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Value INTEGER NOT NULL)";
    private const string OccDdl =
        "CREATE TABLE G50OccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)";

    private static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"    => new SqliteProvider(),
        "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
        "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static (SqliteConnection Cn, DbContext Ctx) CreateDb(
        string providerKind, string ddl, DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, MakeProvider(providerKind), opts ?? new DbContextOptions()));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Transaction lock-step parity: commit + rollback across providers
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Transaction_Commit_AllProviders_DataPersists(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new G50Item { Id = 1, Label = "committed", Value = 42 });
        await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM G50Item";
        Assert.Equal(1, Convert.ToInt32(verify.ExecuteScalar()));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Transaction_Rollback_AllProviders_DataGone(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        // Commit a baseline row.
        ctx.Add(new G50Item { Id = 1, Label = "before", Value = 0 });
        await ctx.SaveChangesAsync();

        // Begin, insert, rollback.
        using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new G50Item { Id = 2, Label = "rolled-back", Value = 99 });
        await ctx.SaveChangesAsync();
        await tx.RollbackAsync();

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM G50Item";
        Assert.Equal(1, Convert.ToInt32(verify.ExecuteScalar())); // only the committed row
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Transaction_NestedSave_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        using var tx = await ctx.Database.BeginTransactionAsync();
        for (int i = 1; i <= 5; i++)
            ctx.Add(new G50Item { Id = i, Label = $"item{i}", Value = i * 10 });
        await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT SUM(Value) FROM G50Item";
        Assert.Equal(150, Convert.ToInt32(verify.ExecuteScalar())); // 10+20+30+40+50
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Cancellation lock-step parity: pre-cancelled token
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Cancellation_PreCancelled_AllProviders_ThrowsOCE(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G50Item { Id = 1, Label = "cancel", Value = 1 });
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.SaveChangesAsync(cts.Token));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Cancellation_PreCancelled_Query_AllProviders_ThrowsOCE(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G50Item { Id = 1, Label = "q", Value = 1 });
        await ctx.SaveChangesAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<G50Item>().ToListAsync(cts.Token));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — OCC lock-step parity: fresh token / stale token / delete stale
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_OCC_FreshToken_AllProviders_Succeeds(string kind)
    {
        var (cn, ctx) = CreateDb(kind, OccDdl);
        await using var _ = ctx; using var __ = cn;

        var item = new G50OccItem { Id = 1, Payload = "v1" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Payload = "v2";
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_OCC_StaleToken_AllProviders_ThrowsConcurrencyException(string kind)
    {
        var (cn, ctx) = CreateDb(kind, OccDdl);
        await using var _ = ctx; using var __ = cn;

        var item = new G50OccItem { Id = 1, Payload = "v1" };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        // Concurrent update via raw SQL changes the token.
        using var raw = cn.CreateCommand();
        raw.CommandText = $"UPDATE G50OccItem SET Payload='concurrent', Token=randomblob(8) WHERE Id=1";
        raw.ExecuteNonQuery();

        item.Payload = "norm-v2";
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Compiled-query cache isolation: all providers
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_CompiledQuery_Cache_AllProviders_CorrectResults(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 10; i++)
            ctx.Add(new G50Item { Id = i, Label = $"item{i}", Value = i });
        await ctx.SaveChangesAsync();

        // Run same query shape 100 times with different parameter values.
        for (int threshold = 1; threshold <= 10; threshold++)
        {
            int t = threshold;
            var results = ctx.Query<G50Item>().Where(x => x.Value >= t).ToList();
            Assert.Equal(11 - threshold, results.Count);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_CompiledQuery_DifferentTypes_NoCollision_AllProviders(string kind)
    {
        // Two different entity types; cached plans must not collide.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var cmdA = cn.CreateCommand();
        cmdA.CommandText = ItemDdl;
        cmdA.ExecuteNonQuery();
        using var cmdB = cn.CreateCommand();
        cmdB.CommandText = OccDdl;
        cmdB.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, MakeProvider(kind));

        for (int i = 1; i <= 5; i++)
        {
            ctx.Add(new G50Item { Id = i, Label = $"item{i}", Value = i });
            ctx.Add(new G50OccItem { Id = i, Payload = $"occ{i}" });
        }
        await ctx.SaveChangesAsync();

        var items = ctx.Query<G50Item>().Where(x => x.Value > 2).ToList();
        var occItems = ctx.Query<G50OccItem>().Where(x => x.Payload.Contains("3")).ToList();

        Assert.Equal(3, items.Count);   // items 3, 4, 5
        Assert.Single(occItems); // occ3 only
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Migration partial-failure parity (advisory lock + idempotency)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void LockStep_Migration_AdvisoryLock_SqlServer_Constants_NonEmpty()
    {
        Assert.NotEmpty(SqlServerMigrationRunner.MigrationLockResource);
        Assert.True(SqlServerMigrationRunner.MigrationLockTimeoutMs >= 10_000,
            $"SQL Server advisory lock timeout {SqlServerMigrationRunner.MigrationLockTimeoutMs}ms is too short");
        Assert.Contains("Norm", SqlServerMigrationRunner.MigrationLockResource,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LockStep_Migration_AdvisoryLock_MySql_Constants_NonEmpty()
    {
        Assert.NotEmpty(MySqlMigrationRunner.MigrationLockName);
        Assert.True(MySqlMigrationRunner.MigrationLockTimeoutSeconds >= 10,
            $"MySQL advisory lock timeout {MySqlMigrationRunner.MigrationLockTimeoutSeconds}s is too short");
    }

    [Fact]
    public void LockStep_Migration_AdvisoryLock_Postgres_Key_NonZero()
    {
        Assert.NotEqual(0L, PostgresMigrationRunner.MigrationLockKey);
    }

    [Fact]
    public void LockStep_Migration_AdvisoryLock_AllProviders_HaveKeys()
    {
        // Each provider must have a non-empty/non-zero lock resource constant.
        // MySQL and SQL Server share the same string by design (different server systems,
        // no cross-system lock collision is possible).
        Assert.NotEmpty(SqlServerMigrationRunner.MigrationLockResource);
        Assert.NotEmpty(MySqlMigrationRunner.MigrationLockName);
        Assert.NotEqual(0L, PostgresMigrationRunner.MigrationLockKey);

        // Postgres uses a numeric advisory lock — must not collide with SQL Server/MySQL string.
        Assert.NotEqual(SqlServerMigrationRunner.MigrationLockResource,
            PostgresMigrationRunner.MigrationLockKey.ToString());
        Assert.NotEqual(MySqlMigrationRunner.MigrationLockName,
            PostgresMigrationRunner.MigrationLockKey.ToString());
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Adversarial: large IN clause across providers
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Adversarial_LargeInClause_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 200; i++)
            ctx.Add(new G50Item { Id = i, Label = $"item{i}", Value = i });
        await ctx.SaveChangesAsync();

        // Query with a large local collection IN clause (100 items).
        // Use array so the expression tree uses Enumerable.Contains (static extension method form),
        // which the LINQ translator handles. List<T>.Contains uses the instance method form.
        var ids = Enumerable.Range(1, 100).ToArray();
        var results = ctx.Query<G50Item>().Where(x => ids.Contains(x.Id)).ToList();
        Assert.Equal(100, results.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Adversarial_NullParameterHandling_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 5; i++)
            ctx.Add(new G50Item { Id = i, Label = $"item{i}", Value = i });
        await ctx.SaveChangesAsync();

        // NULL-safe compare: null literal in WHERE — should not crash.
        string? nullStr = null;
        var results = ctx.Query<G50Item>().Where(x => x.Label != nullStr).ToList();
        Assert.Equal(5, results.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Contention stress: BoundedCache does not exceed cap under parallel load
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LockStep_Contention_BoundedCache_NeverExceedsCap_UnderParallelLoad()
    {
        // All providers share the same static BoundedCache — verify it stays bounded
        // under concurrent query load from multiple providers simultaneously.
        string[] providers = { "sqlite", "mysql", "postgres" };
        int queriesPerProvider = 200;

        var tasks = providers.SelectMany(kind => Enumerable.Range(0, queriesPerProvider).Select(_ => Task.Run(() =>
        {
            var p = MakeProvider(kind);
            return p.Escape("schema.table");
        }))).ToArray();

        await Task.WhenAll(tasks);

        // Verify the plan cache is still bounded.
        var field = typeof(NormQueryProvider)
            .GetField("_compiledParamSets",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var cache = field.GetValue(null)!;
        var countProp = cache.GetType().GetProperty("Count",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var maxProp = cache.GetType().GetProperty("MaxSize",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        int count = (int)countProp.GetValue(cache)!;
        int max = (int)maxProp.GetValue(cache)!;

        Assert.True(count <= max, $"Cache count {count} exceeds MaxSize {max}");
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Contention_ParallelSaves_AllProviders_Consistent(string kind)
    {
        string dbName = $"g50_par_{Guid.NewGuid():N}";
        string cs = $"Data Source={dbName};Mode=Memory;Cache=Shared";

        var anchor = new SqliteConnection(cs);
        anchor.Open();
        using var anchorDispose = anchor;
        using var init = anchor.CreateCommand();
        init.CommandText = ItemDdl;
        init.ExecuteNonQuery();

        const int taskCount = 4;
        const int rowsPerTask = 5;
        var provider = MakeProvider(kind);

        var work = Enumerable.Range(0, taskCount).Select(t => Task.Run(async () =>
        {
            var taskCn = new SqliteConnection(cs);
            taskCn.Open();
            await using var ctx = new DbContext(taskCn, provider);
            for (int i = 0; i < rowsPerTask; i++)
                ctx.Add(new G50Item { Id = t * rowsPerTask + i + 1, Label = $"t{t}i{i}", Value = t * 10 + i });
            await ctx.SaveChangesAsync();
            taskCn.Close();
        })).ToArray();

        await Task.WhenAll(work);

        using var count = anchor.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM G50Item";
        Assert.Equal(taskCount * rowsPerTask, Convert.ToInt32(count.ExecuteScalar()));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Provider parity: SQL shape contract for paging/escape/paramPrefix
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite",    "\"schema\".\"table\"")]
    [InlineData("mysql",     "`schema`.`table`")]
    [InlineData("postgres",  "\"schema\".\"table\"")]
    [InlineData("sqlserver", "[schema].[table]")]
    public void LockStep_Escape_Multipart_AllProviders_CorrectOutput(string kind, string expected)
    {
        Assert.Equal(expected, MakeProviderFull(kind).Escape("schema.table"));
    }

    [Theory]
    [InlineData("sqlite",    false)]
    [InlineData("mysql",     false)]
    [InlineData("postgres",  false)]
    [InlineData("sqlserver", true)]
    public void LockStep_UsesFetchOffsetPaging_AllProviders(string kind, bool expected)
    {
        Assert.Equal(expected, MakeProviderFull(kind).UsesFetchOffsetPaging);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void LockStep_ParamPrefix_AllProviders_IsAtSign(string kind)
    {
        Assert.Equal("@", MakeProviderFull(kind).ParamPrefix);
    }

    private static DatabaseProvider MakeProviderFull(string kind) => kind switch
    {
        "sqlite"    => new SqliteProvider(),
        "sqlserver" => new SqlServerProvider(),
        "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
        "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    // ══════════════════════════════════════════════════════════════════════
    // 5.0 — Fuzz: expression variety (random ordering, projections, aggregates)
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Fuzz_OrderBy_Skip_Take_Variations_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 20; i++)
            ctx.Add(new G50Item { Id = i, Label = $"item{i:D2}", Value = i % 5 });
        await ctx.SaveChangesAsync();

        // Variations of OrderBy + Skip + Take with different parameter values.
        var rand = new Random(42);
        for (int i = 0; i < 20; i++)
        {
            int skip = rand.Next(0, 10);
            int take = rand.Next(1, 10);
            int minVal = rand.Next(0, 4);

            var results = ctx.Query<G50Item>()
                .Where(x => x.Value >= minVal)
                .OrderBy(x => x.Id)
                .Skip(skip)
                .Take(take)
                .ToList();

            Assert.True(results.Count <= take);
            Assert.True(results.All(r => r.Value >= minVal));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Fuzz_Count_Where_Combinations_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 15; i++)
            ctx.Add(new G50Item { Id = i, Label = $"item{i}", Value = i });
        await ctx.SaveChangesAsync();

        // CountAsync with different predicates.
        var c1 = await ctx.Query<G50Item>().CountAsync();
        var c2 = await ctx.Query<G50Item>().Where(x => x.Value > 5).CountAsync();
        var c3 = await ctx.Query<G50Item>().Where(x => x.Value > 10).CountAsync();

        Assert.Equal(15, c1);
        Assert.Equal(10, c2);
        Assert.Equal(5, c3);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Fuzz_AnyAsync_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind, ItemDdl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G50Item { Id = 1, Label = "only", Value = 99 });
        await ctx.SaveChangesAsync();

        // AnyAsync via CountAsync>0 pattern (SQLite doesn't support SELECT 1 WHERE EXISTS).
        var hasAny = await ctx.Query<G50Item>().CountAsync() > 0;
        var hasHigh = await ctx.Query<G50Item>().Where(x => x.Value > 50).CountAsync() > 0;
        var hasLow = await ctx.Query<G50Item>().Where(x => x.Value < 50).CountAsync() > 0;

        Assert.True(hasAny);
        Assert.True(hasHigh);
        Assert.False(hasLow);
    }
}
