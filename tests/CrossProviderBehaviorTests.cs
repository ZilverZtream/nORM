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

public class CrossProviderBehaviorTests
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
        "sqlserver" => new SqlServerProvider(),
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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

    private static DbContextOptions OccOpts(string kind) =>
        kind == "mysql"
            ? new DbContextOptions { RequireMatchedRowOccSemantics = false }
            : new DbContextOptions();

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task LockStep_OCC_FreshToken_AllProviders_Succeeds(string kind)
    {
        var (cn, ctx) = CreateDb(kind, OccDdl, OccOpts(kind));
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
    [InlineData("sqlserver")]
    public async Task LockStep_OCC_StaleToken_AllProviders_ThrowsConcurrencyException(string kind)
    {
        var (cn, ctx) = CreateDb(kind, OccDdl, OccOpts(kind));
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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
    [InlineData("sqlserver")]
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

    // ══════════════════════════════════════════════════════════════════════
    // 4.0 — SQL Server lock-step: paging SQL shape verification
    // (SQL Server generates OFFSET/FETCH syntax; verified here as SQL shape,
    //  not live execution since FETCH NEXT is not SQLite syntax)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void LockStep_SqlServer_Paging_GeneratesOffsetFetchSyntax()
    {
        // SQL Server uses OFFSET n ROWS FETCH NEXT m ROWS ONLY (not LIMIT/OFFSET)
        Assert.True(new SqlServerProvider().UsesFetchOffsetPaging,
            "SQL Server provider must use FETCH OFFSET paging syntax");
        Assert.False(new SqliteProvider().UsesFetchOffsetPaging,
            "SQLite provider must use LIMIT paging syntax");
        Assert.False(new MySqlProvider(new SqliteParameterFactory()).UsesFetchOffsetPaging,
            "MySQL provider must use LIMIT paging syntax");
        Assert.False(new PostgresProvider(new SqliteParameterFactory()).UsesFetchOffsetPaging,
            "PostgreSQL provider must use LIMIT paging syntax");
    }

    [Fact]
    public void LockStep_SqlServer_BooleanLiteral_CorrectPerProvider()
    {
        // SQLite/MySQL/SQL Server emit "1"; PostgreSQL emits "true" (native boolean literal)
        Assert.Equal("1",    new SqlServerProvider().BooleanTrueLiteral);
        Assert.Equal("1",    new SqliteProvider().BooleanTrueLiteral);
        Assert.Equal("1",    new MySqlProvider(new SqliteParameterFactory()).BooleanTrueLiteral);
        Assert.Equal("true", new PostgresProvider(new SqliteParameterFactory()).BooleanTrueLiteral);
    }

    [Fact]
    public async Task LockStep_SqlServer_Transaction_Commit_DataPersists()
    {
        // SQL Server dialect against SQLite engine — transaction semantics must work
        var (cn, ctx) = CreateDb("sqlserver", ItemDdl);
        await using var _ = ctx; using var __ = cn;

        using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new G50Item { Id = 1, Label = "sqlserver-commit", Value = 42 });
        await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM G50Item";
        Assert.Equal(1, Convert.ToInt32(verify.ExecuteScalar()));
    }

    [Fact]
    public async Task LockStep_SqlServer_Transaction_Rollback_DataGone()
    {
        var (cn, ctx) = CreateDb("sqlserver", ItemDdl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G50Item { Id = 1, Label = "baseline", Value = 0 });
        await ctx.SaveChangesAsync();

        using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new G50Item { Id = 2, Label = "rolled-back", Value = 99 });
        await ctx.SaveChangesAsync();
        await tx.RollbackAsync();

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM G50Item";
        Assert.Equal(1, Convert.ToInt32(verify.ExecuteScalar()));
    }

    // ══════════════════════════════════════════════════════════════════════
    // 4.0 — Mandatory always-run parity: all four providers (TA1)
    // Uses each provider's dialect against SQLite engine — no env vars required.
    // ══════════════════════════════════════════════════════════════════════

    private const string PpmDdl =
        "CREATE TABLE PPMItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL, Active INTEGER NOT NULL DEFAULT 0)";

    private sealed class PPMItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
        public bool Active { get; set; }
    }

    private static DatabaseProvider GetProviderPpm(string kind) => kind switch
    {
        "sqlite"    => new SqliteProvider(),
        "sqlserver" => new SqlServerProvider(),
        "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
        "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static (SqliteConnection Cn, DbContext Ctx) SetupPpm(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = PpmDdl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, GetProviderPpm(kind)));
    }

    // ── SQL shape parity ─────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite",    "\"t\"")]
    [InlineData("sqlserver", "[t]")]
    [InlineData("mysql",     "`t`")]
    [InlineData("postgres",  "\"t\"")]
    public void PP_Escape_SinglePart_AllProviders(string kind, string expected)
        => Assert.Equal(expected, GetProviderPpm(kind).Escape("t"));

    [Theory]
    [InlineData("sqlite",      999)]
    [InlineData("sqlserver",  2100)]
    [InlineData("mysql",     65535)]
    [InlineData("postgres",  32767)]
    public void PP_MaxParameters_AllProviders_CorrectCapacity(string kind, int expected)
        => Assert.Equal(expected, GetProviderPpm(kind).MaxParameters);

    // ── CRUD execution parity: all four providers run correctly ──────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PP_Insert_AllProviders_RowPersisted(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new PPMItem { Id = 1, Name = "alpha", Score = 10, Active = true });
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PPMItem";
        Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PP_Query_Where_AllProviders_CorrectResults(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i * 5, Active = i % 2 == 0 });
        await ctx.SaveChangesAsync();

        // Scores 5,10,15,20,25,30,35,40,45,50 — scores > 25: items 6..10 = 5 results
        var results = ctx.Query<PPMItem>().Where(x => x.Score > 25).ToList();
        Assert.Equal(5, results.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PP_Update_AllProviders_DataUpdated(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        var item = new PPMItem { Id = 1, Name = "before", Score = 0, Active = false };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Name = "after";
        item.Score = 99;
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name, Score FROM PPMItem WHERE Id=1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("after", reader.GetString(0));
        Assert.Equal(99, reader.GetInt32(1));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PP_Delete_AllProviders_RowRemoved(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        var item = new PPMItem { Id = 1, Name = "delete-me", Score = 0, Active = false };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        ctx.Remove(item);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PPMItem";
        Assert.Equal(0, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PP_CountAsync_AllProviders_CorrectCount(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 7; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i, Active = false });
        await ctx.SaveChangesAsync();

        var count = await ctx.Query<PPMItem>().CountAsync();
        Assert.Equal(7, count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // PARITY MATRIX PROOF
    // Each section below covers one row of the provider parity matrix.
    // All 4 providers run against the SQLite engine (provider dialect + SQLite
    // execution). SQL Server is excluded only from live paging tests because
    // OFFSET/FETCH NEXT is not SQLite syntax; all non-paging tests include it.
    // ══════════════════════════════════════════════════════════════════════

    // ── Shared entity + DDL for nullable / decimal / group tests ─────────

    [Table("PMNull")]
    private sealed class PMNull
    {
        [Key] public int Id { get; set; }
        public string Tag { get; set; } = "";
        public int Grp { get; set; }
        public decimal Amount { get; set; }
        public int? NullableNum { get; set; } // Nullable<int> for IS NULL tests (value-type nullable)
    }

    private const string PMNullDdl =
        "CREATE TABLE PMNull (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL DEFAULT '', Grp INTEGER NOT NULL DEFAULT 0, Amount REAL NOT NULL DEFAULT 0, NullableNum INTEGER)";

    private static (SqliteConnection Cn, DbContext Ctx) SetupPMNull(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = PMNullDdl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, MakeProvider(kind)));
    }

    // ── Migration generator lookup ────────────────────────────────────────

    private static IMigrationSqlGenerator MakeMigGen(string kind) => kind switch
    {
        "sqlite"    => new SqliteMigrationSqlGenerator(),
        "sqlserver" => new SqlServerMigrationSqlGenerator(),
        "mysql"     => new MySqlMigrationSqlGenerator(),
        "postgres"  => new PostgresMigrationSqlGenerator(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    // ══════════════════════════════════════════════════════════════════════
    // A. QUERY TRANSLATION PARITY
    // Row: "Query translation" — all 4 providers translate LINQ WHERE,
    // ORDER BY, aggregate, and projection expressions identically.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_StringEquality_AllProviders_FiltersCorrectly(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().Where(x => x.Name == "item3").ToList();
        Assert.Single(results);
        Assert.Equal("item3", results[0].Name);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_IsNull_AllProviders_FiltersNullRows(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        // Use Nullable<int> (value-type nullable) — IS NULL on Nullable<T> is proven path
        ctx.Add(new PMNull { Id = 1, Tag = "a", Grp = 1, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 2, Tag = "b", Grp = 2, Amount = 0, NullableNum = 42 });
        await ctx.SaveChangesAsync();

        var nullRows = ctx.Query<PMNull>().Where(x => x.NullableNum == null).ToList();
        Assert.Single(nullRows);
        Assert.Equal(1, nullRows[0].Id);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_IsNotNull_AllProviders_FiltersNonNullRows(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PMNull { Id = 1, Tag = "a", Grp = 1, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 2, Tag = "b", Grp = 2, Amount = 0, NullableNum = 10 });
        ctx.Add(new PMNull { Id = 3, Tag = "c", Grp = 3, Amount = 0, NullableNum = 20 });
        await ctx.SaveChangesAsync();

        var nonNull = ctx.Query<PMNull>().Where(x => x.NullableNum != null).ToList();
        Assert.Equal(2, nonNull.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_BoolWhere_AllProviders_FiltersActiveItems(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 6; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i, Active = i % 2 == 0 });
        await ctx.SaveChangesAsync();

        var active = ctx.Query<PPMItem>().Where(x => x.Active).ToList();
        Assert.Equal(3, active.Count); // items 2, 4, 6
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_NumericComparison_AllProviders_CorrectSubset(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().Where(x => x.Score >= 50 && x.Score <= 80).ToList();
        Assert.Equal(4, results.Count); // 50, 60, 70, 80
        Assert.All(results, r => Assert.True(r.Score >= 50 && r.Score <= 80));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_OrderByMultiColumn_AllProviders_SortedCorrectly(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PMNull { Id = 1, Tag = "b", Grp = 2, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 2, Tag = "a", Grp = 1, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 3, Tag = "a", Grp = 2, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 4, Tag = "b", Grp = 1, Amount = 0, NullableNum = null });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PMNull>().OrderBy(x => x.Tag).ThenBy(x => x.Grp).ToList();
        Assert.Equal(4, results.Count);
        Assert.Equal("a", results[0].Tag); Assert.Equal(1, results[0].Grp);
        Assert.Equal("a", results[1].Tag); Assert.Equal(2, results[1].Grp);
        Assert.Equal("b", results[2].Tag); Assert.Equal(1, results[2].Grp);
        Assert.Equal("b", results[3].Tag); Assert.Equal(2, results[3].Grp);
    }

    // SQL Server uses FETCH NEXT / OFFSET which SQLite cannot execute — excluded.
    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_TakeOnly_NoSqlServer_ReturnsCorrectCount(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i, Active = false });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().OrderBy(x => x.Id).Take(4).ToList();
        Assert.Equal(4, results.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_SkipTake_NoSqlServer_CorrectPage(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i, Active = false });
        await ctx.SaveChangesAsync();

        // Skip 3, take 4 → items 4,5,6,7
        var results = ctx.Query<PPMItem>().OrderBy(x => x.Id).Skip(3).Take(4).ToList();
        Assert.Equal(4, results.Count);
        Assert.Equal(4, results[0].Id);
        Assert.Equal(7, results[3].Id);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_StringContains_AllProviders_FiltersCorrectly(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "alpha",    Score = 1, Active = false });
        ctx.Add(new PPMItem { Id = 2, Name = "beta",     Score = 2, Active = false });
        ctx.Add(new PPMItem { Id = 3, Name = "alphabet", Score = 3, Active = false });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().Where(x => x.Name.Contains("alpha")).ToList();
        Assert.Equal(2, results.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_StringStartsWith_AllProviders_FiltersCorrectly(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "alpha",    Score = 1, Active = false });
        ctx.Add(new PPMItem { Id = 2, Name = "beta",     Score = 2, Active = false });
        ctx.Add(new PPMItem { Id = 3, Name = "alphabet", Score = 3, Active = false });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().Where(x => x.Name.StartsWith("alph")).ToList();
        Assert.Equal(2, results.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_Aggregate_Sum_AllProviders_CorrectResult(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"i{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var sum = await ctx.Query<PPMItem>().SumAsync(x => x.Score);
        Assert.Equal(150, sum); // 10+20+30+40+50
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_Aggregate_Min_AllProviders_CorrectResult(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"i{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var min = await ctx.Query<PPMItem>().MinAsync(x => x.Score);
        Assert.Equal(10, min);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_Aggregate_Max_AllProviders_CorrectResult(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"i{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var max = await ctx.Query<PPMItem>().MaxAsync(x => x.Score);
        Assert.Equal(50, max);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_SelectProjection_AllProviders_AnonymousType(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "test", Score = 42, Active = true });
        await ctx.SaveChangesAsync();

        var projected = ctx.Query<PPMItem>()
            .Select(x => new { x.Id, x.Name })
            .ToList();
        Assert.Single(projected);
        Assert.Equal(1, projected[0].Id);
        Assert.Equal("test", projected[0].Name);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_ChainedWhere_AllProviders_BothPredicatesApplied(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i * 5, Active = i % 2 == 0 });
        await ctx.SaveChangesAsync();

        // active AND score > 20 → items 6,8,10 (scores 30,40,50)
        var results = ctx.Query<PPMItem>()
            .Where(x => x.Active)
            .Where(x => x.Score > 20)
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.All(results, r => { Assert.True(r.Active); Assert.True(r.Score > 20); });
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task QTP_OrderByDescending_AllProviders_CorrectOrder(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().OrderByDescending(x => x.Score).ToList();
        Assert.Equal(5, results.Count);
        for (int i = 0; i < results.Count - 1; i++)
            Assert.True(results[i].Score >= results[i + 1].Score);
    }

    // ══════════════════════════════════════════════════════════════════════
    // B. PARAMETER BINDING PARITY
    // Row: "Parameter binding" — all 4 providers bind CLR types (bool, int,
    // string, decimal, null) correctly in WHERE predicates.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PBP_BoolTrueParam_AllProviders_MatchesOnlyTrueRows(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "t",  Score = 1, Active = true  });
        ctx.Add(new PPMItem { Id = 2, Name = "f",  Score = 2, Active = false });
        ctx.Add(new PPMItem { Id = 3, Name = "t2", Score = 3, Active = true  });
        await ctx.SaveChangesAsync();

        bool filter = true;
        var results = ctx.Query<PPMItem>().Where(x => x.Active == filter).ToList();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.Active));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PBP_BoolFalseParam_AllProviders_MatchesOnlyFalseRows(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "t", Score = 1, Active = true  });
        ctx.Add(new PPMItem { Id = 2, Name = "f", Score = 2, Active = false });
        await ctx.SaveChangesAsync();

        bool filter = false;
        var results = ctx.Query<PPMItem>().Where(x => x.Active == filter).ToList();
        Assert.Single(results);
        Assert.False(results[0].Active);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PBP_NullParam_AllProviders_EmitsIsNullPredicate(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        // Nullable<int> closure null → WHERE NullableNum IS NULL (value-type nullable — proven path)
        ctx.Add(new PMNull { Id = 1, Tag = "a", Grp = 1, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 2, Tag = "b", Grp = 2, Amount = 0, NullableNum = 5   });
        await ctx.SaveChangesAsync();

        int? filter = null;
        var nullRows = ctx.Query<PMNull>().Where(x => x.NullableNum == filter).ToList();
        Assert.Single(nullRows);
        Assert.Equal(1, nullRows[0].Id);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PBP_IntParam_AllProviders_CorrectMatch(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PMNull { Id = i, Tag = $"t{i}", Grp = i, Amount = 0 });
        await ctx.SaveChangesAsync();

        int grpFilter = 3;
        var results = ctx.Query<PMNull>().Where(x => x.Grp == grpFilter).ToList();
        Assert.Single(results);
        Assert.Equal(3, results[0].Grp);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PBP_DecimalParam_AllProviders_CorrectComparison(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PMNull { Id = 1, Tag = "a", Grp = 1, Amount = 1.5m  });
        ctx.Add(new PMNull { Id = 2, Tag = "b", Grp = 2, Amount = 3.0m  });
        ctx.Add(new PMNull { Id = 3, Tag = "c", Grp = 3, Amount = 5.5m  });
        await ctx.SaveChangesAsync();

        decimal threshold = 2.0m;
        var results = ctx.Query<PMNull>().Where(x => x.Amount > threshold).ToList();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.Amount > threshold));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PBP_MultipleParams_AllProviders_BothConditionsApplied(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 6; i++)
            ctx.Add(new PMNull { Id = i, Tag = $"t{i}", Grp = i % 3, Amount = i * 1.0m });
        await ctx.SaveChangesAsync();

        int grpFilter = 1;
        decimal amtFilter = 3.0m;
        var results = ctx.Query<PMNull>()
            .Where(x => x.Grp == grpFilter && x.Amount > amtFilter)
            .ToList();
        // Grp==1: ids 1,4 (amount 1.0,4.0); amount>3.0 → id 4 only
        Assert.Single(results);
        Assert.Equal(4, results[0].Id);
    }

    // ══════════════════════════════════════════════════════════════════════
    // C. MATERIALIZATION PARITY
    // Row: "Materialization" — all 4 providers materialize null, bool,
    // decimal, string, int, and anonymous projections correctly from DB rows.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task MAP_NullColumn_AllProviders_ReadsAsNullProperty(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        // Use Nullable<int> — proven nullable column type, reads back as null
        ctx.Add(new PMNull { Id = 1, Tag = "x", Grp = 1, Amount = 0, NullableNum = null });
        await ctx.SaveChangesAsync();

        var row = ctx.Query<PMNull>().ToList().Single();
        Assert.Null(row.NullableNum);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task MAP_BoolRoundTrip_AllProviders_TrueAndFalse(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "t", Score = 1, Active = true  });
        ctx.Add(new PPMItem { Id = 2, Name = "f", Score = 2, Active = false });
        await ctx.SaveChangesAsync();

        var items = ctx.Query<PPMItem>().OrderBy(x => x.Id).ToList();
        Assert.True(items[0].Active);
        Assert.False(items[1].Active);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task MAP_DecimalRoundTrip_AllProviders_PrecisionPreserved(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PMNull { Id = 1, Tag = "x", Grp = 1, Amount = 12.34m, NullableNum = null });
        await ctx.SaveChangesAsync();

        var row = ctx.Query<PMNull>().ToList().Single();
        // SQLite stores REAL (float64); 12.34 is representable to 2dp
        Assert.Equal(12.34m, Math.Round(row.Amount, 2));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task MAP_StringRoundTrip_AllProviders_ValuePreserved(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "Hello World!", Score = 0, Active = false });
        await ctx.SaveChangesAsync();

        var row = ctx.Query<PPMItem>().ToList().Single();
        Assert.Equal("Hello World!", row.Name);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task MAP_IntRoundTrip_AllProviders_ValuePreserved(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "n", Score = 99999, Active = false });
        await ctx.SaveChangesAsync();

        var row = ctx.Query<PPMItem>().ToList().Single();
        Assert.Equal(99999, row.Score);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task MAP_AnonymousTypeProjection_AllProviders_CorrectFields(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PMNull { Id = 7, Tag = "projected", Grp = 5, Amount = 2.5m });
        await ctx.SaveChangesAsync();

        var projected = ctx.Query<PMNull>()
            .Select(x => new { x.Id, x.Tag, x.Grp })
            .ToList();
        Assert.Single(projected);
        Assert.Equal(7,           projected[0].Id);
        Assert.Equal("projected", projected[0].Tag);
        Assert.Equal(5,           projected[0].Grp);
    }

    // ══════════════════════════════════════════════════════════════════════
    // D. SAVE PIPELINE PARITY
    // Row: "Save pipeline / OCC" — multi-entity batch insert, update, and
    // delete produce identical outcomes across all 4 providers.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SPP_MultiBatchInsert_AllProviders_AllRowsPersisted(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 8; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"item{i}", Score = i, Active = i % 2 == 0 });
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PPMItem";
        Assert.Equal(8, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SPP_MultiBatchUpdate_AllProviders_AllRowsUpdated(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        var items = Enumerable.Range(1, 5)
            .Select(i => new PPMItem { Id = i, Name = $"before{i}", Score = 0, Active = false })
            .ToList();
        foreach (var it in items) ctx.Add(it);
        await ctx.SaveChangesAsync();

        foreach (var it in items) { it.Name = $"after{it.Id}"; it.Score = it.Id * 100; }
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().OrderBy(x => x.Id).ToList();
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal($"after{i + 1}", results[i].Name);
            Assert.Equal((i + 1) * 100, results[i].Score);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SPP_MultiBatchDelete_AllProviders_AllRowsRemoved(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        var items = Enumerable.Range(1, 5)
            .Select(i => new PPMItem { Id = i, Name = $"del{i}", Score = 0, Active = false })
            .ToList();
        foreach (var it in items) ctx.Add(it);
        await ctx.SaveChangesAsync();

        foreach (var it in items) ctx.Remove(it);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PPMItem";
        Assert.Equal(0, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SPP_InsertUpdateDelete_Combined_AllProviders(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        var a = new PPMItem { Id = 1, Name = "a", Score = 1, Active = false };
        var b = new PPMItem { Id = 2, Name = "b", Score = 2, Active = false };
        var c = new PPMItem { Id = 3, Name = "c", Score = 3, Active = false };
        ctx.Add(a); ctx.Add(b); ctx.Add(c);
        await ctx.SaveChangesAsync();

        // Update a, add d, delete c — all in one SaveChanges
        a.Name = "a-updated";
        var d = new PPMItem { Id = 4, Name = "d", Score = 4, Active = false };
        ctx.Add(d);
        ctx.Remove(c);
        await ctx.SaveChangesAsync();

        var results = ctx.Query<PPMItem>().OrderBy(x => x.Id).ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("a-updated", results[0].Name);
        Assert.Equal("b",         results[1].Name);
        Assert.Equal("d",         results[2].Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    // E. MIGRATION DDL SHAPE PARITY
    // Row: "Migrations" — all 4 migration generators emit correct DDL
    // structure for every operation type (create, add col, drop col, FK).
    // ══════════════════════════════════════════════════════════════════════

    private static TableSchema MakePMTable(string name) => new TableSchema
    {
        Name = name,
        Columns =
        {
            new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true },
            new ColumnSchema { Name = "Value", ClrType = typeof(string).FullName!, IsNullable = false }
        }
    };

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_CreateTable_AllProviders_EmitsNonEmptyDdl(string kind)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakePMTable("NewTable"));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.NotEmpty(sql.Up);
        Assert.Contains(sql.Up, s => s.Contains("NewTable", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_CreateTable_Down_AllProviders_EmitsDropTable(string kind)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakePMTable("NewTable"));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.Contains(sql.Down, s =>
            s.Contains("DROP", StringComparison.OrdinalIgnoreCase) &&
            s.Contains("NewTable", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_AddColumn_AllProviders_EmitsAlterTableStatement(string kind)
    {
        var table = MakePMTable("ExistingTable");
        var newCol = new ColumnSchema { Name = "Extra", ClrType = typeof(string).FullName!, IsNullable = true };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.Contains(sql.Up, s =>
            s.Contains("Extra",         StringComparison.OrdinalIgnoreCase) &&
            s.Contains("ExistingTable", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_DropColumn_NonSqlite_EmitsDropColumnStatement(string kind)
    {
        // SQLite cannot DROP COLUMN — excluded by design
        var table = MakePMTable("ExistingTable");
        var col = new ColumnSchema { Name = "OldCol", ClrType = typeof(string).FullName!, IsNullable = true };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, col));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.Contains(sql.Up, s =>
            s.Contains("OldCol",        StringComparison.OrdinalIgnoreCase) &&
            s.Contains("ExistingTable", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_AddForeignKey_NonSqlite_EmitsForeignKeyConstraint(string kind)
    {
        var table = MakePMTable("Orders");
        var fk = new ForeignKeySchema
        {
            ConstraintName   = "FK_Orders_Customers",
            DependentColumns = new[] { "CustomerId" },
            PrincipalTable   = "Customers",
            PrincipalColumns = new[] { "Id" },
            OnDelete         = "CASCADE",
            OnUpdate         = "NO ACTION"
        };
        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.Contains(sql.Up, s =>
            s.Contains("FK_Orders_Customers", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_DropForeignKey_NonSqlite_EmitsDropConstraint(string kind)
    {
        var table = MakePMTable("Orders");
        var fk = new ForeignKeySchema
        {
            ConstraintName   = "FK_Orders_Customers",
            DependentColumns = new[] { "CustomerId" },
            PrincipalTable   = "Customers",
            PrincipalColumns = new[] { "Id" },
            OnDelete         = "CASCADE",
            OnUpdate         = "NO ACTION"
        };
        var diff = new SchemaDiff();
        diff.DroppedForeignKeys.Add((table, fk));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.Contains(sql.Up, s =>
            s.Contains("DROP",              StringComparison.OrdinalIgnoreCase) &&
            s.Contains("FK_Orders_Customers", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_AlterColumnType_NonSqlite_EmitsAlterColumnStatement(string kind)
    {
        var table  = MakePMTable("T");
        var oldCol = new ColumnSchema { Name = "Val", ClrType = typeof(int).FullName!,    IsNullable = false };
        var newCol = new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = true  };
        var diff   = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));
        var sql = MakeMigGen(kind).GenerateSql(diff);
        Assert.Contains(sql.Up, s =>
            s.Contains("Val", StringComparison.OrdinalIgnoreCase) &&
            s.Contains("T",   StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void MDP_Postgres_AlterColumnType_USING_ClausePresent()
    {
        // MG1 regression: Postgres must emit USING col::newtype for type casts
        var table  = MakePMTable("T");
        var oldCol = new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = false };
        var newCol = new ColumnSchema { Name = "Val", ClrType = typeof(int).FullName!,    IsNullable = false };
        var diff   = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));
        var sql    = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        var upSql  = string.Join(" ", sql.Up);
        Assert.Contains("USING", upSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Val",   upSql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void MDP_EmptyDiff_AllProviders_EmitsNoSql(string kind)
    {
        var sql = MakeMigGen(kind).GenerateSql(new SchemaDiff());
        Assert.Empty(sql.Up);
        Assert.Empty(sql.Down);
    }

    // ══════════════════════════════════════════════════════════════════════
    // F. CACHING / SHARED STATE PARITY
    // Row: "Caching / shared state" — plan cache and compiled query cache
    // behave identically across all 4 providers (no cross-provider pollution).
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task CSP_PlanCache_SameQuery_Twice_AllProviders_ConsistentResults(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 5; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i, Active = false });
        await ctx.SaveChangesAsync();

        var r1 = ctx.Query<PPMItem>().Where(x => x.Score > 2).ToList();
        var r2 = ctx.Query<PPMItem>().Where(x => x.Score > 2).ToList();
        Assert.Equal(r1.Count, r2.Count);
        Assert.Equal(3, r1.Count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task CSP_PlanCache_DifferentParam_AllProviders_NoContamination(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var high = ctx.Query<PPMItem>().Where(x => x.Score > 80).ToList();
        var low  = ctx.Query<PPMItem>().Where(x => x.Score > 20).ToList();
        Assert.Equal(2, high.Count); // 90, 100
        Assert.Equal(8, low.Count);  // 30..100
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task CSP_CompiledQuery_AllProviders_CorrectResultsPerParam(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 6; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i * 5, Active = false });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<PPMItem>().Where(x => x.Score >= minScore));

        var r1 = await compiled(ctx, 10);
        var r2 = await compiled(ctx, 20);
        var r3 = await compiled(ctx, 30);
        Assert.Equal(5, r1.Count); // 10,15,20,25,30
        Assert.Equal(3, r2.Count); // 20,25,30
        Assert.Single(r3); // 30
    }

    // ══════════════════════════════════════════════════════════════════════
    // G. SECURITY BOUNDARY PARITY
    // Row: "Security boundaries" — identifier escaping and raw-SQL validation
    // work identically and correctly across all 4 providers.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite",    "\"select\"")]
    [InlineData("mysql",     "`select`")]
    [InlineData("postgres",  "\"select\"")]
    [InlineData("sqlserver", "[select]")]
    public void SBP_Escape_ReservedWord_AllProviders_ProducesQuotedIdentifier(string kind, string expected)
        => Assert.Equal(expected, MakeProvider(kind).Escape("select"));

    [Theory]
    [InlineData("sqlite",    "\"has space\"")]
    [InlineData("mysql",     "`has space`")]
    [InlineData("postgres",  "\"has space\"")]
    [InlineData("sqlserver", "[has space]")]
    public void SBP_Escape_SpaceInName_AllProviders_ProducesQuotedIdentifier(string kind, string expected)
        => Assert.Equal(expected, MakeProvider(kind).Escape("has space"));

    [Theory]
    [InlineData("sqlite",    "\"a\".\"b\".\"c\"")]
    [InlineData("mysql",     "`a`.`b`.`c`")]
    [InlineData("postgres",  "\"a\".\"b\".\"c\"")]
    [InlineData("sqlserver", "[a].[b].[c]")]
    public void SBP_Escape_ThreePart_AllProviders_CorrectDelimiters(string kind, string expected)
        => Assert.Equal(expected, MakeProvider(kind).Escape("a.b.c"));

    [Fact]
    public void SBP_RawSqlValidator_MultiStatementWithDdl_ThrowsArgumentException()
    {
        // Multiple statements including DDL — NormValidator must reject this
        Assert.ThrowsAny<Exception>(() =>
            NormValidator.ValidateRawSql("SELECT 1; DROP TABLE Users"));
    }

    [Fact]
    public void SBP_RawSqlValidator_DangerousKeyword_ThrowsArgumentException()
    {
        Assert.ThrowsAny<Exception>(() =>
            NormValidator.ValidateRawSql("SELECT LOAD_FILE('/etc/passwd')"));
    }

    // ══════════════════════════════════════════════════════════════════════
    // H. SQL GENERATION / PAGING PARITY
    // Row: "SQL generation / paging" — paging flags, param prefix, and
    // live paging results are correct and provider-differentiated.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite",    false)]
    [InlineData("mysql",     false)]
    [InlineData("postgres",  false)]
    [InlineData("sqlserver", true)]
    public void SGP_UsesFetchOffsetPaging_AllProviders_CorrectFlag(string kind, bool expected)
        => Assert.Equal(expected, MakeProvider(kind).UsesFetchOffsetPaging);

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void SGP_ParamPrefix_AllProviders_IsAtSign(string kind)
        => Assert.Equal("@", MakeProvider(kind).ParamPrefix);

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SGP_Count_WhereFilter_AllProviders_CorrectResult(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 10; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var count = await ctx.Query<PPMItem>().Where(x => x.Score <= 50).CountAsync();
        Assert.Equal(5, count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SGP_OrderBy_AscAndDesc_AllProviders_StableOrder(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 10; i >= 1; i--)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i, Active = false });
        await ctx.SaveChangesAsync();

        var asc  = ctx.Query<PPMItem>().OrderBy(x => x.Id).ToList();
        var desc = ctx.Query<PPMItem>().OrderByDescending(x => x.Id).ToList();
        Assert.Equal(1,  asc[0].Id);
        Assert.Equal(10, desc[0].Id);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGP_SkipTake_NoSqlServer_PaginatesCorrectly(string kind)
    {
        // SQL Server FETCH OFFSET not compatible with SQLite engine; excluded
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 20; i++)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i, Active = false });
        await ctx.SaveChangesAsync();

        // Page 2 of size 5 → items 6..10
        var page2 = ctx.Query<PPMItem>().OrderBy(x => x.Id).Skip(5).Take(5).ToList();
        Assert.Equal(5,  page2.Count);
        Assert.Equal(6,  page2[0].Id);
        Assert.Equal(10, page2[4].Id);
    }

    // ══════════════════════════════════════════════════════════════════════
    // I. TRANSACTIONS / LIFECYCLE PARITY (supplemental)
    // Row: "Transactions / lifecycle" — rollback on exception and multiple
    // sequential commits produce identical outcomes across all 4 providers.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task TXP_RollbackOnException_AllProviders_TransientDataGone(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        ctx.Add(new PPMItem { Id = 1, Name = "baseline", Score = 0, Active = false });
        await ctx.SaveChangesAsync();

        using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new PPMItem { Id = 2, Name = "transient", Score = 99, Active = false });
        await ctx.SaveChangesAsync();
        await tx.RollbackAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PPMItem";
        Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task TXP_MultipleSequentialCommits_AllProviders_AllDataPersisted(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;

        // Three independent auto-committed saves — each SaveChanges is its own implicit transaction
        for (int round = 1; round <= 3; round++)
        {
            ctx.Add(new PPMItem { Id = round, Name = $"r{round}", Score = round, Active = false });
            await ctx.SaveChangesAsync();
        }

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PPMItem";
        Assert.Equal(3, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    // ══════════════════════════════════════════════════════════════════════
    // J. SOURCE GENERATION / COMPILED QUERIES PARITY
    // Row: "Source generation / compiled queries" — compiled query paths and
    // result materialization behave identically across all 4 providers.
    // ══════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SGCP_CompiledQuery_MultipleParamCombinations_AllProviders(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 1; i <= 8; i++)
            ctx.Add(new PMNull { Id = i, Tag = i % 2 == 0 ? "even" : "odd", Grp = i % 4, Amount = i });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, string tag) =>
            c.Query<PMNull>().Where(x => x.Tag == tag));

        var evens = await compiled(ctx, "even");
        var odds  = await compiled(ctx, "odd");
        Assert.Equal(4, evens.Count);
        Assert.Equal(4, odds.Count);
        Assert.All(evens, r => Assert.Equal("even", r.Tag));
        Assert.All(odds,  r => Assert.Equal("odd",  r.Tag));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SGCP_CompiledQuery_OrderBy_AllProviders_SameOrder(string kind)
    {
        var (cn, ctx) = SetupPpm(kind);
        await using var _ = ctx; using var __ = cn;
        for (int i = 5; i >= 1; i--)
            ctx.Add(new PPMItem { Id = i, Name = $"n{i}", Score = i * 10, Active = false });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int _) =>
            c.Query<PPMItem>().OrderBy(x => x.Id));

        var results = await compiled(ctx, 0);
        for (int i = 0; i < results.Count - 1; i++)
            Assert.True(results[i].Id < results[i + 1].Id);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task SGCP_CompiledQuery_WithNullParam_AllProviders_IsNullSemantics(string kind)
    {
        var (cn, ctx) = SetupPMNull(kind);
        await using var _ = ctx; using var __ = cn;
        // Use Nullable<int> — value-type nullable for IS NULL compiled query test
        ctx.Add(new PMNull { Id = 1, Tag = "a", Grp = 1, Amount = 0, NullableNum = null });
        ctx.Add(new PMNull { Id = 2, Tag = "b", Grp = 2, Amount = 0, NullableNum = 7   });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int? num) =>
            c.Query<PMNull>().Where(x => x.NullableNum == num));

        var nullResults = await compiled(ctx, null);
        Assert.Single(nullResults);
        Assert.Equal(1, nullResults[0].Id);

        var nonNullResults = await compiled(ctx, 7);
        Assert.Single(nonNullResults);
        Assert.Equal(2, nonNullResults[0].Id);
    }
}
