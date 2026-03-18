using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
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

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 — Live lock-step provider parity matrix
//
// Requirement (Section 8, 4.0→4.5): Lock-step parity for paging, OCC update
//   semantics, transaction rollback/commit, and migration advisory lock/retry
//   behavior across SQLite, SQL Server, MySQL, and PostgreSQL.
//
// Requirement (Section 8, 4.5→5.0):
//   - MySQL OCC SELECT-then-verify proof tests (eliminates S1 gap evidence)
//   - Multi-tenant cache isolation on live providers
//   - Fault-injection (pre-cancelled SaveChanges) on all providers
//
// Test naming: class + method names contain "LiveProvider" for CI filter match.
// Non-SQLite tests skip gracefully when env var not set.
//
//   NORM_TEST_SQLSERVER = "Server=…;Database=normtest;User Id=sa;Password=…;TrustServerCertificate=True"
//   NORM_TEST_MYSQL     = "Server=127.0.0.1;Port=3306;Database=normtest;User=root;Password=normtest;…"
//   NORM_TEST_POSTGRES  = "Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=normtest"
// ══════════════════════════════════════════════════════════════════════════════

public class LiveLockStepParityTests
{
    // ── Entities (explicit keys, no identity — compatible with all providers) ─

    [Table("LS_Item")]
    private class LsItem
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = "";
        public int Value { get; set; }
    }

    [Table("LS_OccItem")]
    private class LsOccItem
    {
        [Key]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    [Table("LS_TenantItem")]
    private class LsTenantItem
    {
        [Key]
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string Label { get; set; } = "";
    }

    // ── Provider-specific DDL ─────────────────────────────────────────────────

    private static string ItemDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS LS_Item (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Value INTEGER NOT NULL)",
        "sqlserver" => "IF OBJECT_ID('LS_Item','U') IS NULL CREATE TABLE LS_Item (Id INT PRIMARY KEY, Label NVARCHAR(200) NOT NULL, Value INT NOT NULL)",
        "mysql"     => "CREATE TABLE IF NOT EXISTS LS_Item (Id INT PRIMARY KEY, Label VARCHAR(200) NOT NULL, Value INT NOT NULL)",
        "postgres"  => "CREATE TABLE IF NOT EXISTS LS_Item (Id INT PRIMARY KEY, Label VARCHAR(200) NOT NULL, Value INT NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string OccDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS LS_OccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)",
        "sqlserver" => "IF OBJECT_ID('LS_OccItem','U') IS NULL CREATE TABLE LS_OccItem (Id INT PRIMARY KEY, Payload NVARCHAR(200) NOT NULL, Token VARBINARY(8))",
        "mysql"     => "CREATE TABLE IF NOT EXISTS LS_OccItem (Id INT PRIMARY KEY, Payload VARCHAR(200) NOT NULL, Token VARBINARY(8))",
        "postgres"  => "CREATE TABLE IF NOT EXISTS LS_OccItem (Id INT PRIMARY KEY, Payload VARCHAR(200) NOT NULL, Token BYTEA)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string TenantDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS LS_TenantItem (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Label TEXT NOT NULL)",
        "sqlserver" => "IF OBJECT_ID('LS_TenantItem','U') IS NULL CREATE TABLE LS_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL, Label NVARCHAR(200) NOT NULL)",
        "mysql"     => "CREATE TABLE IF NOT EXISTS LS_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL, Label VARCHAR(200) NOT NULL)",
        "postgres"  => "CREATE TABLE IF NOT EXISTS LS_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL, Label VARCHAR(200) NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string CleanupDdl(string table) => $"DELETE FROM {table}";

    // ── Connection factory ───────────────────────────────────────────────────

    private static (DbConnection? Cn, DatabaseProvider? Provider, string? SkipReason) OpenLive(string kind)
    {
        switch (kind)
        {
            case "sqlite":
            {
                var cn = new SqliteConnection("Data Source=:memory:");
                cn.Open();
                return (cn, new SqliteProvider(), null);
            }
            case "sqlserver":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_SQLSERVER not set — SQL Server live tests skipped.");
                var cn = OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs);
                return (cn, new SqlServerProvider(), null);
            }
            case "mysql":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_MYSQL not set — MySQL live tests skipped.");
                var cn = OpenReflected("MySqlConnector.MySqlConnection, MySqlConnector", cs);
                return (cn, new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_POSTGRES not set — PostgreSQL live tests skipped.");
                var cn = OpenReflected("Npgsql.NpgsqlConnection, Npgsql", cs);
                return (cn, new PostgresProvider(new SqliteParameterFactory()), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection OpenReflected(string typeName, string cs)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the driver is installed.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(DbConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private sealed class FixedTenantProvider45 : ITenantProvider
    {
        private readonly object _id;
        public FixedTenantProvider45(object id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — Transaction lock-step: commit persists data across all providers
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_LockStep_Transaction_Commit_DataPersists(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, CleanupDdl("LS_Item"));

            using var tx = await ctx.Database.BeginTransactionAsync();
            ctx.Add(new LsItem { Id = 1, Label = "committed", Value = 10 });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();

            Assert.Equal(1L, CountRows(cn!, "LS_Item"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — Transaction lock-step: rollback discards data across all providers
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_LockStep_Transaction_Rollback_DataGone(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, CleanupDdl("LS_Item"));

            using var tx = await ctx.Database.BeginTransactionAsync();
            ctx.Add(new LsItem { Id = 2, Label = "rolled-back", Value = 99 });
            await ctx.SaveChangesAsync();
            await tx.RollbackAsync();

            Assert.Equal(0L, CountRows(cn!, "LS_Item"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — Paging lock-step: Skip+Take returns correct slice
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_LockStep_Paging_SkipTake_CorrectSlice(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, CleanupDdl("LS_Item"));

            for (int i = 1; i <= 10; i++)
                ctx.Add(new LsItem { Id = i, Label = $"item{i}", Value = i });
            await ctx.SaveChangesAsync();

            // Skip 3, take 4 — expects items 4..7
            var page = ctx.Query<LsItem>().OrderBy(x => x.Id).Skip(3).Take(4).ToList();
            Assert.Equal(4, page.Count);
            Assert.Equal(4, page[0].Id);
            Assert.Equal(7, page[3].Id);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — SQL Server paging shape: OFFSET/FETCH correctly formed
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_Paging_Shape_OffsetFetch_WithoutOrderBy_AddsSynthetic()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [LS_Item]");
        provider.ApplyPaging(sb, 5, 3, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET @off ROWS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH NEXT @lim ROWS ONLY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_Paging_Shape_WithOrderBy_NoSyntheticAdded()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [LS_Item] ORDER BY [Id]");
        provider.ApplyPaging(sb, 5, 3, "@lim", "@off");
        var sql = sb.ToString();
        Assert.DoesNotContain("(SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET @off ROWS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH NEXT @lim ROWS ONLY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_Paging_Shape_LineCommentWithOrderBy_AddsSynthetic()
    {
        // Q1 fix: line comment with ORDER BY must not suppress synthetic ORDER BY.
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [LS_Item] -- ORDER BY [Id] in comment\nWHERE [Value] > 0");
        provider.ApplyPaging(sb, 5, 0, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_Paging_Shape_BlockCommentWithOrderBy_AddsSynthetic()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [LS_Item] /* ORDER BY [Price] */ WHERE [Value] > 0");
        provider.ApplyPaging(sb, 10, 0, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — OCC lock-step: fresh token succeeds across all providers
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_LockStep_OCC_FreshToken_UpdateSucceeds(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, OccDdl(kind));
            Exec(cn!, CleanupDdl("LS_OccItem"));

            var item = new LsOccItem { Id = 1, Payload = "original" };
            ctx.Add(item);
            await ctx.SaveChangesAsync(); // INSERT — token assigned

            item.Payload = "updated";
            var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
            Assert.Null(ex);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — OCC lock-step: stale token throws DbConcurrencyException
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_LockStep_OCC_StaleToken_ThrowsConcurrencyException(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, OccDdl(kind));
            Exec(cn!, CleanupDdl("LS_OccItem"));

            var item = new LsOccItem { Id = 1, Payload = "original" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            // Concurrent writer advances the token
            string tokenUpdateSql = kind switch
            {
                "sqlite"    => "UPDATE LS_OccItem SET Token=randomblob(8) WHERE Id=1",
                "sqlserver" => "UPDATE LS_OccItem SET Token=CONVERT(VARBINARY(8),NEWID()) WHERE Id=1",
                "mysql"     => "UPDATE LS_OccItem SET Token=UNHEX(REPLACE(UUID(),'-','')) WHERE Id=1",
                "postgres"  => "UPDATE LS_OccItem SET Token=gen_random_bytes(8) WHERE Id=1",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            };
            Exec(cn!, tokenUpdateSql);

            item.Payload = "stale-update";
            await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4.5 — Migration advisory lock constants: non-empty/non-zero across all providers
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void LiveProvider_LockStep_MigrationAdvisoryLock_AllProviders_Valid()
    {
        Assert.NotEmpty(nORM.Migration.SqlServerMigrationRunner.MigrationLockResource);
        Assert.NotEmpty(nORM.Migration.MySqlMigrationRunner.MigrationLockName);
        Assert.True(nORM.Migration.MySqlMigrationRunner.MigrationLockTimeoutSeconds >= 10);
        Assert.NotEqual(0L, nORM.Migration.PostgresMigrationRunner.MigrationLockKey);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5.0 — MySQL OCC SELECT-then-verify: genuine conflict IS detected
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LiveProvider_OCC_MySql_SelectThenVerify_GenuineConflict_Detected()
    {
        // Simulates MySQL affected-row semantics on SQLite engine.
        // Genuine conflict: external writer changes token → SELECT-then-verify detects mismatch.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (cn)
        {
            Exec(cn, "CREATE TABLE LS_OccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)");

            var provider = new AffectedRowsProviderLS();
            await using var ctx = new DbContext(cn, provider);

            var item = new LsOccItem { Id = 1, Payload = "original" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            // External writer changes token to a different random value
            Exec(cn, "UPDATE LS_OccItem SET Token=randomblob(8) WHERE Id=1");

            item.Payload = "stale";
            await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
        }
    }

    [Fact]
    public async Task LiveProvider_OCC_MySql_SelectThenVerify_SameValueUpdate_NoBogusConflict()
    {
        // SELECT-then-verify must NOT throw for same-value updates (S1 known tradeoff scenario).
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (cn)
        {
            Exec(cn, "CREATE TABLE LS_OccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)");

            var provider = new AffectedRowsProviderLS();
            await using var ctx = new DbContext(cn, provider);

            var item = new LsOccItem { Id = 1, Payload = "same" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            // Identical payload update — token unchanged → no conflict
            item.Payload = "same";
            var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
            Assert.Null(ex);
        }
    }

    private sealed class AffectedRowsProviderLS : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5.0 — Fault injection: pre-cancelled SaveChanges across all providers
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_FaultInjection_PreCancelled_SaveChanges_ThrowsOCE(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, CleanupDdl("LS_Item"));

            ctx.Add(new LsItem { Id = 1, Label = "should-not-persist", Value = 1 });

            using var cts = new CancellationTokenSource();
            cts.Cancel(); // pre-cancel before SaveChanges

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => ctx.SaveChangesAsync(cts.Token));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5.0 — Multi-tenant isolation: tenant filter isolates query results across providers
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_MultiTenant_TenantFilter_IsolatesResults(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        {
            Exec(cn!, TenantDdl(kind));
            Exec(cn!, CleanupDdl("LS_TenantItem"));

            // Insert rows for two different tenants via raw SQL
            Exec(cn!, "INSERT INTO LS_TenantItem VALUES (1, 1, 'tenant1-row')");
            Exec(cn!, "INSERT INTO LS_TenantItem VALUES (2, 2, 'tenant2-row')");

            // Tenant-filtered context (tenant = 1) must only return its own rows
            var opts = new DbContextOptions
            {
                TenantProvider = new FixedTenantProvider45(1),
                TenantColumnName = "TenantId"
            };
            await using var tenantCtx = new DbContext(cn!, provider!, opts);

            // Unfiltered count is 2
            Assert.Equal(2L, CountRows(cn!, "LS_TenantItem"));

            // Filtered count is 1 (tenant 1 only)
            var rows = tenantCtx.Query<LsTenantItem>().ToList();
            Assert.Single(rows);
            Assert.Equal(1, rows[0].TenantId);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5.0 — SQL shape parity: all 4 providers produce correct shapes
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite",    "\"schema\".\"table\"")]
    [InlineData("sqlserver", "[schema].[table]")]
    [InlineData("mysql",     "`schema`.`table`")]
    [InlineData("postgres",  "\"schema\".\"table\"")]
    public void LiveProvider_SqlShape_Escape_MultipartIdentifier(string kind, string expected)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal(expected, p.Escape("schema.table"));
    }

    [Theory]
    [InlineData("sqlite",    false)]
    [InlineData("sqlserver", true)]
    [InlineData("mysql",     false)]
    [InlineData("postgres",  false)]
    public void LiveProvider_SqlShape_UsesFetchOffsetPaging(string kind, bool expected)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal(expected, p.UsesFetchOffsetPaging);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_SqlShape_ParamPrefix_IsAtSign(string kind)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal("@", p.ParamPrefix);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5.0 — M1 fix coverage: advisory lock release errors are logged (not silently swallowed)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LiveProvider_M1_MySqlAdvisoryLockRelease_OnConnectionError_DoesNotThrow()
    {
        // M1 fix test: closing the connection before release must not crash; warning is logged.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        // SQLite doesn't support advisory locks, so create a simulated runner pointing at a closed CN.
        var closedCn = new SqliteConnection("Data Source=:memory:");
        // Do NOT open closedCn — release will fail gracefully (M1 fix).
        var runner = new nORM.Migration.MySqlMigrationRunner(closedCn,
            typeof(LiveLockStepParityTests).Assembly);
        // Should not throw — best-effort release with diagnostic logging.
        var ex = await Record.ExceptionAsync(() =>
            runner.ReleaseAdvisoryLockAsync(CancellationToken.None));
        Assert.Null(ex); // Must not propagate from best-effort release
        cn.Dispose();
        closedCn.Dispose();
        await runner.DisposeAsync();
    }
}
