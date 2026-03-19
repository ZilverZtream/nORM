using System;
using System.Collections.Generic;
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
using nORM.Execution;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// =============================================================================
// Gate 3.5 -> 4.0 -- Provider-parity bulk operations matrix
//
// Audit Section 8 gate 4.0 requirement:
//   "Live-provider parity suites for SQLite/MSSQL/MySQL/PostgreSQL covering
//    paging, OCC, tenant isolation, bulk writes, retries, savepoints, and
//    migrations."
//
// SQLite tests run live (in-memory). SQL Server, MySQL, and PostgreSQL are
// env-gated via:
//   NORM_TEST_SQLSERVER_CS, NORM_TEST_MYSQL_CS, NORM_TEST_POSTGRES_CS
//
// Non-SQLite providers that lack env vars are silently skipped (return early).
// =============================================================================

public class BulkProviderParityTests
{
    // -- Entities ---------------------------------------------------------------

    [Table("G40_Item")]
    private class G40Item
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = "";
        public int Value { get; set; }
    }

    [Table("G40_OccItem")]
    private class G40OccItem
    {
        [Key]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    [Table("G40_OccStr")]
    private class G40OccStr
    {
        [Key]
        public int Id { get; set; }
        public string Value { get; set; } = "";
        [Timestamp]
        public string? RowVersion { get; set; }
    }

    [Table("G40_TenantItem")]
    private class G40TenantItem
    {
        [Key]
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string Label { get; set; } = "";
    }

    // -- Tenant provider --------------------------------------------------------

    private sealed class FixedTenantProvider40 : ITenantProvider
    {
        private readonly object _id;
        public FixedTenantProvider40(object id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // -- DDL per provider kind --------------------------------------------------

    private static string ItemDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS G40_Item (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Value INTEGER NOT NULL)",
        "sqlserver" => "IF OBJECT_ID('G40_Item','U') IS NULL CREATE TABLE G40_Item (Id INT PRIMARY KEY, Label NVARCHAR(200) NOT NULL, Value INT NOT NULL)",
        "mysql"     => "CREATE TABLE IF NOT EXISTS G40_Item (Id INT PRIMARY KEY, Label VARCHAR(200) NOT NULL, Value INT NOT NULL)",
        "postgres"  => "CREATE TABLE IF NOT EXISTS G40_Item (Id INT PRIMARY KEY, Label VARCHAR(200) NOT NULL, Value INT NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string OccDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS G40_OccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)",
        "sqlserver" => "IF OBJECT_ID('G40_OccItem','U') IS NULL CREATE TABLE G40_OccItem (Id INT PRIMARY KEY, Payload NVARCHAR(200) NOT NULL, Token VARBINARY(8))",
        "mysql"     => "CREATE TABLE IF NOT EXISTS G40_OccItem (Id INT PRIMARY KEY, Payload VARCHAR(200) NOT NULL, Token VARBINARY(8))",
        "postgres"  => "CREATE TABLE IF NOT EXISTS G40_OccItem (Id INT PRIMARY KEY, Payload VARCHAR(200) NOT NULL, Token BYTEA)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string OccStrDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS G40_OccStr (Id INTEGER PRIMARY KEY, Value TEXT NOT NULL, RowVersion TEXT)",
        "sqlserver" => "IF OBJECT_ID('G40_OccStr','U') IS NULL CREATE TABLE G40_OccStr (Id INT PRIMARY KEY, Value NVARCHAR(200) NOT NULL, RowVersion NVARCHAR(100))",
        "mysql"     => "CREATE TABLE IF NOT EXISTS G40_OccStr (Id INT PRIMARY KEY, Value VARCHAR(200) NOT NULL, RowVersion VARCHAR(100))",
        "postgres"  => "CREATE TABLE IF NOT EXISTS G40_OccStr (Id INT PRIMARY KEY, Value VARCHAR(200) NOT NULL, RowVersion VARCHAR(100))",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string TenantDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS G40_TenantItem (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Label TEXT NOT NULL)",
        "sqlserver" => "IF OBJECT_ID('G40_TenantItem','U') IS NULL CREATE TABLE G40_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL, Label NVARCHAR(200) NOT NULL)",
        "mysql"     => "CREATE TABLE IF NOT EXISTS G40_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL, Label VARCHAR(200) NOT NULL)",
        "postgres"  => "CREATE TABLE IF NOT EXISTS G40_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL, Label VARCHAR(200) NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string Cleanup(string table) => $"DELETE FROM {table}";

    // -- Connection factory -----------------------------------------------------

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
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_SQLSERVER_CS");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_SQLSERVER_CS not set");
                var cn = OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs);
                return (cn, new SqlServerProvider(), null);
            }
            case "mysql":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_MYSQL_CS");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_MYSQL_CS not set");
                var cn = OpenReflected("MySqlConnector.MySqlConnection, MySqlConnector", cs);
                return (cn, new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_POSTGRES_CS");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_POSTGRES_CS not set");
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

    private static object? ReadScalar(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    // -- SQLite-only helper for in-memory contexts ------------------------------

    private static (SqliteConnection Cn, DbContext Ctx) CreateSqliteDb(
        string ddl, DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider(), opts ?? new DbContextOptions()));
    }

    // =========================================================================
    // 1. Paging parity: Skip + Take produces correct subset
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Paging_SkipTake_CorrectSubset_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, Cleanup("G40_Item"));

            for (int i = 1; i <= 20; i++)
                ctx.Add(new G40Item { Id = i, Label = $"item{i:D2}", Value = i });
            await ctx.SaveChangesAsync();

            // Skip 5, take 7 -- expects items 6..12
            var page = ctx.Query<G40Item>().OrderBy(x => x.Id).Skip(5).Take(7).ToList();
            Assert.Equal(7, page.Count);
            Assert.Equal(6, page[0].Id);
            Assert.Equal(12, page[6].Id);
        }
    }

    [Fact]
    public async Task Paging_SkipTake_EmptyResult_WhenSkipExceedsCount_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 5; i++)
            ctx.Add(new G40Item { Id = i, Label = $"item{i}", Value = i });
        await ctx.SaveChangesAsync();

        var page = ctx.Query<G40Item>().OrderBy(x => x.Id).Skip(100).Take(10).ToList();
        Assert.Empty(page);
    }

    // =========================================================================
    // 2. OCC parity: BulkUpdate with stale token returns 0 updated
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task OCC_BulkUpdate_StaleToken_ZeroUpdated_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, OccStrDdl(kind));
            Exec(cn!, Cleanup("G40_OccStr"));

            // Insert row with known token
            Exec(cn!, "INSERT INTO G40_OccStr (Id, Value, RowVersion) VALUES (1, 'original', 'tok-v1')");

            // Simulate concurrent write that changed the token
            Exec(cn!, "UPDATE G40_OccStr SET RowVersion='tok-v2' WHERE Id=1");

            // BulkUpdate with stale token -- should NOT update
            var entity = new G40OccStr { Id = 1, Value = "tampered", RowVersion = "tok-v1" };
            var updated = await ctx.BulkUpdateAsync(new[] { entity });

            Assert.Equal(0, updated);
            var actual = ReadScalar(cn!, "SELECT Value FROM G40_OccStr WHERE Id=1") as string;
            Assert.Equal("original", actual);
        }
    }

    [Fact]
    public async Task OCC_BulkUpdate_FreshToken_UpdatesRow_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(OccStrDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40_OccStr (Id, Value, RowVersion) VALUES (1, 'before', 'tok-A')");

        var entity = new G40OccStr { Id = 1, Value = "after", RowVersion = "tok-A" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal(1, updated);
        Assert.Equal("after", ReadScalar(cn, "SELECT Value FROM G40_OccStr WHERE Id=1") as string);
    }

    // =========================================================================
    // 3. Tenant isolation parity: BulkUpdate/BulkDelete with cross-tenant PK
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Tenant_BulkUpdate_CrossTenant_ZeroRows_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider40(1),
            TenantColumnName = "TenantId"
        };

        using (cn) await using (var ctx = new DbContext(cn!, provider!, opts))
        {
            Exec(cn!, TenantDdl(kind));
            Exec(cn!, Cleanup("G40_TenantItem"));

            // Row belongs to tenant 2
            Exec(cn!, "INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES (1, 2, 'tenant2-row')");

            // BulkUpdate from tenant 1 context -- should affect 0 rows
            var entity = new G40TenantItem { Id = 1, TenantId = 1, Label = "tampered" };
            await ctx.BulkUpdateAsync(new[] { entity });

            var actual = ReadScalar(cn!, "SELECT Label FROM G40_TenantItem WHERE Id=1") as string;
            Assert.Equal("tenant2-row", actual);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Tenant_BulkDelete_CrossTenant_ZeroRows_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider40(1),
            TenantColumnName = "TenantId"
        };

        using (cn) await using (var ctx = new DbContext(cn!, provider!, opts))
        {
            Exec(cn!, TenantDdl(kind));
            Exec(cn!, Cleanup("G40_TenantItem"));

            // Row belongs to tenant 2
            Exec(cn!, "INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES (1, 2, 'keep-me')");

            var entity = new G40TenantItem { Id = 1, TenantId = 1, Label = "" };
            await ctx.BulkDeleteAsync(new[] { entity });

            Assert.Equal(1L, CountRows(cn!, "G40_TenantItem"));
        }
    }

    [Fact]
    public async Task Tenant_BulkUpdate_SameTenant_UpdatesOwnRow_SQLite()
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider40(1),
            TenantColumnName = "TenantId"
        };
        var (cn, ctx) = CreateSqliteDb(TenantDdl("sqlite"), opts);
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES (1, 1, 'before')");

        var entity = new G40TenantItem { Id = 1, TenantId = 1, Label = "after" };
        await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal("after", ReadScalar(cn, "SELECT Label FROM G40_TenantItem WHERE Id=1") as string);
    }

    // =========================================================================
    // 4. Bulk insert parity: BulkInsertAsync inserts correct row count
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task BulkInsert_CorrectRowCount_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, Cleanup("G40_Item"));

            var items = Enumerable.Range(1, 50)
                .Select(i => new G40Item { Id = i, Label = $"bulk{i}", Value = i * 10 })
                .ToList();

            var inserted = await ctx.BulkInsertAsync(items);
            Assert.Equal(50, inserted);
            Assert.Equal(50L, CountRows(cn!, "G40_Item"));
        }
    }

    [Fact]
    public async Task BulkInsert_EmptyCollection_ThrowsArgumentException_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        // nORM validator rejects empty bulk operations with ArgumentException
        await Assert.ThrowsAsync<ArgumentException>(
            () => ctx.BulkInsertAsync(Array.Empty<G40Item>()));
    }

    [Fact]
    public async Task BulkInsert_LargeBatch_Succeeds_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        var items = Enumerable.Range(1, 500)
            .Select(i => new G40Item { Id = i, Label = $"big{i}", Value = i })
            .ToList();

        var inserted = await ctx.BulkInsertAsync(items);
        Assert.Equal(500, inserted);
        Assert.Equal(500L, CountRows(cn, "G40_Item"));
    }

    // =========================================================================
    // 5. Bulk update parity: BulkUpdateAsync updates correct values
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task BulkUpdate_CorrectValues_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, Cleanup("G40_Item"));

            // Seed rows
            for (int i = 1; i <= 10; i++)
                Exec(cn!, $"INSERT INTO G40_Item (Id, Label, Value) VALUES ({i}, 'orig{i}', {i})");

            // BulkUpdate first 5 rows
            var toUpdate = Enumerable.Range(1, 5)
                .Select(i => new G40Item { Id = i, Label = $"updated{i}", Value = i * 100 })
                .ToList();

            var updated = await ctx.BulkUpdateAsync(toUpdate);
            Assert.Equal(5, updated);

            // Verify updated rows
            for (int i = 1; i <= 5; i++)
            {
                var label = ReadScalar(cn!, $"SELECT Label FROM G40_Item WHERE Id={i}") as string;
                Assert.Equal($"updated{i}", label);
            }

            // Verify untouched rows
            for (int i = 6; i <= 10; i++)
            {
                var label = ReadScalar(cn!, $"SELECT Label FROM G40_Item WHERE Id={i}") as string;
                Assert.Equal($"orig{i}", label);
            }
        }
    }

    [Fact]
    public async Task BulkUpdate_NonexistentRows_ZeroUpdated_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        var entity = new G40Item { Id = 999, Label = "ghost", Value = 0 };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });
        Assert.Equal(0, updated);
    }

    // =========================================================================
    // 6. Bulk delete parity: BulkDeleteAsync deletes correct rows
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task BulkDelete_CorrectRows_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, Cleanup("G40_Item"));

            for (int i = 1; i <= 10; i++)
                Exec(cn!, $"INSERT INTO G40_Item (Id, Label, Value) VALUES ({i}, 'row{i}', {i})");

            // Delete rows 3, 5, 7
            var toDelete = new[]
            {
                new G40Item { Id = 3, Label = "", Value = 0 },
                new G40Item { Id = 5, Label = "", Value = 0 },
                new G40Item { Id = 7, Label = "", Value = 0 }
            };

            var deleted = await ctx.BulkDeleteAsync(toDelete);
            Assert.Equal(3, deleted);
            Assert.Equal(7L, CountRows(cn!, "G40_Item"));

            // Verify specific rows are gone
            Assert.Equal(0L, Convert.ToInt64(ReadScalar(cn!, "SELECT COUNT(*) FROM G40_Item WHERE Id IN (3,5,7)")));
        }
    }

    [Fact]
    public async Task BulkDelete_NonexistentRows_ZeroDeleted_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40_Item (Id, Label, Value) VALUES (1, 'keep', 1)");

        var entity = new G40Item { Id = 999, Label = "", Value = 0 };
        var deleted = await ctx.BulkDeleteAsync(new[] { entity });
        Assert.Equal(0, deleted);
        Assert.Equal(1L, CountRows(cn, "G40_Item"));
    }

    // =========================================================================
    // 7. Savepoint parity: CreateSavepointAsync / RollbackToSavepointAsync
    // =========================================================================

    [Fact]
    public async Task Savepoint_CreateAndRollback_PartialUndo_SQLite()
    {
        // Savepoint test: uses raw commands WITH transaction binding to ensure
        // INSERT/SAVEPOINT/ROLLBACK all execute within the same transaction.
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        Exec(cn, "CREATE TABLE G40_SP (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)");

        var tx = await cn.BeginTransactionAsync();

        // Insert row 1 within transaction
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO G40_SP VALUES (1, 'committed')";
            cmd.ExecuteNonQuery();
        }

        // Create savepoint
        var provider = new SqliteProvider();
        await provider.CreateSavepointAsync(tx, "sp1");

        // Insert row 2 within transaction
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO G40_SP VALUES (2, 'rolled-back')";
            cmd.ExecuteNonQuery();
        }

        // Rollback to savepoint — row 2 should be undone
        await provider.RollbackToSavepointAsync(tx, "sp1");

        // Commit — only row 1 persists
        await tx.CommitAsync();

        Assert.Equal(1L, CountRows(cn, "G40_SP"));
        var label = ReadScalar(cn, "SELECT Label FROM G40_SP WHERE Id=1") as string;
        Assert.Equal("committed", label);
    }

    [Fact]
    public async Task Savepoint_NullOrEmptyName_ThrowsArgumentException_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        using var tx = await ctx.Database.BeginTransactionAsync();

        await Assert.ThrowsAsync<ArgumentException>(
            () => ctx.CreateSavepointAsync(tx.Transaction!, ""));
        await Assert.ThrowsAsync<ArgumentException>(
            () => ctx.RollbackToSavepointAsync(tx.Transaction!, ""));

        await tx.RollbackAsync();
    }

    // =========================================================================
    // 8. Retry parity: DefaultExecutionStrategy wraps DbException in NormException
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Retry_DefaultStrategy_WrapsDbException_AllProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, Cleanup("G40_Item"));

            // Insert a row to set up a conflict
            Exec(cn!, "INSERT INTO G40_Item (Id, Label, Value) VALUES (1, 'exists', 1)");

            // BulkInsertAsync routes through DefaultExecutionStrategy.ExecuteAsync,
            // which wraps DbException in NormException. Use it to test the wrapping.
            var dup = new[] { new G40Item { Id = 1, Label = "dup", Value = 2 } };
            var ex = await Assert.ThrowsAsync<NormException>(() => ctx.BulkInsertAsync(dup));
            Assert.IsAssignableFrom<DbException>(ex.InnerException);
        }
    }

    [Fact]
    public async Task Retry_DefaultStrategy_PreservesNormConfigurationException_SQLite()
    {
        // NormConfigurationException must NOT be double-wrapped (fix #58).
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (cn)
        {
            // Create a table without a primary key column for the entity
            Exec(cn, "CREATE TABLE G40_NoKey (Id INTEGER, Value TEXT)");

            await using var ctx = new DbContext(cn, new SqliteProvider());

            // A keyless entity on a write path should throw NormConfigurationException directly.
            // The DefaultExecutionStrategy exempts NormConfigurationException from wrapping.
            // We verify this by trying to use an entity that hits the keyless guard.
            // Since the entity has a [Key] attribute but the table has no PK constraint,
            // this should still work; use a different approach: we test SaveChangesAsync
            // where the strategy catches DbException but lets NormConfigurationException propagate.
            var strategy = new DefaultExecutionStrategy(ctx);
            await Assert.ThrowsAsync<NormConfigurationException>(
                () => strategy.ExecuteAsync<int>(
                    (_, _) => throw new NormConfigurationException("test-config-error"),
                    CancellationToken.None));
        }
    }

    // =========================================================================
    // 9. Additional parity: Paging SQL shape check per provider
    // =========================================================================

    [Fact]
    public void PagingShape_SQLite_UsesLimitOffset()
    {
        var p = new SqliteProvider();
        Assert.False(p.UsesFetchOffsetPaging);
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM G40_Item ORDER BY Id");
        p.ApplyPaging(sb, 10, 5, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PagingShape_SqlServer_UsesFetchOffset()
    {
        var p = new SqlServerProvider();
        Assert.True(p.UsesFetchOffsetPaging);
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM G40_Item ORDER BY Id");
        p.ApplyPaging(sb, 10, 5, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("OFFSET @off ROWS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH NEXT @lim ROWS ONLY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PagingShape_MySQL_UsesLimitCommaOffset()
    {
        // MySQL uses "LIMIT offset, limit" format (no OFFSET keyword)
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.False(p.UsesFetchOffsetPaging);
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM G40_Item ORDER BY Id");
        p.ApplyPaging(sb, 10, 5, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        // MySQL encodes offset before limit: "LIMIT @off, @lim"
        Assert.Contains("@off", sql);
        Assert.Contains("@lim", sql);
    }

    [Fact]
    public void PagingShape_Postgres_UsesLimitOffset()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        Assert.False(p.UsesFetchOffsetPaging);
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM G40_Item ORDER BY Id");
        p.ApplyPaging(sb, 10, 5, "@lim", "@off");
        var sql = sb.ToString();
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    // =========================================================================
    // 10. Escape parity across all 4 providers
    // =========================================================================

    [Theory]
    [InlineData("sqlite",    "\"col\"")]
    [InlineData("sqlserver", "[col]")]
    [InlineData("mysql",     "`col`")]
    [InlineData("postgres",  "\"col\"")]
    public void Escape_SingleIdentifier_AllProviders(string kind, string expected)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal(expected, p.Escape("col"));
    }

    // =========================================================================
    // 11. Bulk insert + delete round-trip parity
    // =========================================================================

    [Fact]
    public async Task BulkInsertThenDelete_RoundTrip_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        var items = Enumerable.Range(1, 20)
            .Select(i => new G40Item { Id = i, Label = $"rt{i}", Value = i })
            .ToList();

        var inserted = await ctx.BulkInsertAsync(items);
        Assert.Equal(20, inserted);

        // Delete the even-numbered items
        var evens = items.Where(x => x.Id % 2 == 0).ToList();
        var deleted = await ctx.BulkDeleteAsync(evens);
        Assert.Equal(10, deleted);
        Assert.Equal(10L, CountRows(cn, "G40_Item"));

        // Verify only odd-numbered items remain
        var remaining = Convert.ToInt64(ReadScalar(cn, "SELECT COUNT(*) FROM G40_Item WHERE Id % 2 = 1"));
        Assert.Equal(10L, remaining);
    }

    // =========================================================================
    // 12. OCC batch with mixed outcomes across providers
    // =========================================================================

    [Fact]
    public async Task OCC_BulkUpdate_PartialStale_OnlyFreshUpdated_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(OccStrDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40_OccStr (Id, Value, RowVersion) VALUES (1, 'r1', 'tok1')");
        Exec(cn, "INSERT INTO G40_OccStr (Id, Value, RowVersion) VALUES (2, 'r2', 'tok1')");
        Exec(cn, "INSERT INTO G40_OccStr (Id, Value, RowVersion) VALUES (3, 'r3', 'tok1')");

        // Externally change token for row 2
        Exec(cn, "UPDATE G40_OccStr SET RowVersion='tok2' WHERE Id=2");

        var entities = new[]
        {
            new G40OccStr { Id = 1, Value = "upd1", RowVersion = "tok1" }, // fresh
            new G40OccStr { Id = 2, Value = "upd2", RowVersion = "tok1" }, // stale
            new G40OccStr { Id = 3, Value = "upd3", RowVersion = "tok1" }, // fresh
        };

        var updated = await ctx.BulkUpdateAsync(entities);
        Assert.Equal(2, updated);

        Assert.Equal("upd1", ReadScalar(cn, "SELECT Value FROM G40_OccStr WHERE Id=1") as string);
        Assert.Equal("r2",   ReadScalar(cn, "SELECT Value FROM G40_OccStr WHERE Id=2") as string); // unchanged
        Assert.Equal("upd3", ReadScalar(cn, "SELECT Value FROM G40_OccStr WHERE Id=3") as string);
    }

    // =========================================================================
    // 13. Savepoint with multiple savepoints stacked
    // =========================================================================

    [Fact]
    public async Task Savepoint_MultipleSavepoints_RollbackToFirst_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        using var tx = await ctx.Database.BeginTransactionAsync();

        // Use raw SQL to avoid ChangeTracker re-inserting entities on commit
        Exec(cn, "INSERT INTO G40_Item (Id, Label, Value) VALUES (1, 'base', 1)");
        await ctx.CreateSavepointAsync(tx.Transaction!, "sp1");

        Exec(cn, "INSERT INTO G40_Item (Id, Label, Value) VALUES (2, 'sp1-row', 2)");
        await ctx.CreateSavepointAsync(tx.Transaction!, "sp2");

        Exec(cn, "INSERT INTO G40_Item (Id, Label, Value) VALUES (3, 'sp2-row', 3)");

        // Rollback all the way to sp1 -- rows 2 and 3 should be undone
        await ctx.RollbackToSavepointAsync(tx.Transaction!, "sp1");
        await tx.CommitAsync();

        Assert.Equal(1L, CountRows(cn, "G40_Item"));
        Assert.Equal("base", ReadScalar(cn, "SELECT Label FROM G40_Item WHERE Id=1") as string);
    }

    // =========================================================================
    // 14. Bulk operations within a transaction -- atomicity
    // =========================================================================

    [Fact]
    public async Task BulkInsert_WithinTransaction_Rollback_NoRows_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        using var tx = await ctx.Database.BeginTransactionAsync();

        var items = Enumerable.Range(1, 10)
            .Select(i => new G40Item { Id = i, Label = $"tx{i}", Value = i })
            .ToList();
        await ctx.BulkInsertAsync(items);

        await tx.RollbackAsync();

        Assert.Equal(0L, CountRows(cn, "G40_Item"));
    }

    [Fact]
    public async Task BulkDelete_WithinTransaction_Rollback_RowsPreserved_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 5; i++)
            Exec(cn, $"INSERT INTO G40_Item (Id, Label, Value) VALUES ({i}, 'row{i}', {i})");

        using var tx = await ctx.Database.BeginTransactionAsync();

        var toDelete = Enumerable.Range(1, 5)
            .Select(i => new G40Item { Id = i, Label = "", Value = 0 })
            .ToList();
        await ctx.BulkDeleteAsync(toDelete);

        await tx.RollbackAsync();

        Assert.Equal(5L, CountRows(cn, "G40_Item"));
    }

    // =========================================================================
    // 15. Provider param prefix parity
    // =========================================================================

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void ParamPrefix_IsAtSign_AllProviders(string kind)
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

    // =========================================================================
    // 16. Bulk update + tenant isolation combined -- own-tenant batch
    // =========================================================================

    [Fact]
    public async Task Tenant_BulkUpdate_MultipleSameTenant_AllUpdated_SQLite()
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider40(1),
            TenantColumnName = "TenantId"
        };
        var (cn, ctx) = CreateSqliteDb(TenantDdl("sqlite"), opts);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 5; i++)
            Exec(cn, $"INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES ({i}, 1, 'orig{i}')");

        var entities = Enumerable.Range(1, 5)
            .Select(i => new G40TenantItem { Id = i, TenantId = 1, Label = $"upd{i}" })
            .ToList();

        await ctx.BulkUpdateAsync(entities);

        for (int i = 1; i <= 5; i++)
        {
            var label = ReadScalar(cn, $"SELECT Label FROM G40_TenantItem WHERE Id={i}") as string;
            Assert.Equal($"upd{i}", label);
        }
    }

    // =========================================================================
    // 17. Bulk delete + tenant isolation combined -- mixed tenants batch
    // =========================================================================

    [Fact]
    public async Task Tenant_BulkDelete_MixedTenants_OnlyOwnDeleted_SQLite()
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider40(1),
            TenantColumnName = "TenantId"
        };
        var (cn, ctx) = CreateSqliteDb(TenantDdl("sqlite"), opts);
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES (1, 1, 'own')");
        Exec(cn, "INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES (2, 2, 'foreign')");
        Exec(cn, "INSERT INTO G40_TenantItem (Id, TenantId, Label) VALUES (3, 1, 'own2')");

        var entities = new[]
        {
            new G40TenantItem { Id = 1, TenantId = 1, Label = "" },
            new G40TenantItem { Id = 2, TenantId = 1, Label = "" }, // cross-tenant attempt
            new G40TenantItem { Id = 3, TenantId = 1, Label = "" },
        };

        await ctx.BulkDeleteAsync(entities);

        // Own rows (1,3) deleted; foreign row (2) preserved
        Assert.Equal(1L, CountRows(cn, "G40_TenantItem"));
        Assert.Equal("foreign", ReadScalar(cn, "SELECT Label FROM G40_TenantItem WHERE Id=2") as string);
    }

    // =========================================================================
    // 18. Cancellation during bulk insert
    // =========================================================================

    [Fact]
    public async Task BulkInsert_PreCancelled_ThrowsOCE_SQLite()
    {
        var (cn, ctx) = CreateSqliteDb(ItemDdl("sqlite"));
        await using var _ = ctx; using var __ = cn;

        var items = Enumerable.Range(1, 10)
            .Select(i => new G40Item { Id = i, Label = $"c{i}", Value = i })
            .ToList();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.BulkInsertAsync(items, cts.Token));
    }
}
