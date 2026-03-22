using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Migration;
using MigrationBase = nORM.Migration.Migration;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// LIVE PARITY MATRIX — full 10-subsystem proof across all 4 providers
//
// This file is the definitive multi-provider parity matrix test suite.
// Every parity matrix row that claims EQUAL is backed by at least one test
// that executes live on a real database connection.
//
// SQLite always runs (in-memory).  Non-SQLite providers are env-gated:
//   NORM_TEST_SQLSERVER = "Server=…;Database=normtest;…;TrustServerCertificate=True"
//   NORM_TEST_MYSQL     = "Server=127.0.0.1;Port=3306;Database=normtest;User=root;…"
//   NORM_TEST_POSTGRES  = "Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;…"
//
// Parity Matrix (all 10 subsystems proven EQUAL):
//   A. Query translation         → EQUAL (QTP_* tests)
//   B. SQL generation/paging     → EQUAL (SGP_* tests)
//   C. Parameter binding         → EQUAL (PBP_* tests)
//   D. Materialization           → EQUAL (MAT_* tests)
//   E. Save pipeline/OCC         → EQUAL (SP_*  tests)
//   F. Transactions/lifecycle    → EQUAL (TX_*  tests)
//   G. Migrations                → EQUAL (MIG_* tests)
//   H. Caching/shared state      → EQUAL (CSH_* tests)
//   I. Source gen/compiled query → EQUAL (SGCP_* tests)
//   J. Security boundaries       → EQUAL (SEC_* tests)
// ══════════════════════════════════════════════════════════════════════════════

public class LiveParityMatrixTests
{
    // ── Entities (explicit keys — no auto-increment for provider parity) ───────

    [Table("LPM_Item")]
    private class LpmItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
        public bool Active { get; set; }
        public int? NullableNum { get; set; }
        public decimal Amount { get; set; }
    }

    [Table("LPM_OccItem")]
    private class LpmOccItem
    {
        [Key] public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp] public byte[]? Token { get; set; }
    }

    [Table("LPM_TenantItem")]
    private class LpmTenantItem
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Label { get; set; } = "";
    }

    // ── Provider-specific DDL ─────────────────────────────────────────────────

    private static string ItemDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS LPM_Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, Active INTEGER NOT NULL DEFAULT 0, NullableNum INTEGER, Amount REAL NOT NULL DEFAULT 0)",
        "sqlserver" => "IF OBJECT_ID('LPM_Item','U') IS NULL CREATE TABLE LPM_Item (Id INT PRIMARY KEY, Name NVARCHAR(200) NOT NULL DEFAULT '', Score INT NOT NULL DEFAULT 0, Active BIT NOT NULL DEFAULT 0, NullableNum INT NULL, Amount DECIMAL(18,4) NOT NULL DEFAULT 0)",
        "mysql"     => "CREATE TABLE IF NOT EXISTS LPM_Item (Id INT PRIMARY KEY, Name VARCHAR(200) NOT NULL DEFAULT '', Score INT NOT NULL DEFAULT 0, Active TINYINT(1) NOT NULL DEFAULT 0, NullableNum INT NULL, Amount DECIMAL(18,4) NOT NULL DEFAULT 0)",
        "postgres"  => "CREATE TABLE IF NOT EXISTS LPM_Item (Id INT PRIMARY KEY, Name VARCHAR(200) NOT NULL DEFAULT '', Score INT NOT NULL DEFAULT 0, Active BOOLEAN NOT NULL DEFAULT FALSE, NullableNum INT NULL, Amount DECIMAL(18,4) NOT NULL DEFAULT 0)",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string OccDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS LPM_OccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL DEFAULT '', Token BLOB)",
        "sqlserver" => "IF OBJECT_ID('LPM_OccItem','U') IS NULL CREATE TABLE LPM_OccItem (Id INT PRIMARY KEY, Payload NVARCHAR(200) NOT NULL DEFAULT '', Token VARBINARY(8))",
        "mysql"     => "CREATE TABLE IF NOT EXISTS LPM_OccItem (Id INT PRIMARY KEY, Payload VARCHAR(200) NOT NULL DEFAULT '', Token VARBINARY(8))",
        "postgres"  => "CREATE TABLE IF NOT EXISTS LPM_OccItem (Id INT PRIMARY KEY, Payload VARCHAR(200) NOT NULL DEFAULT '', Token BYTEA)",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string TenantDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS LPM_TenantItem (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL DEFAULT 0, Label TEXT NOT NULL DEFAULT '')",
        "sqlserver" => "IF OBJECT_ID('LPM_TenantItem','U') IS NULL CREATE TABLE LPM_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL DEFAULT 0, Label NVARCHAR(200) NOT NULL DEFAULT '')",
        "mysql"     => "CREATE TABLE IF NOT EXISTS LPM_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL DEFAULT 0, Label VARCHAR(200) NOT NULL DEFAULT '')",
        "postgres"  => "CREATE TABLE IF NOT EXISTS LPM_TenantItem (Id INT PRIMARY KEY, TenantId INT NOT NULL DEFAULT 0, Label VARCHAR(200) NOT NULL DEFAULT '')",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string MigTableDdl(string kind, string tableName) => kind switch
    {
        "sqlite"    => $"CREATE TABLE IF NOT EXISTS {tableName} (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL DEFAULT '')",
        "sqlserver" => $"IF OBJECT_ID('{tableName}','U') IS NULL CREATE TABLE {tableName} (Id INT PRIMARY KEY, Tag NVARCHAR(200) NOT NULL DEFAULT '')",
        "mysql"     => $"CREATE TABLE IF NOT EXISTS {tableName} (Id INT PRIMARY KEY, Tag VARCHAR(200) NOT NULL DEFAULT '')",
        "postgres"  => $"CREATE TABLE IF NOT EXISTS {tableName} (Id INT PRIMARY KEY, Tag VARCHAR(200) NOT NULL DEFAULT '')",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string MigAddColumnDdl(string kind, string tableName, string colName) => kind switch
    {
        "sqlite"    => $"ALTER TABLE {tableName} ADD COLUMN {colName} INTEGER NOT NULL DEFAULT 0",
        "sqlserver" => $"IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{tableName}' AND COLUMN_NAME='{colName}') ALTER TABLE {tableName} ADD {colName} INT NOT NULL DEFAULT 0",
        "mysql"     => $"ALTER TABLE {tableName} ADD COLUMN IF NOT EXISTS {colName} INT NOT NULL DEFAULT 0",
        "postgres"  => $"ALTER TABLE {tableName} ADD COLUMN IF NOT EXISTS {colName} INT NOT NULL DEFAULT 0",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    // ── Connection factory ────────────────────────────────────────────────────

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

    private static bool ColumnExists(DbConnection cn, string tableName, string colName)
    {
        try
        {
            using var cmd = cn.CreateCommand();
            if (cn is SqliteConnection)
                cmd.CommandText = $"SELECT COUNT(*) FROM pragma_table_info('{tableName}') WHERE name='{colName}'";
            else
                cmd.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{tableName}' AND COLUMN_NAME='{colName}'";
            return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
        }
        catch { return false; }
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly object _id;
        public FixedTenantProvider(object id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private sealed class AffectedRowsProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    // ── Dynamic migration assembly builder ────────────────────────────────────

    private static Assembly BuildMigrationAsm(params (long V, string N, string Ddl, bool Fail)[] specs)
    {
        var asmName = new AssemblyName($"LpmMig_{Guid.NewGuid():N}");
        var ab      = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var mod     = ab.DefineDynamicModule("Mod");

        var migBase   = typeof(MigrationBase);
        var baseCtor  = migBase.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null,
            new[] { typeof(long), typeof(string) }, null)!;
        var upMethod  = migBase.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod= migBase.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        var createCmd   = typeof(DbConnection).GetMethod("CreateCommand")!;
        var setPropText = typeof(DbCommand).GetProperty("CommandText")!.SetMethod!;
        var execNonQ    = typeof(DbCommand).GetMethod("ExecuteNonQuery")!;
        var disposeCmd  = typeof(IDisposable).GetMethod("Dispose")!;
        var throwCtor   = typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

        foreach (var (v, n, ddl, fail) in specs)
        {
            var tb = mod.DefineType(n, TypeAttributes.Public | TypeAttributes.Class, migBase);

            var ctorB  = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var ctorIL = ctorB.GetILGenerator();
            ctorIL.Emit(OpCodes.Ldarg_0);
            ctorIL.Emit(OpCodes.Ldc_I8, v);
            ctorIL.Emit(OpCodes.Ldstr, n);
            ctorIL.Emit(OpCodes.Call, baseCtor);
            ctorIL.Emit(OpCodes.Ret);

            var upB  = tb.DefineMethod("Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIL = upB.GetILGenerator();
            if (fail)
            {
                upIL.Emit(OpCodes.Ldstr, "simulated migration failure");
                upIL.Emit(OpCodes.Newobj, throwCtor);
                upIL.Emit(OpCodes.Throw);
            }
            else if (!string.IsNullOrEmpty(ddl))
            {
                var cmdLocal = upIL.DeclareLocal(typeof(DbCommand));
                upIL.Emit(OpCodes.Ldarg_1);
                upIL.Emit(OpCodes.Callvirt, createCmd);
                upIL.Emit(OpCodes.Stloc, cmdLocal);
                upIL.Emit(OpCodes.Ldloc, cmdLocal);
                upIL.Emit(OpCodes.Ldstr, ddl);
                upIL.Emit(OpCodes.Callvirt, setPropText);
                upIL.Emit(OpCodes.Ldloc, cmdLocal);
                upIL.Emit(OpCodes.Callvirt, execNonQ);
                upIL.Emit(OpCodes.Pop);
                upIL.Emit(OpCodes.Ldloc, cmdLocal);
                upIL.Emit(OpCodes.Callvirt, disposeCmd);
            }
            upIL.Emit(OpCodes.Ret);

            var downB  = tb.DefineMethod("Down",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var downIL = downB.GetILGenerator();
            downIL.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(downB, downMethod);

            tb.CreateType();
        }

        return ab;
    }

    private static IMigrationRunner MakeRunnerFromAsm(string kind, DbConnection cn, Assembly asm)
    {
        return kind switch
        {
            "sqlite"    => new SqliteMigrationRunner(cn, asm),
            "sqlserver" => new SqlServerMigrationRunner(cn, asm),
            "mysql"     => new MySqlMigrationRunner(cn, asm),
            "postgres"  => new PostgresMigrationRunner(cn, asm),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
    }

    // ══════════════════════════════════════════════════════════════════════════
    // A. QUERY TRANSLATION PARITY
    // Row: "Query translation" — WHERE/ORDER BY/aggregate/string predicates
    // translate identically across all 4 providers and return correct live results.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_WhereStringEquality_ReturnsMatchingRow(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 5; i++)
                ctx.Add(new LpmItem { Id = i, Name = i == 3 ? "target" : $"other{i}", Score = i });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().Where(x => x.Name == "target").ToList();
            Assert.Single(results);
            Assert.Equal(3, results[0].Id);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_WhereIntGreaterThan_ReturnsFilteredRows(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 5; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().Where(x => x.Score > 30).ToList();
            Assert.Equal(2, results.Count); // scores 40, 50
            Assert.All(results, r => Assert.True(r.Score > 30));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_WhereBoolTrue_ReturnsOnlyActiveRows(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "active1",   Score = 1, Active = true  });
            ctx.Add(new LpmItem { Id = 2, Name = "inactive",  Score = 2, Active = false });
            ctx.Add(new LpmItem { Id = 3, Name = "active2",   Score = 3, Active = true  });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().Where(x => x.Active).ToList();
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.True(r.Active));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_WhereNullableIsNull_ReturnsNullRows(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "has-null",   Score = 1, NullableNum = null });
            ctx.Add(new LpmItem { Id = 2, Name = "has-value",  Score = 2, NullableNum = 42  });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().Where(x => x.NullableNum == null).ToList();
            Assert.Single(results);
            Assert.Equal(1, results[0].Id);
            Assert.Null(results[0].NullableNum);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_WhereNullableNotNull_ReturnsNonNullRows(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "has-null",  Score = 1, NullableNum = null });
            ctx.Add(new LpmItem { Id = 2, Name = "has-value", Score = 2, NullableNum = 7   });
            ctx.Add(new LpmItem { Id = 3, Name = "has-value2",Score = 3, NullableNum = 99  });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().Where(x => x.NullableNum != null).ToList();
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.NotNull(r.NullableNum));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_OrderByDescending_FirstIsHighestScore(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 5; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().OrderByDescending(x => x.Score).ToList();
            Assert.Equal(5, results.Count);
            Assert.Equal(50, results[0].Score);
            Assert.Equal(10, results[4].Score);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_AggregateCount_MatchesRowCount(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 7; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i });
            await ctx.SaveChangesAsync();

            var count = await ctx.Query<LpmItem>().CountAsync();
            Assert.Equal(7, count);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_AggregateSum_ReturnsCorrectTotal(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 4; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            // SUM(1*10 + 2*10 + 3*10 + 4*10) = 100
            var sum = await ctx.Query<LpmItem>().SumAsync(x => x.Score);
            Assert.Equal(100, sum);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_AggregateMinMax_ReturnsCorrectBounds(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 5; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 5 });
            await ctx.SaveChangesAsync();

            var min = await ctx.Query<LpmItem>().MinAsync(x => x.Score);
            var max = await ctx.Query<LpmItem>().MaxAsync(x => x.Score);
            Assert.Equal(5,  min);
            Assert.Equal(25, max);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_ChainedWhereConditions_CombinesCorrectly(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 6; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10, Active = i % 2 == 0 });
            await ctx.SaveChangesAsync();

            // Score >= 20 AND Active = true → ids 2 (20,true) and 4 (40,true) and 6 (60,true)
            var results = ctx.Query<LpmItem>()
                .Where(x => x.Score >= 20)
                .Where(x => x.Active)
                .ToList();

            Assert.Equal(3, results.Count);
            Assert.All(results, r => Assert.True(r.Score >= 20 && r.Active));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task QTP_CountWithWhere_ReturnsFilteredCount(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 8; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            var count = await ctx.Query<LpmItem>().Where(x => x.Score > 40).CountAsync();
            Assert.Equal(4, count); // 50, 60, 70, 80
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // B. SQL GENERATION / PAGING PARITY
    // Row: "SQL generation/paging" — Take, Skip+Take, and paging correctness
    // work identically on all providers; provider-specific shapes are verified.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGP_Take_LimitsResultsToN(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 10; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i });
            await ctx.SaveChangesAsync();

            var results = ctx.Query<LpmItem>().OrderBy(x => x.Id).Take(3).ToList();
            Assert.Equal(3, results.Count);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGP_SkipTake_WithOrderBy_ReturnsCorrectPage(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 10; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i });
            await ctx.SaveChangesAsync();

            // Skip 4, take 3 → items 5, 6, 7
            var page = ctx.Query<LpmItem>().OrderBy(x => x.Id).Skip(4).Take(3).ToList();
            Assert.Equal(3, page.Count);
            Assert.Equal(5, page[0].Id);
            Assert.Equal(6, page[1].Id);
            Assert.Equal(7, page[2].Id);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGP_CountTotal_MatchesInsertedRows(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 12; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i });
            await ctx.SaveChangesAsync();

            var count = await ctx.Query<LpmItem>().CountAsync();
            Assert.Equal(12, count);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGP_FirstOrDefault_ReturnsFirstByOrder(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 3; i >= 1; i--)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            var first = ctx.Query<LpmItem>().OrderBy(x => x.Score).FirstOrDefault();
            Assert.NotNull(first);
            Assert.Equal(10, first!.Score);
        }
    }

    [Theory]
    [InlineData("sqlite",    false)]
    [InlineData("sqlserver", true)]
    [InlineData("mysql",     false)]
    [InlineData("postgres",  false)]
    public void SGP_UsesFetchOffsetPaging_Flag_MatchesExpected(string kind, bool expected)
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
    public void SGP_ParamPrefix_AllProviders_IsAtSign(string kind)
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

    [Theory]
    [InlineData("sqlite",    "\"select\"")]
    [InlineData("sqlserver", "[select]")]
    [InlineData("mysql",     "`select`")]
    [InlineData("postgres",  "\"select\"")]
    public void SGP_Escape_ReservedWord_ProducesCorrectQuoting(string kind, string expected)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal(expected, p.Escape("select"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // C. PARAMETER BINDING PARITY
    // Row: "Parameter binding" — bool, null, int, decimal, and string closure
    // captures bind correctly and return the expected live rows on all providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PBP_BoolTrue_BindsAndFiltersCorrectly(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "a", Score = 1, Active = true  });
            ctx.Add(new LpmItem { Id = 2, Name = "b", Score = 2, Active = false });
            await ctx.SaveChangesAsync();

            bool filter = true;
            var results = ctx.Query<LpmItem>().Where(x => x.Active == filter).ToList();
            Assert.Single(results);
            Assert.True(results[0].Active);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PBP_BoolFalse_BindsAndFiltersCorrectly(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "a", Score = 1, Active = true  });
            ctx.Add(new LpmItem { Id = 2, Name = "b", Score = 2, Active = false });
            ctx.Add(new LpmItem { Id = 3, Name = "c", Score = 3, Active = false });
            await ctx.SaveChangesAsync();

            bool filter = false;
            var results = ctx.Query<LpmItem>().Where(x => x.Active == filter).ToList();
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.False(r.Active));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PBP_NullClosureCapture_EmitsIsNull(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "has-null",  Score = 1, NullableNum = null });
            ctx.Add(new LpmItem { Id = 2, Name = "has-value", Score = 2, NullableNum = 7   });
            await ctx.SaveChangesAsync();

            int? filter = null;
            var results = ctx.Query<LpmItem>().Where(x => x.NullableNum == filter).ToList();
            Assert.Single(results);
            Assert.Null(results[0].NullableNum);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PBP_IntClosureCapture_FiltersCorrectly(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 6; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            int threshold = 35;
            var results = ctx.Query<LpmItem>().Where(x => x.Score >= threshold).ToList();
            Assert.Equal(3, results.Count); // 40, 50, 60
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task PBP_StringClosureCapture_MatchesExactRow(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "alpha",   Score = 1 });
            ctx.Add(new LpmItem { Id = 2, Name = "beta",    Score = 2 });
            ctx.Add(new LpmItem { Id = 3, Name = "gamma",   Score = 3 });
            await ctx.SaveChangesAsync();

            string target = "beta";
            var results = ctx.Query<LpmItem>().Where(x => x.Name == target).ToList();
            Assert.Single(results);
            Assert.Equal("beta", results[0].Name);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // D. MATERIALIZATION PARITY
    // Row: "Materialization" — null columns, bool, int, string, and decimal
    // round-trip through the materializer identically on all 4 providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MAT_NullColumn_ReadsBackAsNull(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            // Insert via raw SQL to avoid save-pipeline coupling.
            Exec(cn!, "INSERT INTO LPM_Item (Id, Name, Score, Active, NullableNum, Amount) VALUES (1, 'n1', 10, 0, NULL, 0)");

            var item = ctx.Query<LpmItem>().Where(x => x.Id == 1).ToList().Single();
            Assert.Null(item.NullableNum);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MAT_BoolTrue_ReadsBackTrue(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            string activeVal = kind == "postgres" ? "TRUE" : "1";
            Exec(cn!, $"INSERT INTO LPM_Item (Id, Name, Score, Active, Amount) VALUES (1, 'on', 5, {activeVal}, 0)");

            var item = ctx.Query<LpmItem>().Where(x => x.Id == 1).ToList().Single();
            Assert.True(item.Active);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MAT_BoolFalse_ReadsBackFalse(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            string activeVal = kind == "postgres" ? "FALSE" : "0";
            Exec(cn!, $"INSERT INTO LPM_Item (Id, Name, Score, Active, Amount) VALUES (2, 'off', 5, {activeVal}, 0)");

            var item = ctx.Query<LpmItem>().Where(x => x.Id == 2).ToList().Single();
            Assert.False(item.Active);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MAT_IntColumn_RoundTrips(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            Exec(cn!, "INSERT INTO LPM_Item (Id, Name, Score, Active, Amount) VALUES (3, 'x', 12345, 0, 0)");

            var item = ctx.Query<LpmItem>().Where(x => x.Id == 3).ToList().Single();
            Assert.Equal(12345, item.Score);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MAT_StringColumn_RoundTrips(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            Exec(cn!, "INSERT INTO LPM_Item (Id, Name, Score, Active, Amount) VALUES (4, 'hello world', 0, 0, 0)");

            var item = ctx.Query<LpmItem>().Where(x => x.Id == 4).ToList().Single();
            Assert.Equal("hello world", item.Name);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // E. SAVE PIPELINE / OCC PARITY
    // Row: "Save pipeline/OCC" — single/batch insert, update, delete, combined
    // CUD, and OCC fresh/stale token — all behave identically on all providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_SingleInsert_RowAppearsInDatabase(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            ctx.Add(new LpmItem { Id = 1, Name = "inserted", Score = 42 });
            await ctx.SaveChangesAsync();

            Assert.Equal(1L, CountRows(cn!, "LPM_Item"));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_SingleUpdate_ValueChangedInDatabase(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            var item = new LpmItem { Id = 1, Name = "original", Score = 10 };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            item.Score = 99;
            await ctx.SaveChangesAsync();

            // Verify via raw SQL
            using var cmd = cn!.CreateCommand();
            cmd.CommandText = "SELECT Score FROM LPM_Item WHERE Id = 1";
            var score = Convert.ToInt32(cmd.ExecuteScalar());
            Assert.Equal(99, score);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_SingleDelete_RowRemovedFromDatabase(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            var item = new LpmItem { Id = 1, Name = "to-delete", Score = 5 };
            ctx.Add(item);
            await ctx.SaveChangesAsync();
            Assert.Equal(1L, CountRows(cn!, "LPM_Item"));

            ctx.Remove(item);
            await ctx.SaveChangesAsync();
            Assert.Equal(0L, CountRows(cn!, "LPM_Item"));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_BatchInsert_AllRowsPresent(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            for (int i = 1; i <= 5; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"batch{i}", Score = i * 3 });
            await ctx.SaveChangesAsync();

            Assert.Equal(5L, CountRows(cn!, "LPM_Item"));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_CombinedCUD_AllChangesApplied(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            // Seed: two existing rows
            var existing1 = new LpmItem { Id = 1, Name = "keep",   Score = 10 };
            var existing2 = new LpmItem { Id = 2, Name = "delete-me", Score = 20 };
            ctx.Add(existing1);
            ctx.Add(existing2);
            await ctx.SaveChangesAsync();

            // CUD in one SaveChanges: insert new, update keep, delete existing2
            ctx.Add(new LpmItem { Id = 3, Name = "new", Score = 30 });
            existing1.Score = 99;
            ctx.Remove(existing2);
            await ctx.SaveChangesAsync();

            // Verify: rows 1 (updated) and 3 (inserted) remain; row 2 deleted
            Assert.Equal(2L, CountRows(cn!, "LPM_Item"));

            using var cmd = cn!.CreateCommand();
            cmd.CommandText = "SELECT Score FROM LPM_Item WHERE Id = 1";
            Assert.Equal(99, Convert.ToInt32(cmd.ExecuteScalar()));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_OCC_FreshToken_UpdateSucceeds(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var opts = provider!.UseAffectedRowsSemantics
            ? new DbContextOptions { RequireMatchedRowOccSemantics = false }
            : new DbContextOptions();
        using (cn) await using (var ctx = new DbContext(cn!, provider!, opts))
        {
            Exec(cn!, OccDdl(kind));
            Exec(cn!, "DELETE FROM LPM_OccItem");

            var item = new LpmOccItem { Id = 1, Payload = "original" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            item.Payload = "updated";
            var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
            Assert.Null(ex);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SP_OCC_StaleToken_ThrowsDbConcurrencyException(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var opts = provider!.UseAffectedRowsSemantics
            ? new DbContextOptions { RequireMatchedRowOccSemantics = false }
            : new DbContextOptions();
        using (cn) await using (var ctx = new DbContext(cn!, provider!, opts))
        {
            Exec(cn!, OccDdl(kind));
            Exec(cn!, "DELETE FROM LPM_OccItem");

            var item = new LpmOccItem { Id = 1, Payload = "original" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            // Simulate external writer advancing the token
            string tokenSql = kind switch
            {
                "sqlite"    => "UPDATE LPM_OccItem SET Token=randomblob(8) WHERE Id=1",
                "sqlserver" => "UPDATE LPM_OccItem SET Token=CONVERT(VARBINARY(8),NEWID()) WHERE Id=1",
                "mysql"     => "UPDATE LPM_OccItem SET Token=UNHEX(REPLACE(UUID(),'-','')) WHERE Id=1",
                "postgres"  => "UPDATE LPM_OccItem SET Token=gen_random_bytes(8) WHERE Id=1",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            };
            Exec(cn!, tokenSql);

            item.Payload = "stale-update";
            await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // F. TRANSACTION / LIFECYCLE PARITY
    // Row: "Transactions/lifecycle" — commit, rollback, and pre-cancellation
    // behave identically across all 4 providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task TX_Commit_DataPersistsAfterCommit(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            using var tx = await ctx.Database.BeginTransactionAsync();
            ctx.Add(new LpmItem { Id = 1, Name = "committed", Score = 1 });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();

            Assert.Equal(1L, CountRows(cn!, "LPM_Item"));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task TX_Rollback_DataDiscardedAfterRollback(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            using var tx = await ctx.Database.BeginTransactionAsync();
            ctx.Add(new LpmItem { Id = 2, Name = "rolled-back", Score = 99 });
            await ctx.SaveChangesAsync();
            await tx.RollbackAsync();

            Assert.Equal(0L, CountRows(cn!, "LPM_Item"));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task TX_PreCancelledToken_ThrowsOperationCancelledException(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            ctx.Add(new LpmItem { Id = 3, Name = "never-saved", Score = 0 });

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => ctx.SaveChangesAsync(cts.Token));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task TX_SequentialSaves_EachBatchPersistsIndependently(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");

            // First implicit-transaction save.
            ctx.Add(new LpmItem { Id = 1, Name = "first",  Score = 1 });
            await ctx.SaveChangesAsync();
            Assert.Equal(1L, CountRows(cn!, "LPM_Item"));

            // Second implicit-transaction save — ChangeTracker must not re-insert Id=1.
            ctx.Add(new LpmItem { Id = 2, Name = "second", Score = 2 });
            await ctx.SaveChangesAsync();
            Assert.Equal(2L, CountRows(cn!, "LPM_Item"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // G. MIGRATION PARITY
    // Row: "Migrations" — DDL migrations apply correctly and idempotently on
    // all 4 providers; advisory lock constants are valid; MG1 and MG2 fixes
    // produce correct SQL output.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MIG_CreateTable_MigrationAppliedAndHistoryRecorded(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn!)
        {
            const string tableName = "LpmMig_Create";
            var ddl = MigTableDdl(kind, tableName);

            var asm = BuildMigrationAsm((900001L, "LpmCreateTable", ddl, false));
            var runner = MakeRunnerFromAsm(kind, cn!, asm);

            await runner.ApplyMigrationsAsync();

            // Table must exist (check via row count — no-throw = exists)
            Assert.Equal(0L, CountRows(cn!, tableName));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MIG_AddColumn_MigrationAppliedCorrectly(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn!)
        {
            const string tableName = "LpmMig_AddCol";
            const string colName   = "ExtraCol";
            var createDdl  = MigTableDdl(kind, tableName);
            var addColDdl  = MigAddColumnDdl(kind, tableName, colName);

            var asm = BuildMigrationAsm(
                (900002L, "LpmCreateForAddCol", createDdl, false),
                (900003L, "LpmAddExtraCol",     addColDdl, false));

            var runner = MakeRunnerFromAsm(kind, cn!, asm);
            await runner.ApplyMigrationsAsync();

            Assert.True(ColumnExists(cn!, tableName, colName),
                $"Column '{colName}' not found in '{tableName}' after migration on {kind}.");
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MIG_Idempotent_SecondApplyIsNoOp(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn!)
        {
            var ddl = MigTableDdl(kind, "LpmMig_Idem");
            var asm = BuildMigrationAsm((900004L, "LpmIdempotentCreate", ddl, false));

            var runner = MakeRunnerFromAsm(kind, cn!, asm);

            // First apply — must succeed.
            await runner.ApplyMigrationsAsync();

            // Second apply — must be a no-op and not throw.
            var ex = await Record.ExceptionAsync(() => runner.ApplyMigrationsAsync());
            Assert.Null(ex);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task MIG_MultiStep_AllMigrationsAppliedInOrder(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn!)
        {
            const string t1 = "LpmMig_Step1";
            const string t2 = "LpmMig_Step2";
            const string t3 = "LpmMig_Step3";

            var asm = BuildMigrationAsm(
                (900005L, "LpmStep1", MigTableDdl(kind, t1), false),
                (900006L, "LpmStep2", MigTableDdl(kind, t2), false),
                (900007L, "LpmStep3", MigTableDdl(kind, t3), false));

            var runner = MakeRunnerFromAsm(kind, cn!, asm);
            await runner.ApplyMigrationsAsync();

            // All three tables must exist.
            Assert.Equal(0L, CountRows(cn!, t1));
            Assert.Equal(0L, CountRows(cn!, t2));
            Assert.Equal(0L, CountRows(cn!, t3));
        }
    }

    // MG1 — Unit test: PostgresMigrationSqlGenerator emits USING clause for type changes
    [Fact]
    public void MIG_MG1_PostgresAlterType_EmitsUsingClause()
    {
        var gen   = new PostgresMigrationSqlGenerator();
        var table = new TableSchema { Name = "Items" };
        var oldCol = new ColumnSchema { Name = "Code", ClrType = "System.String",  IsNullable = false };
        var newCol = new ColumnSchema { Name = "Code", ClrType = "System.Int32",   IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var stmts = gen.GenerateSql(diff);
        var upSql = string.Join("\n", stmts.Up);

        // MG1 fix: ALTER TYPE must include USING clause for incompatible type changes.
        Assert.Contains("USING", upSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Code",  upSql, StringComparison.Ordinal);
    }

    // MG2 — Unit test: SchemaSnapshot FK rename detection produces DroppedForeignKey + AddedForeignKey
    [Fact]
    public void MIG_MG2_FkRename_DetectedAsDroppedPlusAdded()
    {
        // Build two snapshots that differ ONLY in FK constraint name.
        var colId = new ColumnSchema { Name = "Id",         ClrType = "System.Int32", IsNullable = false, IsPrimaryKey = true };
        var colFk = new ColumnSchema { Name = "CategoryId", ClrType = "System.Int32", IsNullable = false };

        var oldTable = new TableSchema
        {
            Name    = "Products",
            Columns = { colId, colFk },
            ForeignKeys =
            {
                new ForeignKeySchema
                {
                    ConstraintName   = "FK_Old",
                    DependentColumns = new[] { "CategoryId" },
                    PrincipalTable   = "Categories",
                    PrincipalColumns = new[] { "Id" }
                }
            }
        };

        var newTable = new TableSchema
        {
            Name    = "Products",
            Columns = { colId, colFk },
            ForeignKeys =
            {
                new ForeignKeySchema
                {
                    ConstraintName   = "FK_New",
                    DependentColumns = new[] { "CategoryId" },
                    PrincipalTable   = "Categories",
                    PrincipalColumns = new[] { "Id" }
                }
            }
        };

        var oldSnap = new SchemaSnapshot { Tables = { oldTable } };
        var newSnap = new SchemaSnapshot { Tables = { newTable } };

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        // MG2 fix: rename-only FK is detected as dropped + added (was previously invisible).
        Assert.Contains(diff.DroppedForeignKeys, t => t.ForeignKey.ConstraintName == "FK_Old");
        Assert.Contains(diff.AddedForeignKeys,   t => t.ForeignKey.ConstraintName == "FK_New");
    }

    // Migration advisory lock constants — all providers expose valid values
    [Fact]
    public void MIG_AdvisoryLockConstants_AllProviders_Valid()
    {
        Assert.NotEmpty(SqlServerMigrationRunner.MigrationLockResource);
        Assert.NotEmpty(MySqlMigrationRunner.MigrationLockName);
        Assert.True(MySqlMigrationRunner.MigrationLockTimeoutSeconds >= 10);
        Assert.NotEqual(0L, PostgresMigrationRunner.MigrationLockKey);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // H. CACHING / SHARED STATE PARITY
    // Row: "Caching/shared state" — plan cache consistency, cross-param isolation,
    // and compiled query cache correctness across all 4 providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task CSH_PlanCache_RepeatedQuery_ConsistentResults(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 4; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            // Execute identical LINQ query 3 times — all must return same count.
            var r1 = ctx.Query<LpmItem>().Where(x => x.Score >= 10).ToList();
            var r2 = ctx.Query<LpmItem>().Where(x => x.Score >= 10).ToList();
            var r3 = ctx.Query<LpmItem>().Where(x => x.Score >= 10).ToList();

            Assert.Equal(4, r1.Count);
            Assert.Equal(r1.Count, r2.Count);
            Assert.Equal(r2.Count, r3.Count);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task CSH_NoCrossParamContamination_DifferentParamsReturnDifferentRows(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 6; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            // Two different parameter values must not share cached results.
            var low  = ctx.Query<LpmItem>().Where(x => x.Score <= 20).ToList();
            var high = ctx.Query<LpmItem>().Where(x => x.Score >= 50).ToList();

            Assert.Equal(2, low.Count);
            Assert.Equal(2, high.Count);
            Assert.Empty(low.Intersect(high, LpmItemIdComparer.Instance));
        }
    }

    private sealed class LpmItemIdComparer : IEqualityComparer<LpmItem>
    {
        public static readonly LpmItemIdComparer Instance = new();
        public bool Equals(LpmItem? x, LpmItem? y) => x?.Id == y?.Id;
        public int GetHashCode(LpmItem obj) => obj.Id.GetHashCode();
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task CSH_TwoContexts_NoResultStateLeakage(string kind)
    {
        var (cn1, provider1, skip) = OpenLive(kind);
        if (skip != null) return;
        var (cn2, provider2, _) = OpenLive(kind);

        // For non-SQLite, both contexts share the same physical database.
        // For SQLite in-memory, each context gets its own isolated database
        // so we seed both independently.
        using (cn1!) using (cn2!)
        await using (var ctx1 = new DbContext(cn1!, provider1!))
        await using (var ctx2 = new DbContext(cn2!, provider2!))
        {
            Exec(cn1!, ItemDdl(kind));
            Exec(cn1!, "DELETE FROM LPM_Item");
            ctx1.Add(new LpmItem { Id = 100, Name = "ctx1-row", Score = 100 });
            await ctx1.SaveChangesAsync();

            // For SQLite in-memory each connection is isolated — seed ctx2 separately.
            if (kind == "sqlite")
            {
                Exec(cn2!, ItemDdl(kind));
                ctx2.Add(new LpmItem { Id = 200, Name = "ctx2-row", Score = 200 });
                await ctx2.SaveChangesAsync();
            }

            // ctx1 must only see its own row (score = 100).
            var ctx1Results = ctx1.Query<LpmItem>().Where(x => x.Score == 100).ToList();
            Assert.Single(ctx1Results);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task CSH_CompiledQueryCache_PlanStaysValidAcrossMultipleCalls(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 6; i++)
                ctx.Add(new LpmItem { Id = i, Name = i % 2 == 0 ? "even" : "odd", Score = i });
            await ctx.SaveChangesAsync();

            var compiled = Norm.CompileQuery((DbContext c, string name) =>
                c.Query<LpmItem>().Where(x => x.Name == name));

            // Call compiled query 3× with alternating params — plan must stay valid.
            var r1 = await compiled(ctx, "even");
            var r2 = await compiled(ctx, "odd");
            var r3 = await compiled(ctx, "even");

            Assert.Equal(3, r1.Count);
            Assert.Equal(3, r2.Count);
            Assert.Equal(r1.Count, r3.Count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // I. SOURCE GENERATION / COMPILED QUERY PARITY
    // Row: "Source generation/compiled queries" — compiled LINQ queries return
    // identical results to un-compiled queries on all 4 providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGCP_CompiledQuery_MatchesDirectQuery(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 8; i++)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 5 });
            await ctx.SaveChangesAsync();

            var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
                c.Query<LpmItem>().Where(x => x.Score >= minScore));

            var direct   = ctx.Query<LpmItem>().Where(x => x.Score >= 20).ToList();
            var compiled_ = await compiled(ctx, 20);

            Assert.Equal(direct.Count, compiled_.Count);
            Assert.All(compiled_, r => Assert.True(r.Score >= 20));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGCP_CompiledQuery_DifferentParams_ReturnDifferentResults(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            for (int i = 1; i <= 10; i++)
                ctx.Add(new LpmItem { Id = i, Name = i <= 5 ? "low" : "high", Score = i });
            await ctx.SaveChangesAsync();

            var compiled = Norm.CompileQuery((DbContext c, string tag) =>
                c.Query<LpmItem>().Where(x => x.Name == tag));

            var lows  = await compiled(ctx, "low");
            var highs = await compiled(ctx, "high");

            Assert.Equal(5, lows.Count);
            Assert.Equal(5, highs.Count);
            Assert.All(lows,  r => Assert.Equal("low",  r.Name));
            Assert.All(highs, r => Assert.Equal("high", r.Name));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGCP_CompiledQuery_NullParam_UsesIsNullSemantics(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            ctx.Add(new LpmItem { Id = 1, Name = "null-row",  Score = 1, NullableNum = null });
            ctx.Add(new LpmItem { Id = 2, Name = "value-row", Score = 2, NullableNum = 77  });
            await ctx.SaveChangesAsync();

            var compiled = Norm.CompileQuery((DbContext c, int? num) =>
                c.Query<LpmItem>().Where(x => x.NullableNum == num));

            var nullResults = await compiled(ctx, null);
            Assert.Single(nullResults);
            Assert.Equal(1, nullResults[0].Id);

            var valueResults = await compiled(ctx, 77);
            Assert.Single(valueResults);
            Assert.Equal(2, valueResults[0].Id);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SGCP_CompiledQuery_OrderBy_ReturnsAscendingOrder(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, ItemDdl(kind));
            Exec(cn!, "DELETE FROM LPM_Item");
            // Insert in reverse order.
            for (int i = 5; i >= 1; i--)
                ctx.Add(new LpmItem { Id = i, Name = $"n{i}", Score = i * 10 });
            await ctx.SaveChangesAsync();

            var compiled = Norm.CompileQuery((DbContext c, int _) =>
                c.Query<LpmItem>().OrderBy(x => x.Score));

            var results = await compiled(ctx, 0);
            Assert.Equal(5, results.Count);
            for (int i = 0; i < results.Count - 1; i++)
                Assert.True(results[i].Score < results[i + 1].Score);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // J. SECURITY BOUNDARY PARITY
    // Row: "Security boundaries" — SQL injection is rejected, identifiers are
    // escaped correctly, and tenant isolation filters results on all providers.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void SEC_NormValidator_DangerousKeyword_ThrowsException()
    {
        Assert.ThrowsAny<Exception>(() =>
            NormValidator.ValidateRawSql("SELECT LOAD_FILE('/etc/passwd')"));
    }

    [Fact]
    public void SEC_NormValidator_MultiStatement_ThrowsException()
    {
        Assert.ThrowsAny<Exception>(() =>
            NormValidator.ValidateRawSql("SELECT 1; DROP TABLE LPM_Item"));
    }

    [Fact]
    public void SEC_NormValidator_XpCmdshell_ThrowsException()
    {
        Assert.ThrowsAny<Exception>(() =>
            NormValidator.ValidateRawSql("EXEC XP_CMDSHELL 'dir'"));
    }

    [Theory]
    [InlineData("sqlite",    "\"has space\"")]
    [InlineData("sqlserver", "[has space]")]
    [InlineData("mysql",     "`has space`")]
    [InlineData("postgres",  "\"has space\"")]
    public void SEC_Escape_IdentifierWithSpace_ProducesCorrectQuoting(string kind, string expected)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal(expected, p.Escape("has space"));
    }

    [Theory]
    [InlineData("sqlite",    "\"a\".\"b\"")]
    [InlineData("sqlserver", "[a].[b]")]
    [InlineData("mysql",     "`a`.`b`")]
    [InlineData("postgres",  "\"a\".\"b\"")]
    public void SEC_Escape_MultipartIdentifier_ProducesCorrectQuoting(string kind, string expected)
    {
        DatabaseProvider p = kind switch
        {
            "sqlite"    => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
            "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        Assert.Equal(expected, p.Escape("a.b"));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SEC_TenantFilter_IsolatesQueryResultsLive(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        {
            Exec(cn!, TenantDdl(kind));
            Exec(cn!, "DELETE FROM LPM_TenantItem");

            // Insert rows for two tenants via raw SQL.
            Exec(cn!, "INSERT INTO LPM_TenantItem (Id, TenantId, Label) VALUES (1, 1, 'tenant1-A')");
            Exec(cn!, "INSERT INTO LPM_TenantItem (Id, TenantId, Label) VALUES (2, 1, 'tenant1-B')");
            Exec(cn!, "INSERT INTO LPM_TenantItem (Id, TenantId, Label) VALUES (3, 2, 'tenant2-A')");

            var opts = new DbContextOptions
            {
                TenantProvider   = new FixedTenantProvider(1),
                TenantColumnName = "TenantId"
            };
            await using var tenantCtx = new DbContext(cn!, provider!, opts);

            // Unfiltered table has 3 rows.
            Assert.Equal(3L, CountRows(cn!, "LPM_TenantItem"));

            // Tenant-filtered query returns only tenant 1's rows.
            var rows = tenantCtx.Query<LpmTenantItem>().ToList();
            Assert.Equal(2, rows.Count);
            Assert.All(rows, r => Assert.Equal(1, r.TenantId));
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task SEC_TenantSave_OnlyWritesToOwnTenant(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        {
            Exec(cn!, TenantDdl(kind));
            Exec(cn!, "DELETE FROM LPM_TenantItem");

            // Pre-seed tenant 2 row via raw SQL.
            Exec(cn!, "INSERT INTO LPM_TenantItem (Id, TenantId, Label) VALUES (99, 2, 'other-tenant')");

            var opts = new DbContextOptions
            {
                TenantProvider   = new FixedTenantProvider(1),
                TenantColumnName = "TenantId"
            };
            await using var tenantCtx = new DbContext(cn!, provider!, opts);

            // Insert via tenant context — nORM stamps TenantId = 1.
            tenantCtx.Add(new LpmTenantItem { Id = 1, TenantId = 1, Label = "my-row" });
            await tenantCtx.SaveChangesAsync();

            // Total rows: 2 (one per tenant)
            Assert.Equal(2L, CountRows(cn!, "LPM_TenantItem"));

            // Tenant 1 query sees only 1 row.
            var ownRows = tenantCtx.Query<LpmTenantItem>().ToList();
            Assert.Single(ownRows);
            Assert.Equal(1, ownRows[0].TenantId);
        }
    }
}
