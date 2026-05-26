using System;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using MigrationBase = nORM.Migration.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live-provider DDL parity gate: verifies that schema-diff migration SQL generators
/// produce correct, executable DDL for ADD COLUMN, DROP COLUMN, CREATE/DROP TABLE,
/// DOWN rollback, and fault-injection recovery across all four providers.
/// Addresses v1 Items 14 (fault-injected recovery evidence) and 15 (migration SQL live parity).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderMigrationDdlParityTests
{
    // ── Generators ───────────────────────────────────────────────────────────

    private static IMigrationSqlGenerator Generator(string kind) => kind switch
    {
        "sqlite"    => new SqliteMigrationSqlGenerator(),
        "sqlserver" => new SqlServerMigrationSqlGenerator(),
        "mysql"     => new MySqlMigrationSqlGenerator(),
        "postgres"  => new PostgresMigrationSqlGenerator(),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    // ── Connection factory ───────────────────────────────────────────────────

    private static (DbConnection? Cn, string? Skip) Open(string kind)
    {
        switch (kind)
        {
            case "sqlite":
            {
                var cn = new SqliteConnection("Data Source=:memory:");
                cn.Open();
                return (cn, null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetConnectionString("sqlserver");
                if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_SQLSERVER not set.");
                return (OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs), null);
            }
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetConnectionString("mysql");
                if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_MYSQL not set.");
                return (OpenReflected("MySqlConnector.MySqlConnection, MySqlConnector", cs), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetConnectionString("postgres");
                if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_POSTGRES not set.");
                return (OpenReflected("Npgsql.NpgsqlConnection, Npgsql", cs), null);
            }
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection OpenReflected(string typeName, string cs)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Cannot load '{typeName}'.");
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

    private static void ExecSafe(DbConnection? cn, string sql)
    {
        if (cn == null) return;
        try { Exec(cn, sql); } catch { }
    }

    // ── Schema introspection helpers ─────────────────────────────────────────

    private static bool ColumnExists(DbConnection cn, string table, string column)
    {
        using var cmd = cn.CreateCommand();
        if (cn is SqliteConnection)
        {
            cmd.CommandText = $"PRAGMA table_info(\"{table}\")";
            using var r = cmd.ExecuteReader();
            while (r.Read())
                if (string.Equals(r.GetString(1), column, StringComparison.OrdinalIgnoreCase))
                    return true;
            return false;
        }
        cmd.CommandText =
            $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
            $"WHERE TABLE_NAME='{table}' AND COLUMN_NAME='{column}'";
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    private static bool TableExists(DbConnection cn, string table)
    {
        try
        {
            using var cmd = cn.CreateCommand();
            if (cn is SqliteConnection)
                cmd.CommandText = $"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'";
            else
                cmd.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{table}'";
            return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
        }
        catch { return false; }
    }

    private static bool IsNullable(DbConnection cn, string table, string column)
    {
        using var cmd = cn.CreateCommand();
        if (cn is SqliteConnection)
        {
            cmd.CommandText = $"PRAGMA table_info(\"{table}\")";
            using var r = cmd.ExecuteReader();
            while (r.Read())
                if (string.Equals(r.GetString(1), column, StringComparison.OrdinalIgnoreCase))
                    return r.GetInt32(3) == 0; // notnull=0 → nullable
            return true;
        }
        cmd.CommandText =
            $"SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
            $"WHERE TABLE_NAME='{table}' AND COLUMN_NAME='{column}'";
        var val = cmd.ExecuteScalar() as string;
        return string.Equals(val, "YES", StringComparison.OrdinalIgnoreCase);
    }

    // ── Base-table DDL (provider-specific CREATE TABLE) ──────────────────────

    private static string CreateBaseDdl(string kind, string table) => kind switch
    {
        "sqlite"    => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL)",
        "sqlserver" => $"IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE [{table}] ([Id] INT NOT NULL PRIMARY KEY, [Name] NVARCHAR(200) NOT NULL)",
        "mysql"     => $"CREATE TABLE IF NOT EXISTS `{table}` (`Id` INT NOT NULL PRIMARY KEY, `Name` VARCHAR(200) NOT NULL)",
        "postgres"  => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INT NOT NULL PRIMARY KEY, \"Name\" VARCHAR(200) NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string DropTableDdl(string kind, string table) => kind switch
    {
        "sqlite"    => $"DROP TABLE IF EXISTS \"{table}\"",
        "sqlserver" => $"IF OBJECT_ID('{table}','U') IS NOT NULL DROP TABLE [{table}]",
        "mysql"     => $"DROP TABLE IF EXISTS `{table}`",
        "postgres"  => $"DROP TABLE IF EXISTS \"{table}\" CASCADE",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    // ── Execute all SQL statements in a SchemaDiff result ───────────────────

    private static void ApplyStatements(DbConnection cn, System.Collections.Generic.IReadOnlyList<string> stmts)
    {
        foreach (var s in stmts)
            if (!string.IsNullOrWhiteSpace(s))
                Exec(cn, s);
    }

    // ── Build minimal TableSchema / ColumnSchema for diffs ──────────────────

    private static TableSchema BaseTable(string name) => new TableSchema
    {
        Name = name,
        Columns =
        {
            new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!,    IsPrimaryKey = true,  IsNullable = false, IsIdentity = false },
            new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsPrimaryKey = false, IsNullable = false, IsIdentity = false },
        }
    };

    private static TableSchema TableWithExtra(string name) => new TableSchema
    {
        Name = name,
        Columns =
        {
            new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsPrimaryKey = true,  IsNullable = false, IsIdentity = false },
            new ColumnSchema { Name = "Name",  ClrType = typeof(string).FullName!, IsPrimaryKey = false, IsNullable = false, IsIdentity = false },
            new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!,    IsPrimaryKey = false, IsNullable = true,  IsIdentity = false },
        }
    };

    // ── Fault-injection migration assembly builder ───────────────────────────

    private static readonly ConstructorInfo _migBaseCtor =
        typeof(MigrationBase).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;
    private static readonly MethodInfo _upAbstract =
        typeof(MigrationBase).GetMethod("Up",
            new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
    private static readonly MethodInfo _downAbstract =
        typeof(MigrationBase).GetMethod("Down",
            new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
    private static readonly ConstructorInfo _ioExCtor =
        typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;
    private static readonly MethodInfo _createCmdMi =
        typeof(DbConnection).GetMethod("CreateCommand")!;
    private static readonly MethodInfo _setPropTextMi =
        typeof(DbCommand).GetProperty("CommandText")!.SetMethod!;
    private static readonly MethodInfo _setPropTxMi =
        typeof(DbCommand).GetProperty("Transaction")!.SetMethod!;
    private static readonly MethodInfo _execNonQMi =
        typeof(DbCommand).GetMethod("ExecuteNonQuery")!;
    private static readonly MethodInfo _disposeCmdMi =
        typeof(IDisposable).GetMethod("Dispose")!;

    private static Assembly FaultAssembly(long goodVer, string goodDdl, long badVer)
    {
        var ab  = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("FiDdl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        // Good migration — executes DDL
        var tb1 = mod.DefineType("GoodMig", TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb1, goodVer, "GoodMig");
        EmitDdlUp(tb1, goodDdl);
        EmitNoOpDown(tb1);
        tb1.CreateType();

        // Bad migration — throws immediately
        var tb2 = mod.DefineType("BadMig", TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb2, badVer, "BadMig");
        EmitThrowUp(tb2);
        EmitNoOpDown(tb2);
        tb2.CreateType();

        return ab;
    }

    private static Assembly GoodAssembly(long ver, string ddl)
    {
        var ab  = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("GoodDdl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb  = mod.DefineType("GoodMig", TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, ver, "GoodMig");
        EmitDdlUp(tb, ddl);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    private static void EmitCtor(TypeBuilder tb, long ver, string name)
    {
        var cb = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
        var il = cb.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldc_I8, ver);
        il.Emit(OpCodes.Ldstr, name);
        il.Emit(OpCodes.Call, _migBaseCtor);
        il.Emit(OpCodes.Ret);
    }

    private static void EmitDdlUp(TypeBuilder tb, string ddl)
    {
        var mb = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
            typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
        var il    = mb.GetILGenerator();
        var local = il.DeclareLocal(typeof(DbCommand));
        il.Emit(OpCodes.Ldarg_1); il.Emit(OpCodes.Callvirt, _createCmdMi); il.Emit(OpCodes.Stloc, local);
        il.Emit(OpCodes.Ldloc, local); il.Emit(OpCodes.Ldarg_2); il.Emit(OpCodes.Callvirt, _setPropTxMi);
        il.Emit(OpCodes.Ldloc, local); il.Emit(OpCodes.Ldstr, ddl); il.Emit(OpCodes.Callvirt, _setPropTextMi);
        il.Emit(OpCodes.Ldloc, local); il.Emit(OpCodes.Callvirt, _execNonQMi); il.Emit(OpCodes.Pop);
        il.Emit(OpCodes.Ldloc, local); il.Emit(OpCodes.Callvirt, _disposeCmdMi);
        il.Emit(OpCodes.Ret);
        tb.DefineMethodOverride(mb, _upAbstract);
    }

    private static void EmitThrowUp(TypeBuilder tb)
    {
        var mb = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
            typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
        var il = mb.GetILGenerator();
        il.Emit(OpCodes.Ldstr, "simulated migration fault");
        il.Emit(OpCodes.Newobj, _ioExCtor);
        il.Emit(OpCodes.Throw);
        tb.DefineMethodOverride(mb, _upAbstract);
    }

    private static void EmitNoOpDown(TypeBuilder tb)
    {
        var mb = tb.DefineMethod("Down",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
            typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
        mb.GetILGenerator().Emit(OpCodes.Ret);
        tb.DefineMethodOverride(mb, _downAbstract);
    }

    private static IMigrationRunner MigRunner(string kind, DbConnection cn, Assembly asm) => kind switch
    {
        "sqlite"    => new SqliteMigrationRunner(cn, asm),
        "sqlserver" => new SqlServerMigrationRunner(cn, asm),
        "mysql"     => new MySqlMigrationRunner(cn, asm),
        "postgres"  => new PostgresMigrationRunner(cn, asm),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static long HistoryCount(DbConnection cn, long version)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = {version}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — ADD COLUMN: nullable int added to existing table
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_AddColumn_NullableInt_AppearsInSchema(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_AddCol";

        try
        {
            Exec(db, CreateBaseDdl(kind, table));
            Assert.False(ColumnExists(db, table, "Score"), "Score should not exist yet");

            var baseTable = BaseTable(table);
            var score     = new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = true };
            var diff      = new SchemaDiff();
            diff.AddedColumns.Add((baseTable, score));

            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(ColumnExists(db, table, "Score"),
                $"[{kind}] Column Score should exist after ADD COLUMN.");
            Assert.True(IsNullable(db, table, "Score"),
                $"[{kind}] Column Score should be nullable.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — ADD COLUMN NOT NULL with DEFAULT
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_AddColumnNotNullWithDefault_AppearsInSchema(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_AddColNN";

        try
        {
            Exec(db, CreateBaseDdl(kind, table));

            var baseTable = BaseTable(table);
            var col       = new ColumnSchema { Name = "Rating", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0" };
            var diff      = new SchemaDiff();
            diff.AddedColumns.Add((baseTable, col));

            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(ColumnExists(db, table, "Rating"),
                $"[{kind}] Column Rating should exist after ADD COLUMN NOT NULL DEFAULT 0.");
            Assert.False(IsNullable(db, table, "Rating"),
                $"[{kind}] Column Rating should be NOT NULL.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — DROP COLUMN: column removed from existing table
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_DropColumn_DisappearsFromSchema(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DropCol";

        // SQLite < 3.35.0 does not support ALTER TABLE … DROP COLUMN.
        if (kind == "sqlite")
        {
            try
            {
                Exec(db, "CREATE TABLE \"_ddl_probe\" (a INT, b INT)");
                Exec(db, "ALTER TABLE \"_ddl_probe\" DROP COLUMN b");
                ExecSafe(db, "DROP TABLE \"_ddl_probe\"");
            }
            catch { db.Dispose(); return; }
        }

        try
        {
            var createWithExtra = kind switch
            {
                "sqlite"    => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"Score\" INTEGER)",
                "sqlserver" => $"IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE [{table}] ([Id] INT PRIMARY KEY, [Name] NVARCHAR(200) NOT NULL, [Score] INT NULL)",
                "mysql"     => $"CREATE TABLE IF NOT EXISTS `{table}` (`Id` INT PRIMARY KEY, `Name` VARCHAR(200) NOT NULL, `Score` INT NULL)",
                "postgres"  => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INT PRIMARY KEY, \"Name\" VARCHAR(200) NOT NULL, \"Score\" INT NULL)",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            };
            Exec(db, createWithExtra);
            Assert.True(ColumnExists(db, table, "Score"), "Score must exist before drop");

            var extTable = TableWithExtra(table);
            var score    = extTable.Columns.First(c => c.Name == "Score");
            var diff     = new SchemaDiff();
            diff.DroppedColumns.Add((extTable, score));

            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.False(ColumnExists(db, table, "Score"),
                $"[{kind}] Column Score should be gone after DROP COLUMN.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — CREATE TABLE via SchemaDiff AddedTables
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_CreateTable_AppearsInSchema(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_CreateTable";

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));
            Assert.False(TableExists(db, table));

            var diff = new SchemaDiff();
            diff.AddedTables.Add(BaseTable(table));
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(TableExists(db, table),
                $"[{kind}] Table {table} should exist after CREATE TABLE.");
            Assert.True(ColumnExists(db, table, "Id"),
                $"[{kind}] Column Id must be present.");
            Assert.True(ColumnExists(db, table, "Name"),
                $"[{kind}] Column Name must be present.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — DROP TABLE via SchemaDiff DroppedTables
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_DropTable_DisappearsFromSchema(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DropTable";

        try
        {
            Exec(db, CreateBaseDdl(kind, table));
            Assert.True(TableExists(db, table));

            var diff = new SchemaDiff();
            diff.DroppedTables.Add(BaseTable(table));
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.False(TableExists(db, table),
                $"[{kind}] Table {table} should be gone after DROP TABLE.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — DOWN migration reverses ADD COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_DownReverses_AddColumn(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DownAdd";

        if (kind == "sqlite")
        {
            try
            {
                Exec(db, "CREATE TABLE \"_ddl_probe2\" (a INT, b INT)");
                Exec(db, "ALTER TABLE \"_ddl_probe2\" DROP COLUMN b");
                ExecSafe(db, "DROP TABLE \"_ddl_probe2\"");
            }
            catch { db.Dispose(); return; }
        }

        try
        {
            Exec(db, CreateBaseDdl(kind, table));
            var baseTable = BaseTable(table);
            var score     = new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = true };
            var diff      = new SchemaDiff();
            diff.AddedColumns.Add((baseTable, score));
            var sql = Generator(kind).GenerateSql(diff);

            ApplyStatements(db, sql.Up);
            Assert.True(ColumnExists(db, table, "Score"), $"[{kind}] UP: Score must exist");

            ApplyStatements(db, sql.Down);
            Assert.False(ColumnExists(db, table, "Score"),
                $"[{kind}] DOWN: Score should be removed after rollback.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 15 — DOWN migration reverses CREATE TABLE
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_DownReverses_CreateTable(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DownCreate";

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));

            var diff = new SchemaDiff();
            diff.AddedTables.Add(BaseTable(table));
            var sql = Generator(kind).GenerateSql(diff);

            ApplyStatements(db, sql.Up);
            Assert.True(TableExists(db, table), $"[{kind}] UP: table must exist");

            ApplyStatements(db, sql.Down);
            Assert.False(TableExists(db, table),
                $"[{kind}] DOWN: table should be dropped after rollback.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 14 — Fault injection: failing migration does NOT record history
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Migration_FaultInjection_HistoryNotRecordedOnFailure(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table   = "DdlParity_FaultInj";
        const long   goodVer = 9920L;
        const long   badVer  = 9921L;

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));

            var goodDdl = CreateBaseDdl(kind, table);
            var asm     = FaultAssembly(goodVer, goodDdl, badVer);
            var runner  = MigRunner(kind, db, asm);

            var ex = await Assert.ThrowsAnyAsync<Exception>(() => runner.ApplyMigrationsAsync());
            Assert.Contains("simulated migration fault", ex.ToString(), StringComparison.OrdinalIgnoreCase);

            // SQLite wraps all pending migrations in ONE transaction: when the bad migration
            // throws, the full batch is rolled back — the good migration's history is also gone.
            // Per-migration runners (MySQL, SQL Server, PostgreSQL) commit each migration
            // individually, so the good migration's history survives the bad one's failure.
            var expectGoodPresent = kind != "sqlite";
            Assert.True(HistoryCount(db, goodVer) == (expectGoodPresent ? 1L : 0L),
                $"[{kind}] Good migration history must {(expectGoodPresent ? "be present" : "be absent (SQLite batch rollback)")} after failure.");
            // The bad migration threw; its history row must NOT appear on any provider.
            Assert.True(HistoryCount(db, badVer) == 0L,
                $"[{kind}] Bad migration history entry must NOT be present after failure.");
        }
        finally
        {
            ExecSafe(db, DropTableDdl(kind, table));
            ExecSafe(db, $"DELETE FROM \"__NormMigrationsHistory\" WHERE Version IN ({goodVer}, {badVer})");
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 14 — Fault injection recovery: re-apply after failure succeeds
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Migration_FaultInjection_ReApplyAfterFailureSucceeds(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_Replay";
        const long   ver   = 9930L;

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));
            ExecSafe(db, $"DELETE FROM \"__NormMigrationsHistory\" WHERE Version = {ver}");

            // First apply: the migration throws.
            var faultAb  = AssemblyBuilder.DefineDynamicAssembly(
                new AssemblyName("FaultReplay_" + Guid.NewGuid().ToString("N")),
                AssemblyBuilderAccess.Run);
            var faultMod = faultAb.DefineDynamicModule("Main");
            var faultTb  = faultMod.DefineType("ThrowMig", TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
            EmitCtor(faultTb, ver, "ThrowMig");
            EmitThrowUp(faultTb);
            EmitNoOpDown(faultTb);
            faultTb.CreateType();

            await Assert.ThrowsAnyAsync<Exception>(() => MigRunner(kind, db, faultAb).ApplyMigrationsAsync());
            Assert.True(HistoryCount(db, ver) == 0L,
                $"[{kind}] History must be absent after failed migration.");

            // Second apply: a good migration at the same version — succeeds.
            var goodDdl = CreateBaseDdl(kind, table);
            await MigRunner(kind, db, GoodAssembly(ver, goodDdl)).ApplyMigrationsAsync();

            Assert.True(HistoryCount(db, ver) == 1L,
                $"[{kind}] History must be present after successful re-apply.");
            Assert.True(TableExists(db, table),
                $"[{kind}] Table must exist after successful re-apply.");
        }
        finally
        {
            ExecSafe(db, DropTableDdl(kind, table));
            ExecSafe(db, $"DELETE FROM \"__NormMigrationsHistory\" WHERE Version = {ver}");
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Item 13 — RENAME COLUMN: column renamed with data preserved
    // ══════════════════════════════════════════════════════════════════════════

    private static string InsertRowDdl(string kind, string table) => kind switch
    {
        "sqlite"    => $"INSERT INTO \"{table}\" (\"Id\", \"OldName\") VALUES (1, 'hello')",
        "sqlserver" => $"INSERT INTO [{table}] ([Id], [OldName]) VALUES (1, 'hello')",
        "mysql"     => $"INSERT INTO `{table}` (`Id`, `OldName`) VALUES (1, 'hello')",
        "postgres"  => $"INSERT INTO \"{table}\" (\"Id\", \"OldName\") VALUES (1, 'hello')",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string CreateRenameBaseDdl(string kind, string table) => kind switch
    {
        "sqlite"    => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INTEGER PRIMARY KEY, \"OldName\" TEXT NOT NULL)",
        "sqlserver" => $"IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE [{table}] ([Id] INT NOT NULL PRIMARY KEY, [OldName] NVARCHAR(200) NOT NULL)",
        "mysql"     => $"CREATE TABLE IF NOT EXISTS `{table}` (`Id` INT NOT NULL PRIMARY KEY, `OldName` VARCHAR(200) NOT NULL)",
        "postgres"  => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INT NOT NULL PRIMARY KEY, \"OldName\" VARCHAR(200) NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string? ReadNewNameValue(DbConnection cn, string kind, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = kind switch
        {
            "sqlite"    => $"SELECT \"NewName\" FROM \"{table}\" WHERE \"Id\" = 1",
            "sqlserver" => $"SELECT [NewName] FROM [{table}] WHERE [Id] = 1",
            "mysql"     => $"SELECT `NewName` FROM `{table}` WHERE `Id` = 1",
            "postgres"  => $"SELECT \"NewName\" FROM \"{table}\" WHERE \"Id\" = 1",
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var val = cmd.ExecuteScalar();
        return val is DBNull or null ? null : (string)val;
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_RenameColumn_OldGoneNewPresentDataPreserved(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_Rename";

        try
        {
            Exec(db, CreateRenameBaseDdl(kind, table));
            Exec(db, InsertRowDdl(kind, table));

            Assert.True(ColumnExists(db, table, "OldName"),  $"[{kind}] OldName should exist before rename.");
            Assert.False(ColumnExists(db, table, "NewName"), $"[{kind}] NewName should not exist before rename.");

            var baseTable = new TableSchema { Name = table, Columns = { new ColumnSchema { Name = "Id",      IsPrimaryKey = true  }, new ColumnSchema { Name = "OldName" } } };
            var newCol    = new ColumnSchema { Name = "NewName", ClrType = typeof(string).FullName!, IsNullable = false };
            var diff      = new SchemaDiff();
            diff.RenamedColumns.Add((baseTable, "OldName", newCol));

            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.False(ColumnExists(db, table, "OldName"), $"[{kind}] OldName should be gone after rename.");
            Assert.True(ColumnExists(db, table, "NewName"),  $"[{kind}] NewName should exist after rename.");

            // Data must survive the rename.
            var value = ReadNewNameValue(db, kind, table);
            Assert.Equal("hello", value);
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_RenameColumn_DownReverses(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_RenameDown";

        try
        {
            Exec(db, CreateRenameBaseDdl(kind, table));

            var baseTable = new TableSchema { Name = table, Columns = { new ColumnSchema { Name = "Id", IsPrimaryKey = true }, new ColumnSchema { Name = "OldName" } } };
            var newCol    = new ColumnSchema { Name = "NewName", ClrType = typeof(string).FullName!, IsNullable = false };
            var diff      = new SchemaDiff();
            diff.RenamedColumns.Add((baseTable, "OldName", newCol));

            var stmts = Generator(kind).GenerateSql(diff);
            ApplyStatements(db, stmts.Up);

            Assert.False(ColumnExists(db, table, "OldName"), $"[{kind}] OldName should be gone after UP.");
            Assert.True(ColumnExists(db, table, "NewName"),  $"[{kind}] NewName should exist after UP.");

            ApplyStatements(db, stmts.Down);

            Assert.True(ColumnExists(db, table, "OldName"),  $"[{kind}] OldName should be restored after DOWN.");
            Assert.False(ColumnExists(db, table, "NewName"), $"[{kind}] NewName should be gone after DOWN.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }
}
