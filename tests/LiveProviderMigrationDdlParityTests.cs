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

    private static void ResetTable(DbConnection cn, string kind, string table)
        => ExecSafe(cn, DropTableDdl(kind, table));

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

    // True when a SQL Server index exists AND carries a filter predicate (has_filter = 1).
    private static bool SqlServerFilteredIndexExists(DbConnection cn, string table, string indexName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM sys.indexes " +
            $"WHERE name='{indexName}' AND object_id=OBJECT_ID('{table}') AND has_filter=1";
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    // True when a PostgreSQL index exists AND is partial (has a WHERE predicate → indpred IS NOT NULL).
    private static bool PostgresPartialIndexExists(DbConnection cn, string indexName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "SELECT COUNT(*) FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid " +
            $"WHERE c.relname = '{indexName}' AND i.indpred IS NOT NULL";
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    // True when a PostgreSQL index exists AND is an expression/functional index (indexprs IS NOT NULL).
    private static bool PostgresExpressionIndexExists(DbConnection cn, string indexName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "SELECT COUNT(*) FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid " +
            $"WHERE c.relname = '{indexName}' AND i.indexprs IS NOT NULL";
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    private static bool MySqlColumnIsAutoIncrement(DbConnection cn, string table, string column)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
            $"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='{table}' AND COLUMN_NAME='{column}' " +
            $"AND EXTRA LIKE '%auto_increment%'";
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    // MySQL MODIFY COLUMN replaces the WHOLE column definition; altering any attribute (here: widening the
    // type) of an AUTO_INCREMENT column must re-emit AUTO_INCREMENT or MySQL silently drops it, breaking
    // store-generated keys (the next inserts collide on 0). Live-only: needs a real MySQL server.
    [Fact]
    public void LiveProvider_MySql_AlterIdentityColumn_PreservesAutoIncrement()
    {
        var (cn, skip) = Open("mysql");
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_AutoInc";

        try
        {
            ExecSafe(db, $"DROP TABLE IF EXISTS `{table}`");
            Exec(db, $"CREATE TABLE `{table}` (`Id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `Name` VARCHAR(200) NOT NULL)");

            var tableSchema = new TableSchema { Name = table };
            var oldId = new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false, IsIdentity = true };
            var newId = new ColumnSchema { Name = "Id", ClrType = typeof(long).FullName!, IsPrimaryKey = true, IsNullable = false, IsIdentity = true };
            var diff = new SchemaDiff();
            diff.AlteredColumns.Add((tableSchema, newId, oldId));
            ApplyStatements(db, Generator("mysql").GenerateSql(diff).Up);

            // AUTO_INCREMENT must survive the widening (BUG: MODIFY COLUMN dropped it).
            Assert.True(MySqlColumnIsAutoIncrement(db, table, "Id"),
                "AUTO_INCREMENT must be preserved after MODIFY COLUMN on an identity column.");

            // Behavioural: two key-less inserts get distinct auto-generated keys.
            Exec(db, $"INSERT INTO `{table}` (`Name`) VALUES ('a')");
            Exec(db, $"INSERT INTO `{table}` (`Name`) VALUES ('b')");
            using var cnt = db.CreateCommand();
            cnt.CommandText = $"SELECT COUNT(DISTINCT `Id`) FROM `{table}`";
            Assert.Equal(2L, Convert.ToInt64(cnt.ExecuteScalar()));
        }
        finally
        {
            ExecSafe(cn, $"DROP TABLE IF EXISTS `{table}`");
            db.Dispose();
        }
    }

    // MySQL forbids a literal DEFAULT on BLOB/TEXT/JSON/GEOMETRY columns (error 1101: "BLOB, TEXT, GEOMETRY
    // or JSON column can't have a default value"). Adding a NOT NULL LONGTEXT column with a model default to a
    // populated table emitted `... DEFAULT 'none'` and aborted the whole migration. MySQL 8.0.13+ accepts the
    // same default only as a parenthesised expression default (`DEFAULT ('none')`), which also backfills the
    // existing rows. Live-only: needs a real MySQL server.
    [Fact]
    public void LiveProvider_MySql_AddNotNullTextColumnWithDefault_OnPopulatedTable()
    {
        var (cn, skip) = Open("mysql");
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_TextDefault";

        try
        {
            ExecSafe(db, $"DROP TABLE IF EXISTS `{table}`");
            Exec(db, $"CREATE TABLE `{table}` (`Id` INT NOT NULL PRIMARY KEY, `Name` VARCHAR(200) NOT NULL)");
            Exec(db, $"INSERT INTO `{table}` (`Id`, `Name`) VALUES (1, 'row-one')");

            // Added NOT NULL LONGTEXT (no MaxLength → LONGTEXT) column carrying a literal model default, plus a
            // nullable LONGTEXT with an explicit DEFAULT NULL (which MySQL accepts literally — only a non-NULL
            // literal on these types trips error 1101, so NULL must NOT be wrapped).
            var bio = new ColumnSchema { Name = "Bio", ClrType = typeof(string).FullName!, IsNullable = false, DefaultValue = "'none'" };
            var note = new ColumnSchema { Name = "Note", ClrType = typeof(string).FullName!, IsNullable = true, DefaultValue = "NULL" };
            var diff = new SchemaDiff();
            diff.AddedColumns.Add((BaseTable(table), bio));
            diff.AddedColumns.Add((BaseTable(table), note));

            // BUG: emitted `ADD COLUMN `Bio` LONGTEXT NOT NULL DEFAULT 'none'` → MySQL error 1101, migration aborts.
            ApplyStatements(db, Generator("mysql").GenerateSql(diff).Up);

            Assert.True(ColumnExists(db, table, "Bio"), "Bio column must be added.");
            Assert.False(IsNullable(db, table, "Bio"), "Bio must be NOT NULL.");
            Assert.True(ColumnExists(db, table, "Note"), "Note column must be added.");

            // The pre-existing row must be backfilled with the default, not left NULL / rejected.
            using var cmd = db.CreateCommand();
            cmd.CommandText = $"SELECT `Bio` FROM `{table}` WHERE `Id` = 1";
            Assert.Equal("none", Convert.ToString(cmd.ExecuteScalar()));
        }
        finally
        {
            ExecSafe(cn, $"DROP TABLE IF EXISTS `{table}`");
            db.Dispose();
        }
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

    private static long HistoryCount(DbConnection cn, long version, string kind)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = kind switch
        {
            "mysql"    => $"SELECT COUNT(*) FROM `__NormMigrationsHistory` WHERE `Version` = {version}",
            "postgres" => $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE \"Version\" = {version}",
            _          => $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = {version}"
        };
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static string HistoryDeleteSql(string kind, string versionClause) => kind switch
    {
        "mysql"    => $"DELETE FROM `__NormMigrationsHistory` WHERE `Version` {versionClause}",
        "postgres" => $"DELETE FROM \"__NormMigrationsHistory\" WHERE \"Version\" {versionClause}",
        _          => $"DELETE FROM \"__NormMigrationsHistory\" WHERE Version {versionClause}"
    };

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
            ResetTable(db, kind, table);
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
            ResetTable(db, kind, table);
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
            ResetTable(db, kind, table);
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
    // DROP COLUMN over a column that carries a DEFAULT constraint.
    // On SQL Server a defaulted column owns a system-named DEFAULT constraint that must be
    // dropped BEFORE the column can be dropped. The generator does this with dynamic SQL —
    // it looks the constraint name up at run time and executes the drop via EXEC(@var). The
    // sibling DropColumn test drops a NON-defaulted column, so at run time that lookup returns
    // NULL and the EXEC never fires; this test forces the EXEC path to actually run and drop a
    // real constraint. If that dynamic drop is broken (e.g. the historic QUOTENAME-inside-EXEC
    // syntax error, or a wrong constraint-name lookup) the subsequent DROP COLUMN fails with a
    // dependency error, so this is an end-to-end guard, not just a parse check.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_DropDefaultedColumn_DropsConstraintAndColumn(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DropDefaultedCol";

        // SQLite < 3.35.0 does not support ALTER TABLE … DROP COLUMN.
        if (kind == "sqlite")
        {
            try
            {
                Exec(db, "CREATE TABLE \"_ddl_probe_def\" (a INT, b INT)");
                Exec(db, "ALTER TABLE \"_ddl_probe_def\" DROP COLUMN b");
                ExecSafe(db, "DROP TABLE \"_ddl_probe_def\"");
            }
            catch { db.Dispose(); return; }
        }

        try
        {
            ResetTable(db, kind, table);
            // "Score" carries a DEFAULT — on SQL Server this materializes a system-named
            // DEFAULT constraint bound to the column, which the drop path must remove first.
            var createWithDefault = kind switch
            {
                "sqlite"    => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"Score\" INTEGER NOT NULL DEFAULT 7)",
                "sqlserver" => $"IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE [{table}] ([Id] INT PRIMARY KEY, [Name] NVARCHAR(200) NOT NULL, [Score] INT NOT NULL DEFAULT 7)",
                "mysql"     => $"CREATE TABLE IF NOT EXISTS `{table}` (`Id` INT PRIMARY KEY, `Name` VARCHAR(200) NOT NULL, `Score` INT NOT NULL DEFAULT 7)",
                "postgres"  => $"CREATE TABLE IF NOT EXISTS \"{table}\" (\"Id\" INT PRIMARY KEY, \"Name\" VARCHAR(200) NOT NULL, \"Score\" INT NOT NULL DEFAULT 7)",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            };
            Exec(db, createWithDefault);
            Assert.True(ColumnExists(db, table, "Score"), "Score must exist before drop");

            var extTable = TableWithExtra(table);
            var score    = extTable.Columns.First(c => c.Name == "Score");
            var diff     = new SchemaDiff();
            diff.DroppedColumns.Add((extTable, score));

            // Must not throw: the dynamic default-constraint drop has to run and succeed so the
            // column becomes droppable.
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.False(ColumnExists(db, table, "Score"),
                $"[{kind}] Defaulted column Score should be gone after DROP COLUMN.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ADD a computed / generated column (SQL Server AS (expr) PERSISTED; PostgreSQL /
    // MySQL GENERATED ALWAYS AS (expr) STORED). Provider-idiomatic DDL that interpolates
    // the expression raw and was otherwise unexercised live — verify it produces valid,
    // executable DDL AND computes the right value (an invalid expression, wrong storage
    // keyword, or mis-quoted identifier only surfaces against a real server). SQLite is
    // excluded: it rejects ALTER TABLE ADD COLUMN for a STORED generated column by design.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_AddComputedColumn_AppearsAndComputes(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_ComputedCol";

        try
        {
            ResetTable(db, kind, table);
            Exec(db, CreateBaseDdl(kind, table));
            // Seed a row so the STORED/PERSISTED computed column has data to compute over.
            Exec(db, kind switch
            {
                "sqlserver" => $"INSERT INTO [{table}] ([Id],[Name]) VALUES (5,'x')",
                "mysql"     => $"INSERT INTO `{table}` (`Id`,`Name`) VALUES (5,'x')",
                "postgres"  => $"INSERT INTO \"{table}\" (\"Id\",\"Name\") VALUES (5,'x')",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            });

            // Doubled = Id * 2. The referenced identifier is quoted per provider —
            // PostgreSQL folds unquoted identifiers to lower-case, so "Id" must be quoted.
            var expr = kind switch
            {
                "sqlserver" => "[Id] * 2",
                "mysql"     => "`Id` * 2",
                "postgres"  => "\"Id\" * 2",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            };
            var computed = new ColumnSchema
            {
                Name = "Doubled",
                ClrType = typeof(int).FullName!,
                IsNullable = true,
                ComputedColumnSql = expr,
                IsStoredComputedColumn = true,
            };
            var tbl  = BaseTable(table);
            var diff = new SchemaDiff();
            diff.AddedColumns.Add((tbl, computed));

            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(ColumnExists(db, table, "Doubled"),
                $"[{kind}] computed column Doubled should exist after ADD.");

            // The generated column must actually compute Id*2 = 10 for the seeded row.
            using var q = db.CreateCommand();
            q.CommandText = kind switch
            {
                "sqlserver" => $"SELECT [Doubled] FROM [{table}] WHERE [Id]=5",
                "mysql"     => $"SELECT `Doubled` FROM `{table}` WHERE `Id`=5",
                "postgres"  => $"SELECT \"Doubled\" FROM \"{table}\" WHERE \"Id\"=5",
                _           => throw new ArgumentOutOfRangeException(nameof(kind))
            };
            Assert.Equal(10, Convert.ToInt32(q.ExecuteScalar()));
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CREATE a table carrying a FILTERED index with an INCLUDE column (SQL Server). The
    // generator emits `CREATE INDEX ... ([key]) INCLUDE ([inc]) WHERE <filter>` where the
    // filter predicate is interpolated raw and the include columns are escaped — provider-
    // idiomatic DDL with no prior live coverage from the generator. Guard: the CREATE must
    // execute (a malformed INCLUDE/WHERE only fails live) AND the resulting index must
    // actually carry a filter (sys.indexes.has_filter = 1). SQL Server only: it is the
    // provider that supports INCLUDE plus filtered indexes; PostgreSQL partial/expression
    // indexes are a separate follow-up with different introspection.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlserver")]
    public void LiveProvider_Migration_CreateTableWithFilteredIncludeIndex_IsValidAndFiltered(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table  = "DdlParity_FilteredIdx";
        const string ixName = "IX_DdlParity_FilteredIdx_Score";

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));

            var t     = TableWithExtra(table);   // Id (PK), Name (NOT NULL), Score (nullable int)
            var score = t.Columns.First(c => c.Name == "Score");
            var name  = t.Columns.First(c => c.Name == "Name");
            // Filtered index on Score (WHERE [Score] > 0) that INCLUDEs Name.
            score.Indexes.Add(new ColumnIndexSchema { Name = ixName, Order = 0, FilterSql = "[Score] > 0" });
            name.Indexes.Add(new ColumnIndexSchema { Name = ixName, IsIncluded = true });

            var diff = new SchemaDiff();
            diff.AddedTables.Add(t);

            // Must not throw: the INCLUDE/WHERE index DDL has to be valid on a live server.
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(TableExists(db, table), $"[{kind}] table {table} should exist.");
            Assert.True(SqlServerFilteredIndexExists(db, table, ixName),
                $"[{kind}] filtered index {ixName} should exist and carry a filter predicate.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CREATE a table carrying a PARTIAL index with an INCLUDE column (PostgreSQL). Same
    // raw-filter-interpolation path as the SQL Server guard, on the provider where partial
    // indexes are ubiquitous. Guard: the CREATE must execute AND the resulting index must be
    // partial (pg_index.indpred IS NOT NULL). PostgreSQL 11+ (INCLUDE support).
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("postgres")]
    public void LiveProvider_Migration_CreateTableWithPartialIncludeIndex_IsValidAndPartial(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table  = "DdlParity_PartialIdx";
        const string ixName = "IX_DdlParity_PartialIdx_Score";

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));

            var t     = TableWithExtra(table);   // Id (PK), Name (NOT NULL), Score (nullable int)
            var score = t.Columns.First(c => c.Name == "Score");
            var name  = t.Columns.First(c => c.Name == "Name");
            // Partial index on Score (WHERE "Score" > 0) that INCLUDEs Name. Filter is PG-quoted.
            score.Indexes.Add(new ColumnIndexSchema { Name = ixName, Order = 0, FilterSql = "\"Score\" > 0" });
            name.Indexes.Add(new ColumnIndexSchema { Name = ixName, IsIncluded = true });

            var diff = new SchemaDiff();
            diff.AddedTables.Add(t);

            // Must not throw: the partial/INCLUDE index DDL has to be valid on a live server.
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(TableExists(db, table), $"[{kind}] table {table} should exist.");
            Assert.True(PostgresPartialIndexExists(db, ixName),
                $"[{kind}] partial index {ixName} should exist with a predicate.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CREATE a table carrying an EXPRESSION (functional) index (PostgreSQL). The generator
    // emits `CREATE INDEX ... ON "T" (<expr>)` with the key expression interpolated raw —
    // a distinct diff member (table.ExpressionIndexes) with no prior live coverage. Guard:
    // the CREATE must execute AND the index must be a real expression index
    // (pg_index.indexprs IS NOT NULL). SQL Server rejects expression indexes by design, so
    // this is PostgreSQL (and, separately, SQLite).
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("postgres")]
    public void LiveProvider_Migration_CreateTableWithExpressionIndex_IsValidAndFunctional(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table  = "DdlParity_ExprIdx";
        const string ixName = "IX_DdlParity_ExprIdx_LowerName";

        try
        {
            ExecSafe(db, DropTableDdl(kind, table));

            var t = TableWithExtra(table);
            // Functional index on lower("Name"). The key expression is interpolated verbatim.
            t.ExpressionIndexes.Add(new ExpressionIndexSchema
            {
                Name = ixName,
                ExpressionSql = "lower(\"Name\")",
            });

            var diff = new SchemaDiff();
            diff.AddedTables.Add(t);

            // Must not throw: the expression index DDL has to be valid on a live server.
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            Assert.True(TableExists(db, table), $"[{kind}] table {table} should exist.");
            Assert.True(PostgresExpressionIndexExists(db, ixName),
                $"[{kind}] expression index {ixName} should exist as a functional index.");
        }
        finally
        {
            ExecSafe(cn, DropTableDdl(kind, table));
            db.Dispose();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ALTER COLUMN default rebind (SQL Server): changing a column's DEFAULT must drop the
    // existing system-named DEFAULT constraint (dynamic EXEC — same helper as the drop path)
    // and bind the new one. End-to-end guard: a row inserted without the column afterwards
    // must pick up the NEW default, proving the old constraint was dropped and the new bound.
    // SQL Server only — it is the provider that models defaults as droppable named constraints
    // via dynamic SQL; the other providers alter defaults with plain ALTER COLUMN SET DEFAULT.
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlserver")]
    public void LiveProvider_Migration_AlterColumnDefaultRebind_AppliesNewDefault(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_AlterDefault";

        try
        {
            ResetTable(db, kind, table);
            Exec(db, $"IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE [{table}] " +
                     $"([Id] INT PRIMARY KEY, [Name] NVARCHAR(200) NOT NULL, [Score] INT NOT NULL DEFAULT 7)");

            var tbl    = new TableSchema { Name = table };
            var oldCol = new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "7" };
            var newCol = new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "9" };
            var diff   = new SchemaDiff();
            diff.AlteredColumns.Add((tbl, newCol, oldCol));

            // Drops the existing system-named DEFAULT constraint (dynamic EXEC) and rebinds DEFAULT 9.
            ApplyStatements(db, Generator(kind).GenerateSql(diff).Up);

            // A row inserted without Score must now pick up the NEW default (9).
            Exec(db, $"INSERT INTO [{table}] ([Id],[Name]) VALUES (1,'a')");
            using var q = db.CreateCommand();
            q.CommandText = $"SELECT [Score] FROM [{table}] WHERE [Id]=1";
            Assert.Equal(9, Convert.ToInt32(q.ExecuteScalar()));
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
            ResetTable(db, kind, table);
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

    // DOWN restore of a dropped NOT NULL column (no default) must run on a POPULATED table. A bare
    // ADD COLUMN ... NOT NULL fails there ("column contains null values") — the restore must backfill
    // existing rows with a type-appropriate value. SQLite recreates to do this; PostgreSQL must too.
    [Fact]
    public void LiveProvider_Postgres_DownRestores_DroppedNotNullColumn_OnPopulatedTable()
    {
        var (cn, skip) = Open("postgres");
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DownRestoreNn";

        try
        {
            ExecSafe(db, $"DROP TABLE IF EXISTS \"{table}\" CASCADE");
            Exec(db, $"CREATE TABLE \"{table}\" (\"Id\" INT PRIMARY KEY, \"Name\" VARCHAR(200) NOT NULL, \"Score\" INT NOT NULL)");

            var tableAfter = BaseTable(table);   // Id, Name (Score dropped)
            var scoreCol = new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = false };
            var diff = new SchemaDiff();
            diff.DroppedColumns.Add((tableAfter, scoreCol));
            var sql = Generator("postgres").GenerateSql(diff);

            ApplyStatements(db, sql.Up);                        // drop Score
            Assert.False(ColumnExists(db, table, "Score"));
            Exec(db, $"INSERT INTO \"{table}\" (\"Id\",\"Name\") VALUES (1, 'x')");  // POPULATE

            ApplyStatements(db, sql.Down);                      // BUG: ADD COLUMN NOT NULL fails on the populated table
            Assert.True(ColumnExists(db, table, "Score"), "DOWN: Score should be restored.");
            Assert.False(IsNullable(db, table, "Score"), "restored Score must be NOT NULL");
        }
        finally
        {
            ExecSafe(cn, $"DROP TABLE IF EXISTS \"{table}\" CASCADE");
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
            ResetTable(db, kind, table);
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

    // DOWN migration reverses ADD COLUMN when the added column carries a DEFAULT. On SQL Server the inline
    // DEFAULT binds a separate default-constraint object, and a bare DROP COLUMN in the rollback fails while
    // it depends (Msg 5074). The DOWN must drop the constraint first (as the forward DROP COLUMN already does).
    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void LiveProvider_Migration_DownReverses_AddColumnWithDefault(string kind)
    {
        var (cn, skip) = Open(kind);
        if (skip != null) return;
        var db = cn!;
        const string table = "DdlParity_DownAddDefault";

        if (kind == "sqlite")
        {
            try
            {
                Exec(db, "CREATE TABLE \"_ddl_probe3\" (a INT, b INT)");
                Exec(db, "ALTER TABLE \"_ddl_probe3\" DROP COLUMN b");
                ExecSafe(db, "DROP TABLE \"_ddl_probe3\"");
            }
            catch { db.Dispose(); return; }
        }

        try
        {
            ResetTable(db, kind, table);
            Exec(db, CreateBaseDdl(kind, table));
            var baseTable = BaseTable(table);
            // NOT NULL column with a DEFAULT (mandatory for a NOT NULL add) — the default binds a constraint
            // object on SQL Server that the rollback's DROP COLUMN must drop first.
            var status = new ColumnSchema { Name = "Status", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "7" };
            var diff = new SchemaDiff();
            diff.AddedColumns.Add((baseTable, status));
            var sql = Generator(kind).GenerateSql(diff);

            ApplyStatements(db, sql.Up);
            Assert.True(ColumnExists(db, table, "Status"), $"[{kind}] UP: Status must exist");

            ApplyStatements(db, sql.Down);   // BUG on SqlServer: bare DROP COLUMN failed (Msg 5074)
            Assert.False(ColumnExists(db, table, "Status"),
                $"[{kind}] DOWN: Status should be removed after rollback.");
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

            // SQLite, SQL Server, and PostgreSQL wrap all pending migrations in a single
            // atomic transaction: when the bad migration throws, the full batch rolls back —
            // the good migration's DDL and history row are gone too.
            // MySQL commits each migration individually (DDL implicitly auto-commits), so the
            // good migration's history survives the bad one's failure.
            var expectGoodPresent = kind == "mysql";
            Assert.True(HistoryCount(db, goodVer, kind) == (expectGoodPresent ? 1L : 0L),
                $"[{kind}] Good migration history must {(expectGoodPresent ? "be present" : "be absent (batch rollback)")} after failure.");
            // The bad migration threw; its history row must NOT appear on any provider.
            Assert.True(HistoryCount(db, badVer, kind) == 0L,
                $"[{kind}] Bad migration history entry must NOT be present after failure.");
        }
        finally
        {
            ExecSafe(db, DropTableDdl(kind, table));
            ExecSafe(db, HistoryDeleteSql(kind, $"IN ({goodVer}, {badVer})"));
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
            ExecSafe(db, HistoryDeleteSql(kind, $"= {ver}"));

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
            Assert.True(HistoryCount(db, ver, kind) == 0L,
                $"[{kind}] History must be absent after failed migration.");

            // Second apply: a good migration at the same version — succeeds.
            var goodDdl = CreateBaseDdl(kind, table);
            await MigRunner(kind, db, GoodAssembly(ver, goodDdl)).ApplyMigrationsAsync();

            Assert.True(HistoryCount(db, ver, kind) == 1L,
                $"[{kind}] History must be present after successful re-apply.");
            Assert.True(TableExists(db, table),
                $"[{kind}] Table must exist after successful re-apply.");
        }
        finally
        {
            ExecSafe(db, DropTableDdl(kind, table));
            ExecSafe(db, HistoryDeleteSql(kind, $"= {ver}"));
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
            ResetTable(db, kind, table);
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
            ResetTable(db, kind, table);
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
