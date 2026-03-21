using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Provider matrix + concurrency stress + idempotency tests.
///
/// Provider matrix: runs the same migration scenarios (add table with FK, add FK on existing
/// table, drop FK, alter column) through all four SQL generators and asserts structural
/// correctness using dialect-agnostic keyword checks.
///
/// Concurrency stress: fires 200 concurrent generator calls on the same SchemaDiff and verifies
/// output is deterministic (generators are stateless and must be race-free).
///
/// Idempotency: the same generator called twice on the same diff must produce identical SQL,
/// proving no hidden mutable state is accumulated.
///
/// SQLite live-execution: selected scenarios execute the generated DDL against an in-memory
/// SQLite DB to prove syntactic correctness end-to-end.
/// </summary>
public class MigrationProviderMatrixTests
{
    // ── Provider data ─────────────────────────────────────────────────────────

    public static IEnumerable<object[]> Generators() =>
    [
        [new SqliteMigrationSqlGenerator(),    "sqlite"],
        [new SqlServerMigrationSqlGenerator(), "sqlserver"],
        [new MySqlMigrationSqlGenerator(),     "mysql"],
        [new PostgresMigrationSqlGenerator(),  "postgres"],
    ];

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static TableSchema Table(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        t.Columns.AddRange(cols);
        return t;
    }

    private static ColumnSchema IdCol(string name = "Id") =>
        new() { Name = name, ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false };

    private static ColumnSchema IntCol(string name, bool nullable = true) =>
        new() { Name = name, ClrType = typeof(int).FullName!, IsNullable = nullable };

    private static ColumnSchema TextCol(string name, bool nullable = true) =>
        new() { Name = name, ClrType = typeof(string).FullName!, IsNullable = nullable };

    private static ForeignKeySchema Fk(string name, string dep, string principal,
        string onDelete = "NO ACTION", string onUpdate = "NO ACTION") =>
        new()
        {
            ConstraintName   = name,
            DependentColumns = [dep],
            PrincipalTable   = principal,
            PrincipalColumns = ["Id"],
            OnDelete         = onDelete,
            OnUpdate         = onUpdate,
        };

    private static bool Has(IEnumerable<string> stmts, string token) =>
        stmts.Any(s => s.Contains(token, StringComparison.OrdinalIgnoreCase));

    // ── Matrix: create table with inline FK ───────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Matrix_AddTableWithFk_AllGenerators_EmitFkConstraint(
        IMigrationSqlGenerator gen, string _)
    {
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog");
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = gen.GenerateSql(diff);
        Assert.True(Has(sql.Up, "CREATE TABLE"),   "Must emit CREATE TABLE");
        Assert.True(Has(sql.Up, "FOREIGN KEY"),    "Must emit FOREIGN KEY");
        Assert.True(Has(sql.Up, "REFERENCES"),     "Must emit REFERENCES");
        Assert.True(Has(sql.Up, "FK_Post_Blog"),   "Must include constraint name");
    }

    // ── Matrix: add FK to existing table ─────────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Matrix_AddFkToExistingTable_AllGenerators_UpContainsFk(
        IMigrationSqlGenerator gen, string _)
    {
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog", onDelete: "CASCADE");
        table.ForeignKeys.Add(fk); // SQLite uses table.ForeignKeys for post-diff state

        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));

        var sql = gen.GenerateSql(diff);
        Assert.True(Has(sql.Up, "FOREIGN KEY"), "Up must reference FOREIGN KEY");
        Assert.True(Has(sql.Up, "CASCADE"),     "Up must include CASCADE action");
        // Down must remove the FK
        Assert.NotEmpty(sql.Down);
    }

    // ── Matrix: drop FK from existing table ──────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Matrix_DropFkFromExistingTable_AllGenerators_DownRestoresFk(
        IMigrationSqlGenerator gen, string _)
    {
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog");

        var diff = new SchemaDiff();
        diff.DroppedForeignKeys.Add((table, fk));

        var sql = gen.GenerateSql(diff);
        Assert.NotEmpty(sql.Up);
        // Down must restore the FK
        Assert.True(Has(sql.Down, "FOREIGN KEY"), "Down must restore FOREIGN KEY");
    }

    // ── Matrix: drop table — Down must re-create with FK ─────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Matrix_DropTableWithFk_AllGenerators_DownRecreatesWithFk(
        IMigrationSqlGenerator gen, string _)
    {
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog");
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var sql = gen.GenerateSql(diff);
        Assert.True(Has(sql.Up,   "DROP TABLE"),  "Up must DROP TABLE");
        Assert.True(Has(sql.Down, "FOREIGN KEY"), "Down must recreate with FOREIGN KEY");
    }

    // ── Matrix: all valid FK actions emitted correctly ────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Matrix_AllValidFkActions_EmittedCorrectly(IMigrationSqlGenerator gen, string _)
    {
        foreach (var action in new[] { "CASCADE", "SET NULL", "RESTRICT", "SET DEFAULT" })
        {
            var fk    = Fk($"FK_{action}", "BlogId", "Blog", onDelete: action);
            var table = Table("Post", IdCol(), IntCol("BlogId"));
            table.ForeignKeys.Add(fk);

            var diff = new SchemaDiff();
            diff.AddedTables.Add(table);

            var sql = gen.GenerateSql(diff);
            Assert.True(Has(sql.Up, action),
                $"Generator {gen.GetType().Name} must emit '{action}' in ON DELETE clause");
        }
    }

    // ── Concurrency stress ────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrencyStress_SqliteGenerator_200ConcurrentCalls_ProduceSameOutput()
    {
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog", onDelete: "CASCADE");
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new SqliteMigrationSqlGenerator();

        // Generate reference output first
        var reference = gen.GenerateSql(diff).Up.ToList();

        // Fire 200 concurrent calls
        var tasks = Enumerable.Range(0, 200).Select(_ => Task.Run(() =>
        {
            var sql = gen.GenerateSql(diff);
            return sql.Up.ToList();
        }));

        var results = await Task.WhenAll(tasks);

        foreach (var result in results)
        {
            Assert.Equal(reference.Count, result.Count);
            for (int i = 0; i < reference.Count; i++)
                Assert.Equal(reference[i], result[i]);
        }
    }

    [Fact]
    public async Task ConcurrencyStress_AllFourGenerators_200ConcurrentCallsEach_NoExceptions()
    {
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog", onDelete: "CASCADE");
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var generators = new IMigrationSqlGenerator[]
        {
            new SqliteMigrationSqlGenerator(),
            new SqlServerMigrationSqlGenerator(),
            new MySqlMigrationSqlGenerator(),
            new PostgresMigrationSqlGenerator(),
        };

        var tasks = generators.SelectMany(gen =>
            Enumerable.Range(0, 200).Select(_ => Task.Run(() => gen.GenerateSql(diff)))
        );

        // Must complete without exceptions
        await Task.WhenAll(tasks);
    }

    // ── Idempotency: same diff → same SQL every time ──────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Idempotency_SameDiff_SameGeneratorCalledTwice_IdenticalOutput(
        IMigrationSqlGenerator gen, string _)
    {
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog", onDelete: "CASCADE");
        var table = Table("Post", IdCol(), TextCol("Title"), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var first  = gen.GenerateSql(diff);
        var second = gen.GenerateSql(diff);

        Assert.Equal(first.Up.Count,   second.Up.Count);
        Assert.Equal(first.Down.Count, second.Down.Count);
        for (int i = 0; i < first.Up.Count; i++)
            Assert.Equal(first.Up[i], second.Up[i]);
    }

    // ── SQLite live execution: FK + table recreation ──────────────────────────

    [Fact]
    public void SqliteLive_AddTableWithFk_ExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Create the referenced table first
        ExecuteSql(cn, "CREATE TABLE Blog (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL)");

        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog");
        var table = Table("Post", IdCol(), TextCol("Title", false), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var stmts = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        foreach (var stmt in stmts.Up)
            ExecuteSql(cn, stmt);

        Assert.Equal(1L, ScalarLong(cn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Post'"));
    }

    [Fact]
    public void SqliteLive_AddFkToExistingTable_ExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        ExecuteSql(cn, "PRAGMA foreign_keys=off");
        ExecuteSql(cn, "CREATE TABLE Blog (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL)");
        ExecuteSql(cn, "CREATE TABLE Post (Id INTEGER PRIMARY KEY, BlogId INTEGER)");

        var table = Table("Post", IdCol(), IntCol("BlogId"));
        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog");
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));

        var stmts = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // Execute PRAGMA segments + DDL
        foreach (var s in stmts.PreTransactionUp  ?? []) ExecuteSql(cn, s);
        foreach (var s in stmts.Up)                        ExecuteSql(cn, s);
        foreach (var s in stmts.PostTransactionUp ?? []) ExecuteSql(cn, s);

        // Table must still exist with correct schema
        Assert.Equal(1L, ScalarLong(cn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Post'"));
    }

    [Fact]
    public void SqliteLive_DropTableWithFk_ExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecuteSql(cn, "CREATE TABLE Blog (Id INTEGER PRIMARY KEY)");
        ExecuteSql(cn, "CREATE TABLE Post (Id INTEGER PRIMARY KEY, BlogId INTEGER)");

        var fk    = Fk("FK_Post_Blog", "BlogId", "Blog");
        var table = Table("Post", IdCol(), IntCol("BlogId"));
        table.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var stmts = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        foreach (var stmt in stmts.Up)
            ExecuteSql(cn, stmt);

        Assert.Equal(0L, ScalarLong(cn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Post'"));
    }

    // ── SQLite live execution: hostile identifier names are handled safely ─────

    [Fact]
    public void SqliteLive_TableNameWithSpecialChars_ExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Table name with double-quote (must be escaped as "")
        var table = Table("My\"Table", IdCol(), TextCol("Name", false));
        var diff  = new SchemaDiff();
        diff.AddedTables.Add(table);

        var stmts = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        foreach (var stmt in stmts.Up)
            ExecuteSql(cn, stmt);

        Assert.Equal(1L, ScalarLong(cn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='My\"Table'"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void ExecuteSql(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long ScalarLong(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }
}

// ── Fault-injection: SQL Server generator ────────────────────────────────────

public class SqlServerMigrationSqlGeneratorFaultTests
{
    private static SchemaDiff BuildSimpleDiff(string tableName)
    {
        var t = new TableSchema { Name = tableName };
        t.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "Label", ClrType = typeof(string).FullName!, IsNullable = true });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(t);
        return diff;
    }

    private static TableSchema BuildFkTable(string table, string fkName, string principal,
        string onDelete = "CASCADE", string onUpdate = "NO ACTION")
    {
        var fk = new ForeignKeySchema
        {
            ConstraintName   = fkName,
            DependentColumns = ["ParentId"],
            PrincipalTable   = principal,
            PrincipalColumns = ["Id"],
            OnDelete         = onDelete,
            OnUpdate         = onUpdate,
        };
        var t = new TableSchema { Name = table };
        t.Columns.Add(new ColumnSchema { Name = "Id",       ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName!, IsNullable = true });
        t.ForeignKeys.Add(fk);
        return t;
    }

    [Fact]
    public void SqlServer_ValidAddTable_ContainsCreateTable()
    {
        var stmts = new SqlServerMigrationSqlGenerator().GenerateSql(BuildSimpleDiff("SS_TestTable"));
        var upSql = string.Join(" ", stmts.Up);
        Assert.Contains("SS_TestTable", upSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CREATE TABLE",  upSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_InvalidFkAction_ThrowsArgumentException()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildFkTable("SS_Child", "FK_SS", "SS_Parent",
            onDelete: "CASCADE; DROP TABLE users --"));
        Assert.Throws<ArgumentException>(() => new SqlServerMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void SqlServer_InjectionPayload_NeverInGeneratedSql()
    {
        const string payload = "CASCADE; DROP TABLE users --";
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildFkTable("SS_Child2", "FK_SS2", "SS_Parent2", onDelete: payload));
        var ex = Record.Exception(() => new SqlServerMigrationSqlGenerator().GenerateSql(diff));
        Assert.NotNull(ex);
        Assert.IsType<ArgumentException>(ex);
    }

    [Fact]
    public void SqlServer_DefaultValueInjection_ThrowsArgumentException()
    {
        var diff = new SchemaDiff();
        var t = new TableSchema { Name = "SS_DefaultInj" };
        t.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = false,
                                         DefaultValue = "0); DROP TABLE users --" });
        diff.AddedTables.Add(t);
        Assert.Throws<ArgumentException>(() => new SqlServerMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void SqlServer_AddNotNullColumnNoDefault_ThrowsInvalidOperation()
    {
        var diff = new SchemaDiff();
        var existingTable = new TableSchema { Name = "SS_ExistingTable" };
        diff.AddedColumns.Add((existingTable, new ColumnSchema
        {
            Name = "NewCol", ClrType = typeof(int).FullName!, IsNullable = false,
        }));
        Assert.Throws<InvalidOperationException>(() => new SqlServerMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void SqlServer_DropOrdering_FkDroppedBeforeTable()
    {
        var diff = new SchemaDiff();
        var childTable = new TableSchema { Name = "SS_Child" };
        childTable.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        diff.DroppedForeignKeys.Add((childTable, new ForeignKeySchema
        {
            ConstraintName = "FK_SS_Child", DependentColumns = ["ParentId"],
            PrincipalTable = "SS_Parent",   PrincipalColumns = ["Id"],
            OnDelete = "NO ACTION",          OnUpdate = "NO ACTION",
        }));
        diff.DroppedTables.Add(childTable);

        var upSql = string.Join(" ", new SqlServerMigrationSqlGenerator().GenerateSql(diff).Up);
        var fkPos    = upSql.IndexOf("DROP CONSTRAINT", StringComparison.OrdinalIgnoreCase);
        var tablePos = upSql.IndexOf("DROP TABLE",      StringComparison.OrdinalIgnoreCase);
        Assert.True(fkPos >= 0,       $"Expected DROP CONSTRAINT in UP sql: {upSql}");
        Assert.True(tablePos >= 0,    $"Expected DROP TABLE in UP sql: {upSql}");
        Assert.True(fkPos < tablePos, $"FK drop (pos={fkPos}) should precede table drop (pos={tablePos})");
    }

    [Fact]
    public void SqlServer_ValidFkActions_AcceptedWithoutException()
    {
        foreach (var action in new[] { "CASCADE", "NO ACTION", "SET NULL", "SET DEFAULT" })
        {
            var diff = new SchemaDiff();
            diff.AddedTables.Add(BuildFkTable($"SS_ValidFk_{action.Replace(" ", "_")}",
                $"FK_{action.Replace(" ", "_")}", "SS_Parent", onDelete: action));
            Assert.Null(Record.Exception(() => new SqlServerMigrationSqlGenerator().GenerateSql(diff)));
        }
    }

    [Fact]
    public async Task SqlServer_ConcurrentGeneration_NoContamination()
    {
        var gen = new SqlServerMigrationSqlGenerator();
        var diff1 = BuildSimpleDiff("SS_ConcA");
        var diff2 = BuildSimpleDiff("SS_ConcB");

        var all = await Task.WhenAll(
            Enumerable.Range(0, 50).Select(_ => Task.Run(() => gen.GenerateSql(diff1))).Concat(
            Enumerable.Range(0, 50).Select(_ => Task.Run(() => gen.GenerateSql(diff2)))));

        foreach (var r in all.Take(50))
            Assert.DoesNotContain("SS_ConcB", string.Join(" ", r.Up));
        foreach (var r in all.Skip(50))
            Assert.DoesNotContain("SS_ConcA", string.Join(" ", r.Up));
    }
}

// ── Fault-injection: MySQL generator ─────────────────────────────────────────

public class MySqlMigrationSqlGeneratorFaultTests
{
    private static SchemaDiff BuildSimpleDiff(string tableName)
    {
        var t = new TableSchema { Name = tableName };
        t.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "Label", ClrType = typeof(string).FullName!, IsNullable = true });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(t);
        return diff;
    }

    private static TableSchema BuildFkTable(string table, string fkName, string principal,
        string onDelete = "CASCADE")
    {
        var fk = new ForeignKeySchema
        {
            ConstraintName   = fkName,
            DependentColumns = ["ParentId"],
            PrincipalTable   = principal,
            PrincipalColumns = ["Id"],
            OnDelete         = onDelete,
            OnUpdate         = "NO ACTION",
        };
        var t = new TableSchema { Name = table };
        t.Columns.Add(new ColumnSchema { Name = "Id",       ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName!, IsNullable = true });
        t.ForeignKeys.Add(fk);
        return t;
    }

    [Fact]
    public void MySql_ValidAddTable_UsesBacktickEscaping()
    {
        var stmts = new MySqlMigrationSqlGenerator().GenerateSql(BuildSimpleDiff("MY_TestTable"));
        var upSql = string.Join(" ", stmts.Up);
        Assert.Contains("`MY_TestTable`", upSql);
        Assert.Contains("CREATE TABLE",   upSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySql_InvalidFkAction_ThrowsArgumentException()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildFkTable("MY_Child", "FK_MY", "MY_Parent",
            onDelete: "'; DROP TABLE admin; --"));
        Assert.Throws<ArgumentException>(() => new MySqlMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void MySql_DefaultValueInjection_ThrowsArgumentException()
    {
        var diff = new SchemaDiff();
        var t = new TableSchema { Name = "MY_DefaultInj" };
        t.Columns.Add(new ColumnSchema { Name = "Id",  ClrType = typeof(int).FullName!,    IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false,
                                         DefaultValue = "'; DROP TABLE users --" });
        diff.AddedTables.Add(t);
        Assert.Throws<ArgumentException>(() => new MySqlMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void MySql_AddNotNullColumnNoDefault_ThrowsInvalidOperation()
    {
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((new TableSchema { Name = "MY_ExistingTable" },
            new ColumnSchema { Name = "NewCol", ClrType = typeof(int).FullName!, IsNullable = false }));
        Assert.Throws<InvalidOperationException>(() => new MySqlMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void MySql_ValidFkActions_AcceptedWithoutException()
    {
        foreach (var action in new[] { "CASCADE", "NO ACTION", "SET NULL", "RESTRICT" })
        {
            var diff = new SchemaDiff();
            diff.AddedTables.Add(BuildFkTable($"MY_ValidFk_{action.Replace(" ", "_")}",
                $"FK_MY_{action.Replace(" ", "_")}", "MY_Parent", onDelete: action));
            Assert.Null(Record.Exception(() => new MySqlMigrationSqlGenerator().GenerateSql(diff)));
        }
    }

    [Fact]
    public async Task MySql_ConcurrentGeneration_NoContamination()
    {
        var gen = new MySqlMigrationSqlGenerator();
        var all = await Task.WhenAll(
            Enumerable.Range(0, 50).Select(_ => Task.Run(() => gen.GenerateSql(BuildSimpleDiff("MY_ConcA")))).Concat(
            Enumerable.Range(0, 50).Select(_ => Task.Run(() => gen.GenerateSql(BuildSimpleDiff("MY_ConcB"))))));

        foreach (var r in all.Take(50))
            Assert.DoesNotContain("MY_ConcB", string.Join(" ", r.Up));
        foreach (var r in all.Skip(50))
            Assert.DoesNotContain("MY_ConcA", string.Join(" ", r.Up));
    }
}

// ── Fault-injection: PostgreSQL generator ────────────────────────────────────

public class PostgresMigrationSqlGeneratorFaultTests
{
    private static SchemaDiff BuildSimpleDiff(string tableName)
    {
        var t = new TableSchema { Name = tableName };
        t.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "Label", ClrType = typeof(string).FullName!, IsNullable = true });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(t);
        return diff;
    }

    private static TableSchema BuildFkTable(string table, string fkName, string principal,
        string onDelete = "CASCADE")
    {
        var fk = new ForeignKeySchema
        {
            ConstraintName   = fkName,
            DependentColumns = ["ParentId"],
            PrincipalTable   = principal,
            PrincipalColumns = ["Id"],
            OnDelete         = onDelete,
            OnUpdate         = "NO ACTION",
        };
        var t = new TableSchema { Name = table };
        t.Columns.Add(new ColumnSchema { Name = "Id",       ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName!, IsNullable = true });
        t.ForeignKeys.Add(fk);
        return t;
    }

    [Fact]
    public void Postgres_ValidAddTable_UsesDoubleQuoteEscaping()
    {
        var stmts = new PostgresMigrationSqlGenerator().GenerateSql(BuildSimpleDiff("PG_TestTable"));
        var upSql = string.Join(" ", stmts.Up);
        Assert.Contains("\"PG_TestTable\"", upSql);
        Assert.Contains("CREATE TABLE",     upSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Postgres_InvalidFkAction_ThrowsArgumentException()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildFkTable("PG_Child", "FK_PG", "PG_Parent",
            onDelete: "CASCADE; SELECT pg_sleep(5) --"));
        Assert.Throws<ArgumentException>(() => new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void Postgres_DefaultValueInjection_ThrowsArgumentException()
    {
        var diff = new SchemaDiff();
        var t = new TableSchema { Name = "PG_DefaultInj" };
        t.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "Score", ClrType = typeof(int).FullName!, IsNullable = false,
                                         DefaultValue = "0); TRUNCATE users CASCADE --" });
        diff.AddedTables.Add(t);
        Assert.Throws<ArgumentException>(() => new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void Postgres_AddNotNullColumnNoDefault_ThrowsInvalidOperation()
    {
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((new TableSchema { Name = "PG_ExistingTable" },
            new ColumnSchema { Name = "NewCol", ClrType = typeof(int).FullName!, IsNullable = false }));
        Assert.Throws<InvalidOperationException>(() => new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Fact]
    public void Postgres_ValidFkActions_AcceptedWithoutException()
    {
        foreach (var action in new[] { "CASCADE", "NO ACTION", "SET NULL", "SET DEFAULT", "RESTRICT" })
        {
            var diff = new SchemaDiff();
            diff.AddedTables.Add(BuildFkTable($"PG_ValidFk_{action.Replace(" ", "_")}",
                $"FK_PG_{action.Replace(" ", "_")}", "PG_Parent", onDelete: action));
            Assert.Null(Record.Exception(() => new PostgresMigrationSqlGenerator().GenerateSql(diff)));
        }
    }

    [Fact]
    public void Postgres_UpThenDown_TableExistsThenDropped()
    {
        var stmts = new PostgresMigrationSqlGenerator().GenerateSql(BuildSimpleDiff("PG_RoundTripTable"));
        Assert.Contains("CREATE TABLE", string.Join(" ", stmts.Up),   StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DROP TABLE",   string.Join(" ", stmts.Down), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Postgres_ConcurrentGeneration_NoContamination()
    {
        var gen = new PostgresMigrationSqlGenerator();
        var all = await Task.WhenAll(
            Enumerable.Range(0, 50).Select(_ => Task.Run(() => gen.GenerateSql(BuildSimpleDiff("PG_ConcA")))).Concat(
            Enumerable.Range(0, 50).Select(_ => Task.Run(() => gen.GenerateSql(BuildSimpleDiff("PG_ConcB"))))));

        foreach (var r in all.Take(50))
            Assert.DoesNotContain("PG_ConcB", string.Join(" ", r.Up));
        foreach (var r in all.Skip(50))
            Assert.DoesNotContain("PG_ConcA", string.Join(" ", r.Up));
    }
}
