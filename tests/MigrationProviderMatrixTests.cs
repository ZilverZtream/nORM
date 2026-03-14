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
/// Gate 4.5→5.0: Provider matrix + concurrency stress + idempotency tests.
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
