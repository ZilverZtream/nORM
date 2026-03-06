using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Gate 4.5+: Parametrized migration replay tests.
/// Constructs SchemaDiff objects directly, calls generators, inspects SQL tokens.
/// SQLite sequences also execute DDL against a live :memory: DB for syntax correctness.
/// </summary>
public class MigrationReplayTests
{
    // ── Provider data ────────────────────────────────────────────────────────

    public static IEnumerable<object[]> Generators() =>
    [
        [new SqliteMigrationSqlGenerator(), "sqlite"],
        [new SqlServerMigrationSqlGenerator(), "sqlserver"],
        [new MySqlMigrationSqlGenerator(), "mysql"],
        [new PostgresMigrationSqlGenerator(), "postgres"],
    ];

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static TableSchema MakeTable(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        t.Columns.AddRange(cols);
        return t;
    }

    private static ColumnSchema IdCol() => new() { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true };
    private static ColumnSchema TextCol(string name, bool nullable = true) => new() { Name = name, ClrType = typeof(string).FullName!, IsNullable = nullable };
    private static ColumnSchema IntCol(string name, bool nullable = true) => new() { Name = name, ClrType = typeof(int).FullName!, IsNullable = nullable };

    private static bool ContainsIgnoreCase(IEnumerable<string> stmts, string token) =>
        stmts.Any(s => s.Contains(token, StringComparison.OrdinalIgnoreCase));

    // ── Sequence 1: Add table ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Seq1_AddTable_GeneratesCreateTable(IMigrationSqlGenerator gen, string _)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable("Blog", IdCol(), TextCol("Title", false)));

        var sql = gen.GenerateSql(diff);
        Assert.True(ContainsIgnoreCase(sql.Up, "CREATE TABLE"), "Up must contain CREATE TABLE");
        Assert.True(sql.Up.Any(s => s.Contains("Blog", StringComparison.OrdinalIgnoreCase)), "Up must reference table name");
    }

    [Fact]
    public void Seq1_AddTable_SqliteExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable("MrBlog", IdCol(), TextCol("Title", false), TextCol("Body")));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        foreach (var stmt in sql.Up)
            ExecuteSql(cn, stmt);

        // Verify table exists
        Assert.Equal(1L, ScalarLong(cn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='MrBlog'"));
    }

    // ── Sequence 2: Add column ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Seq2_AddColumn_GeneratesAlterTable(IMigrationSqlGenerator gen, string _)
    {
        var table = MakeTable("Post", IdCol(), TextCol("Title", false));
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, TextCol("Summary")));

        var sql = gen.GenerateSql(diff);
        Assert.True(ContainsIgnoreCase(sql.Up, "ALTER TABLE"), "Up must contain ALTER TABLE");
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Seq2_AddNonNullableColumn_WithDefault_GeneratesCorrectSql(IMigrationSqlGenerator gen, string _)
    {
        var table = MakeTable("Article", IdCol(), TextCol("Title", false));
        var newCol = new ColumnSchema { Name = "Status", ClrType = typeof(string).FullName!, IsNullable = false, DefaultValue = "'active'" };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        // For non-SQLite providers, non-null columns need a default — they should not throw
        // SQLite also handles this via table recreation
        var sql = gen.GenerateSql(diff);
        Assert.NotNull(sql.Up);
        Assert.NotEmpty(sql.Up);
    }

    [Fact]
    public void Seq2_AddNullableColumn_SqliteExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecuteSql(cn, "CREATE TABLE MrPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL)");

        var table = MakeTable("MrPost", IdCol(), TextCol("Title", false));
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, TextCol("Summary")));  // nullable — safe to add

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        // SQLite add nullable column does simple ALTER TABLE ... ADD COLUMN
        foreach (var stmt in sql.Up)
            ExecuteSql(cn, stmt);

        Assert.Equal(1L, ScalarLong(cn, "SELECT COUNT(*) FROM pragma_table_info('MrPost') WHERE name='Summary'"));
    }

    // ── Sequence 3: Alter column ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Seq3_AlterColumn_ChangeNullability_GeneratesSql(IMigrationSqlGenerator gen, string _)
    {
        var table = MakeTable("Widget", IdCol(), TextCol("Name"));
        var oldCol = TextCol("Name", nullable: true);
        var newCol = TextCol("Name", nullable: false);
        newCol.DefaultValue = "''";

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = gen.GenerateSql(diff);
        Assert.NotEmpty(sql.Up);
    }

    [Fact]
    public void Seq3_AlterColumn_SqliteExecutesWithoutError()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecuteSql(cn, "CREATE TABLE MrWidget (Id INTEGER PRIMARY KEY, Name TEXT)");

        var table = MakeTable("MrWidget", IdCol(), TextCol("Name"));
        var oldCol = TextCol("Name", nullable: true);
        var newCol = TextCol("Name", nullable: false);
        newCol.DefaultValue = "''";

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        // SQLite alter uses table recreation — execute each statement
        foreach (var stmt in sql.Up)
            ExecuteSql(cn, stmt);
    }

    // ── Sequence 4: Add FK ───────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void Seq4_AddForeignKey_GeneratesFkSql(IMigrationSqlGenerator gen, string _)
    {
        var postTable = MakeTable("Comment", IdCol(), IntCol("PostId", false), TextCol("Body"));
        var fk = new ForeignKeySchema
        {
            ConstraintName = "FK_Comment_Post",
            DependentColumns = ["PostId"],
            PrincipalTable = "Post",
            PrincipalColumns = ["Id"],
            OnDelete = "NO ACTION"
        };
        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((postTable, fk));

        var sql = gen.GenerateSql(diff);
        Assert.NotEmpty(sql.Up);
        // All providers should reference the FK in some way
        Assert.True(
            ContainsIgnoreCase(sql.Up, "FOREIGN KEY") ||
            ContainsIgnoreCase(sql.Up, "REFERENCES") ||
            sql.Up.Any(s => s.Contains("Comment", StringComparison.OrdinalIgnoreCase)),
            "FK SQL must reference FOREIGN KEY or REFERENCES or the table name");
    }

    // ── Sequence 5: Multi-step replay on SQLite ──────────────────────────────

    [Fact]
    public void Seq5_MultiStep_SqliteReplay_AllStepsSucceed()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var gen = new SqliteMigrationSqlGenerator();

        // Step A: add table
        {
            var diff = new SchemaDiff();
            diff.AddedTables.Add(MakeTable("MrAuthor", IdCol(), TextCol("Name", false)));
            foreach (var s in gen.GenerateSql(diff).Up) ExecuteSql(cn, s);
            Assert.Equal(1L, ScalarLong(cn, "SELECT COUNT(*) FROM sqlite_master WHERE name='MrAuthor'"));
        }

        // Step B: add column
        {
            var table = MakeTable("MrAuthor", IdCol(), TextCol("Name", false));
            var diff = new SchemaDiff();
            diff.AddedColumns.Add((table, TextCol("Bio")));
            foreach (var s in gen.GenerateSql(diff).Up) ExecuteSql(cn, s);
            Assert.Equal(1L, ScalarLong(cn, "SELECT COUNT(*) FROM pragma_table_info('MrAuthor') WHERE name='Bio'"));
        }

        // Step C: alter column nullability
        {
            var table = MakeTable("MrAuthor", IdCol(), TextCol("Name", false), TextCol("Bio"));
            var oldCol = TextCol("Bio", nullable: true);
            var newCol = TextCol("Bio", nullable: false); newCol.DefaultValue = "''";
            var diff = new SchemaDiff();
            diff.AlteredColumns.Add((table, newCol, oldCol));
            foreach (var s in gen.GenerateSql(diff).Up) ExecuteSql(cn, s);
        }

        // Step D: drop column
        {
            var table = MakeTable("MrAuthor", IdCol(), TextCol("Name", false), TextCol("Bio", false));
            var diff = new SchemaDiff();
            diff.DroppedColumns.Add((table, TextCol("Bio", false)));
            foreach (var s in gen.GenerateSql(diff).Up) ExecuteSql(cn, s);
            Assert.Equal(0L, ScalarLong(cn, "SELECT COUNT(*) FROM pragma_table_info('MrAuthor') WHERE name='Bio'"));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void ExecuteSql(SqliteConnection cn, string sql)
    {
        if (string.IsNullOrWhiteSpace(sql) || sql.TrimStart().StartsWith("--")) return;
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
