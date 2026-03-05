using System;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for MySqlMigrationSqlGenerator covering PRV-1 and MIG-1 findings.
/// </summary>
public class MySqlMigrationSqlGeneratorTests
{
    private static TableSchema BuildTable(string name, params ColumnSchema[] columns)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in columns) t.Columns.Add(c);
        return t;
    }

    private static readonly MySqlMigrationSqlGenerator Gen = new();

    // ─── PRV-1: Identifier escaping with backtick ────────────────────────────

    [Fact]
    public void CreateTable_EscapesTableNameWithBacktick()
    {
        var table = BuildTable("He`llo",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        // The escaped table name should double the embedded backtick: `He``llo`
        Assert.Contains("`He``llo`", sql.Up[0]);
    }

    [Fact]
    public void CreateTable_EscapesColumnNameWithBacktick()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "co`l", ClrType = typeof(int).FullName!, IsNullable = false });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        Assert.Contains("`co``l`", sql.Up[0]);
    }

    [Fact]
    public void CreateIndex_EscapesIdentifiers()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = true, IndexName = "ix`val" });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        var indexStmt = sql.Up.First(s => s.StartsWith("CREATE INDEX"));
        Assert.Contains("`ix``val`", indexStmt);
    }

    // ─── MIG-1: NOT NULL + DefaultValue ──────────────────────────────────────

    [Fact]
    public void AddColumn_NotNull_WithDefaultValue_EmitsDefault()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false });
        var newCol = new ColumnSchema { Name = "Status", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0" };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var sql = Gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("NOT NULL DEFAULT 0", sql.Up[0]);
    }

    [Fact]
    public void AddColumn_NotNull_WithoutDefaultValue_ThrowsInvalidOperationException()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false });
        var newCol = new ColumnSchema { Name = "Status", ClrType = typeof(int).FullName!, IsNullable = false };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var ex = Assert.Throws<InvalidOperationException>(() => Gen.GenerateSql(diff));
        Assert.Contains("Status", ex.Message);
        Assert.Contains("DefaultValue", ex.Message);
    }

    [Fact]
    public void AddColumn_Nullable_DoesNotRequireDefaultValue()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false });
        var newCol = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var sql = Gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.DoesNotContain("DEFAULT", sql.Up[0]);
    }

    [Fact]
    public void DropTable_EscapesTableName()
    {
        var table = BuildTable("T`able",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false });
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        Assert.Contains("`T``able`", sql.Up[0]);
    }
}
