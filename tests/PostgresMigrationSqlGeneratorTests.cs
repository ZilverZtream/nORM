using System;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for PostgresMigrationSqlGenerator covering SG-1, PRV-1, and MIG-1 findings.
/// </summary>
public class PostgresMigrationSqlGeneratorTests
{
    private static TableSchema BuildTable(string name, params ColumnSchema[] columns)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in columns) t.Columns.Add(c);
        return t;
    }

    private static readonly PostgresMigrationSqlGenerator Gen = new();

    // ─── SG-1: ALTER COLUMN must emit separate TYPE and NOT NULL statements ───

    [Fact]
    public void AlteredColumn_TypeOnly_EmitsTypeStatement_NoNullabilityStatement()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true });
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(int).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = Gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("TYPE", sql.Up[0]);
        Assert.DoesNotContain("NOT NULL", sql.Up[0]);
        Assert.DoesNotContain("NULL", sql.Up[0].Replace("NOT NULL", ""));
        Assert.Single(sql.Down);
        Assert.Contains("TYPE", sql.Down[0]);
    }

    [Fact]
    public void AlteredColumn_NullabilityTrueToFalse_EmitsSetNotNull()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false });
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = Gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("SET NOT NULL", sql.Up[0]);
        Assert.DoesNotContain("TYPE", sql.Up[0]);
    }

    [Fact]
    public void AlteredColumn_NullabilityFalseToTrue_EmitsDropNotNull()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true });
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = Gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("DROP NOT NULL", sql.Up[0]);
        Assert.DoesNotContain("TYPE", sql.Up[0]);
    }

    [Fact]
    public void AlteredColumn_TypeAndNullability_EmitsTwoStatements()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false });
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(int).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = Gen.GenerateSql(diff);

        Assert.Equal(2, sql.Up.Count);
        Assert.Contains(sql.Up, s => s.Contains("TYPE"));
        Assert.Contains(sql.Up, s => s.Contains("SET NOT NULL"));
        // Down should also have 2 statements
        Assert.Equal(2, sql.Down.Count);
    }

    [Fact]
    public void AlteredColumn_DownMigration_ReversesNullabilityCorrectly()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false });
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = Gen.GenerateSql(diff);

        // Up: SET NOT NULL; Down: DROP NOT NULL
        Assert.Contains("SET NOT NULL", sql.Up[0]);
        Assert.Contains("DROP NOT NULL", sql.Down[0]);
    }

    // ─── PRV-1: Identifier escaping ──────────────────────────────────────────

    [Fact]
    public void CreateTable_EscapesTableNameWithDoubleQuote()
    {
        var table = BuildTable("He\"llo",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        // The escaped table name should double the embedded quote: "He""llo"
        Assert.Contains("\"He\"\"llo\"", sql.Up[0]);
    }

    [Fact]
    public void CreateTable_EscapesColumnNameWithDoubleQuote()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "co\"l", ClrType = typeof(int).FullName!, IsNullable = false });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        Assert.Contains("\"co\"\"l\"", sql.Up[0]);
    }

    [Fact]
    public void CreateIndex_EscapesIndexNameAndTableName()
    {
        var table = BuildTable("T",
            new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = true, IndexName = "ix\"val" });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = Gen.GenerateSql(diff);

        var indexStmt = sql.Up.First(s => s.StartsWith("CREATE INDEX"));
        Assert.Contains("\"ix\"\"val\"", indexStmt);
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
        Assert.Contains("NULL", sql.Up[0]);
        Assert.DoesNotContain("DEFAULT", sql.Up[0]);
    }
}
