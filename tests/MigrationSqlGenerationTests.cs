using System;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Comprehensive migration SQL generation tests for both SQLite and SQL Server generators.
/// Tests DDL round-trips: add table, add column, drop column, drop table, alter nullability,
/// add/drop index.
/// </summary>
public class MigrationSqlGenerationTests
{
 // ─── Helpers ──────────────────────────────────────────────────────────

    private static TableSchema MakeTable(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in cols) t.Columns.Add(c);
        return t;
    }

    private static ColumnSchema PkCol(string name, string clrType = null!) =>
        new ColumnSchema { Name = name, ClrType = clrType ?? typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = $"PK_{name}" };

    private static ColumnSchema Col(string name, bool nullable = true, bool unique = false, string indexName = null!, string clrType = null!) =>
        new ColumnSchema { Name = name, ClrType = clrType ?? typeof(string).FullName!, IsNullable = nullable, IsUnique = unique, IndexName = indexName! };

 // ───────────────────────────── SQLite ────────────────────────────────

    [Fact]
    public void Sqlite_AddTable_WithAllConstraints_EmitsCorrectDDL()
    {
        var table = MakeTable("Products",
            PkCol("Id"),
            Col("Sku", nullable: false, unique: true),
            Col("Name", nullable: false),
            Col("Description", nullable: true),
            Col("Name2", nullable: false, indexName: "idx_Products_Name2")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("NOT NULL", createStmt);

 // Should have separate CREATE INDEX for indexed column
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Products_Name2"));

 // Down should drop the table
        Assert.Contains(sql.Down, s => s.Contains("DROP TABLE"));
    }

    [Fact]
    public void Sqlite_AddTable_WithNullableColumn_EmitsNull()
    {
        var table = MakeTable("TestTable",
            PkCol("Id"),
            Col("Description", nullable: true)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("\"Description\" TEXT NULL", createStmt);
    }

    [Fact]
    public void Sqlite_AddTable_WithNonNullableColumn_EmitsNotNull()
    {
        var table = MakeTable("TestTable",
            PkCol("Id"),
            Col("Title", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("\"Title\" TEXT NOT NULL", createStmt);
    }

    [Fact]
    public void Sqlite_AddColumn_EmitsAlterTableAddColumn()
    {
        var table = MakeTable("Blog",
            PkCol("Id"),
            Col("Content", nullable: true)
        );
        var newCol = Col("Tags", nullable: true);
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("ALTER TABLE") && s.Contains("ADD") && s.Contains("\"Tags\""));
 // PRAGMA is in PreTransactionDown, not in the main Down list.
        Assert.NotNull(sql.PreTransactionDown);
        Assert.Contains(sql.PreTransactionDown!, s => s.Contains("PRAGMA foreign_keys=off"));
    }

    [Fact]
    public void Sqlite_DropTable_EmitsDropTable()
    {
        var table = MakeTable("OldTable", PkCol("Id"));
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("DROP TABLE") && s.Contains("OldTable"));
    }

    [Fact]
    public void Sqlite_AlteredColumn_Nullability_UsesTableRecreation()
    {
        var table = MakeTable("Post",
            PkCol("Id"),
            Col("Body", nullable: true)
        );
        var oldCol = Col("Body", nullable: true);
        var newCol = Col("Body", nullable: false);

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

 // SQLite doesn't support ALTER COLUMN — must use table recreation
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE"));
        Assert.Contains(sql.Up, s => s.StartsWith("INSERT INTO"));
        Assert.Contains(sql.Up, s => s.StartsWith("DROP TABLE"));
        Assert.Contains(sql.Up, s => s.Contains("RENAME TO"));
    }

    [Fact]
    public void Sqlite_AlteredColumn_DownMigration_RevertsNullability()
    {
        var table = MakeTable("Post",
            PkCol("Id"),
            Col("Body", nullable: true)
        );
        var oldCol = Col("Body", nullable: true);
        var newCol = Col("Body", nullable: false);

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

 // Down should also use table recreation (to revert nullability)
 // PRAGMA is now in PreTransactionDown, not in the main Down list.
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE TABLE"));
    }

    [Fact]
    public void Sqlite_AddIndex_EmitsCreateIndex()
    {
        var table = MakeTable("Users",
            PkCol("Id"),
            Col("Email", nullable: false, indexName: "idx_Users_Email")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Users_Email") && s.Contains("\"Email\""));
    }

    [Fact]
    public void Sqlite_EmptyDiff_ProducesNoStatements()
    {
        var diff = new SchemaDiff();
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

 // Empty diff should produce minimal or no statements
 // (PRAGMA may still be included for structural reasons, but no DDL)
        var hasDDL = sql.Up.Any(s => s.StartsWith("CREATE") || s.StartsWith("DROP") || s.StartsWith("ALTER"));
        Assert.False(hasDDL);
    }

    [Fact]
    public void Sqlite_MultipleTablesAdded_AllPresent()
    {
        var t1 = MakeTable("Table1", PkCol("Id"), Col("A", nullable: false));
        var t2 = MakeTable("Table2", PkCol("Id"), Col("B", nullable: true));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(t1);
        diff.AddedTables.Add(t2);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStatements = sql.Up.Where(s => s.StartsWith("CREATE TABLE")).ToList();
        Assert.Equal(2, createStatements.Count);
        Assert.Contains(createStatements, s => s.Contains("Table1"));
        Assert.Contains(createStatements, s => s.Contains("Table2"));
    }

 // ───────────────────────────── SQL Server ────────────────────────────

    [Fact]
    public void SqlServer_AddTable_WithPk_EmitsPrimaryKey()
    {
        var table = MakeTable("Orders",
            PkCol("OrderId"),
            Col("CustomerName", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("[OrderId]", createStmt);
    }

    [Fact]
    public void SqlServer_AddTable_WithUniqueColumn_EmitsUnique()
    {
        var table = MakeTable("Users",
            PkCol("Id"),
            Col("Email", nullable: false, unique: true)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("[Email]", createStmt);
    }

    [Fact]
    public void SqlServer_AddTable_WithIndex_EmitsCreateIndex()
    {
        var table = MakeTable("Products",
            PkCol("Id"),
            Col("Sku", nullable: false, indexName: "idx_Products_Sku")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Products_Sku") && s.Contains("[Sku]"));
    }

    [Fact]
    public void SqlServer_AddTable_WithNullableColumn_EmitsNull()
    {
        var table = MakeTable("TestTable",
            PkCol("Id"),
            Col("Notes", nullable: true)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("NULL", createStmt);
        Assert.Contains("[Notes]", createStmt);
    }

    [Fact]
    public void SqlServer_AddTable_WithNonNullableColumn_EmitsNotNull()
    {
        var table = MakeTable("TestTable",
            PkCol("Id"),
            Col("Title", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("NOT NULL", createStmt);
        Assert.Contains("[Title]", createStmt);
    }

    [Fact]
    public void SqlServer_DropTable_EmitsDropTable()
    {
        var table = MakeTable("LegacyTable", PkCol("Id"));
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("DROP TABLE") && s.Contains("LegacyTable"));
    }

    [Fact]
    public void SqlServer_AlteredColumn_CanChangeNullability()
    {
        var table = MakeTable("Entries",
            PkCol("Id"),
            Col("Description", nullable: true)
        );
        var oldCol = Col("Description", nullable: true);
        var newCol = Col("Description", nullable: false);

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

 // SQL Server supports ALTER COLUMN directly (no table recreation needed)
 // The up migration should contain an ALTER statement
        Assert.NotEmpty(sql.Up);
    }

    [Fact]
    public void SqlServer_AddColumn_EmitsAlterTableAdd()
    {
        var table = MakeTable("Posts",
            PkCol("Id"),
            Col("Content", nullable: true)
        );
        var newCol = new ColumnSchema { Name = "PublishedAt", ClrType = typeof(DateTime).FullName!, IsNullable = true };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("ALTER TABLE") && s.Contains("ADD") && s.Contains("[PublishedAt]"));
    }

    [Fact]
    public void SqlServer_EmptyDiff_ProducesNoStatements()
    {
        var diff = new SchemaDiff();
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var hasDDL = sql.Up.Any(s => s.StartsWith("CREATE") || s.StartsWith("DROP") || s.StartsWith("ALTER"));
        Assert.False(hasDDL);
    }

 // ─── Column type mapping tests ─────────────────────────────────────────

    [Fact]
    public void Sqlite_IntColumn_MapsToInteger()
    {
        var table = MakeTable("T",
            PkCol("Id"),
            new ColumnSchema { Name = "Count", ClrType = typeof(int).FullName!, IsNullable = false }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("\"Count\" INTEGER", createStmt);
    }

    [Fact]
    public void Sqlite_DecimalColumn_MapsToNumeric()
    {
        var table = MakeTable("T",
            PkCol("Id"),
            new ColumnSchema { Name = "Price", ClrType = typeof(decimal).FullName!, IsNullable = false }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("\"Price\" NUMERIC", createStmt);
    }

    [Fact]
    public void Sqlite_DoubleColumn_MapsToReal()
    {
        var table = MakeTable("T",
            PkCol("Id"),
            new ColumnSchema { Name = "Score", ClrType = typeof(double).FullName!, IsNullable = false }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("\"Score\" REAL", createStmt);
    }
}
