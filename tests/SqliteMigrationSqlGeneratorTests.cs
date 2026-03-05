using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public class SqliteMigrationSqlGeneratorTests
{
    [Fact]
    public void DownMigration_DisablesForeignKeys()
    {
        var table = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Content", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };

        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns[1]));

        var generator = new SqliteMigrationSqlGenerator();
        var sql = generator.GenerateSql(diff);

        Assert.Equal("PRAGMA foreign_keys=off", sql.Down[0]);
        Assert.Equal("PRAGMA foreign_keys=on", sql.Down[^1]);
    }

    /// <summary>
    /// G2: When AlteredColumns contains a column with changed IsNullable, the generated Up
    /// migration must contain CREATE TABLE + INSERT + DROP TABLE + RENAME statements (no comments).
    /// </summary>
    [Fact]
    public void AlteredColumn_ChangedNullability_GeneratesTableRecreation()
    {
        var table = new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Body", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };

        var oldCol = new ColumnSchema { Name = "Body", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Body", ClrType = typeof(string).FullName!, IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var generator = new SqliteMigrationSqlGenerator();
        var sql = generator.GenerateSql(diff);

        // Up migration must use the table-recreation workaround
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE"));
        Assert.Contains(sql.Up, s => s.StartsWith("INSERT INTO"));
        Assert.Contains(sql.Up, s => s.StartsWith("DROP TABLE"));
        Assert.Contains(sql.Up, s => s.Contains("RENAME TO"));
        Assert.Contains(sql.Up, s => s == "PRAGMA foreign_keys=off");
        Assert.Contains(sql.Up, s => s == "PRAGMA foreign_keys=on");

        // Must NOT emit comment lines
        Assert.DoesNotContain(sql.Up, s => s.TrimStart().StartsWith("--"));

        // Down migration must also use table-recreation
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE TABLE"));
        Assert.Contains(sql.Down, s => s.StartsWith("INSERT INTO"));
        Assert.Contains(sql.Down, s => s.StartsWith("DROP TABLE"));
    }

    [Fact]
    public void AlteredColumn_UpMigration_UsesNewColumnDefinition()
    {
        var table = new TableSchema
        {
            Name = "Item",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false },
                new ColumnSchema { Name = "Value",  ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };

        var oldCol = new ColumnSchema { Name = "Value", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Value", ClrType = typeof(string).FullName!, IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var generator = new SqliteMigrationSqlGenerator();
        var sql = generator.GenerateSql(diff);

        // The CREATE TABLE in Up should define "Value" as NOT NULL
        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("NOT NULL", createStmt);
        // "Value" column should be NOT NULL in the new table
        Assert.Contains("\"Value\" TEXT NOT NULL", createStmt);
    }

    // MIG-1: Tests for PK / UNIQUE / INDEX DDL generation

    [Fact]
    public void CreateTable_WithPkColumn_EmitsPrimaryKeyConstraint()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
                new ColumnSchema { Name = "Name",  ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("\"Id\"", createStmt);
    }

    [Fact]
    public void CreateTable_WithUniqueNonPkColumn_EmitsUniqueConstraint()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
                new ColumnSchema { Name = "Email", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("\"Email\"", createStmt);
    }

    [Fact]
    public void CreateTable_WithIndexColumn_EmitsSeparateCreateIndex()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
                new ColumnSchema { Name = "Name",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Users_Name" }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Users_Name") && s.Contains("\"Users\"") && s.Contains("\"Name\""));
    }

    [Fact]
    public void CreateTable_WithoutConstraints_DoesNotEmitConstraintClauses()
    {
        var table = new TableSchema
        {
            Name = "Plain",
            Columns =
            {
                new ColumnSchema { Name = "Id",  ClrType = typeof(int).FullName!,    IsNullable = false },
                new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.DoesNotContain("PRIMARY KEY", createStmt);
        Assert.DoesNotContain("UNIQUE", createStmt);
        Assert.DoesNotContain(sql.Up, s => s.StartsWith("CREATE INDEX"));
    }

    // ─── MIG-1: AddRecreate preserves PK/UNIQUE/INDEX through ALTER ───────

    /// <summary>
    /// Schema round-trip equivalence test: verify that PK, UNIQUE, and named INDEX
    /// are all present in the recreated table after an ALTER (nullability change).
    /// </summary>
    [Fact]
    public void AlteredColumn_RecreatedTable_PreservesPrimaryKey()
    {
        var table = BuildFullyConstrainedTable();
        var diff = new SchemaDiff();
        // Alter the nullable column (change from nullable to not-null)
        var oldNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var newNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = false };
        diff.AlteredColumns.Add((table, newNullable, oldNullable));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("\"Id\"", createStmt);
    }

    [Fact]
    public void AlteredColumn_RecreatedTable_PreservesUniqueConstraint()
    {
        var table = BuildFullyConstrainedTable();
        var diff = new SchemaDiff();
        var oldNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var newNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = false };
        diff.AlteredColumns.Add((table, newNullable, oldNullable));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("\"Email\"", createStmt);
    }

    [Fact]
    public void AlteredColumn_RecreatedTable_PreservesNamedIndex()
    {
        var table = BuildFullyConstrainedTable();
        var diff = new SchemaDiff();
        var oldNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var newNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = false };
        diff.AlteredColumns.Add((table, newNullable, oldNullable));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // The CREATE INDEX for the non-PK, non-unique indexed column should follow the RENAME
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Name") && s.Contains("\"Name\""));
    }

    [Fact]
    public void AlteredColumn_DownMigration_PreservesPrimaryKey()
    {
        var table = BuildFullyConstrainedTable();
        var diff = new SchemaDiff();
        var oldNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var newNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = false };
        diff.AlteredColumns.Add((table, newNullable, oldNullable));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // Down migration must also preserve constraints (uses original column definitions)
        var downCreate = sql.Down.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", downCreate);
    }

    [Fact]
    public void AlteredColumn_DownMigration_PreservesUniqueAndIndex()
    {
        var table = BuildFullyConstrainedTable();
        var diff = new SchemaDiff();
        var oldNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var newNullable = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = false };
        diff.AlteredColumns.Add((table, newNullable, oldNullable));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var downCreate = sql.Down.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", downCreate);
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Name"));
    }

    [Fact]
    public void AlteredColumn_MultipleIndexes_AllPreservedAfterRecreate()
    {
        var table = new TableSchema
        {
            Name = "Product",
            Columns =
            {
                new ColumnSchema { Name = "Id",       ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Product" },
                new ColumnSchema { Name = "Sku",      ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Sku" },
                new ColumnSchema { Name = "Category", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Category" },
                new ColumnSchema { Name = "Notes",    ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };

        var diff = new SchemaDiff();
        var oldCol = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = false };
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Sku"));
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Category"));
    }

    [Fact]
    public void DroppedColumn_RemainingColumnsPrimaryKeyPreserved()
    {
        // When a non-PK column is dropped, the PK of the remaining columns must still be emitted.
        // DroppedColumns path uses simple column defs — this test documents current behavior
        // and guards that remaining PK columns are correctly described.
        var table = new TableSchema
        {
            Name = "Widget",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Widget" },
                new ColumnSchema { Name = "Name",  ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Extra", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };

        // Drop "Extra" column
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[2]));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // The UP migration must contain the table-recreation sequence for the dropped column
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__Widget"));
        Assert.Contains(sql.Up, s => s.StartsWith("DROP TABLE") && s.Contains("Widget"));
        Assert.Contains(sql.Up, s => s.Contains("RENAME TO"));
    }

    // ─── Fix 1: DroppedColumns up migration preserves constraints ─────────

    [Fact]
    public void DroppedColumn_NonKey_UpMigration_PreservesPrimaryKey()
    {
        // Drop a non-PK column; the remaining recreated table must still have PRIMARY KEY.
        var table = new TableSchema
        {
            Name = "Order",
            Columns =
            {
                new ColumnSchema { Name = "Id",     ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Order" },
                new ColumnSchema { Name = "Total",  ClrType = typeof(decimal).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Notes",  ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[2])); // drop Notes

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("\"Id\"", createStmt);
    }

    [Fact]
    public void DroppedColumn_NonKey_UpMigration_PreservesUniqueConstraint()
    {
        var table = new TableSchema
        {
            Name = "Customer",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Customer" },
                new ColumnSchema { Name = "Email", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
                new ColumnSchema { Name = "Phone", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[2])); // drop Phone

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("\"Email\"", createStmt);
    }

    [Fact]
    public void DroppedColumn_NonKey_UpMigration_PreservesNamedIndex()
    {
        var table = new TableSchema
        {
            Name = "Product",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Product" },
                new ColumnSchema { Name = "Sku",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Sku" },
                new ColumnSchema { Name = "Tmp",  ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[2])); // drop Tmp

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Sku"));
    }

    [Fact]
    public void AddedColumn_DownMigration_RecreatesTableWithoutAddedColumn_AndPreservesConstraints()
    {
        // Down migration for an added column should recreate the table WITHOUT that column,
        // while preserving all constraints of the remaining columns.
        var table = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",      ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",   ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Title" },
                new ColumnSchema { Name = "Summary", ClrType = typeof(string).FullName!, IsNullable = true } // this is the newly-added column
            }
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns[2])); // Summary was added

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // Up: should be a simple ALTER TABLE ADD COLUMN
        Assert.Contains(sql.Up, s => s.Contains("ALTER TABLE") && s.Contains("ADD COLUMN") && s.Contains("Summary"));

        // Down: should recreate WITHOUT Summary, WITH PK and index preserved
        var downCreate = sql.Down.First(s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__"));
        Assert.DoesNotContain("Summary", downCreate);
        Assert.Contains("PRIMARY KEY", downCreate);
        // Named index on Title should be emitted as a separate CREATE INDEX
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Title"));
        // Foreign key pragma wrapping
        Assert.Contains(sql.Down, s => s == "PRAGMA foreign_keys=off");
        Assert.Contains(sql.Down, s => s == "PRAGMA foreign_keys=on");
    }

    [Fact]
    public void DroppedColumn_RoundTrip_OriginalSchemaConstraintsPreserved()
    {
        // End-to-end: table with PK+UNIQUE+INDEX → drop a non-key column →
        // resulting up migration has all constraints intact.
        var table = BuildFullyConstrainedTable();
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[3])); // drop Notes

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Name"));
    }

    // ─── Helper ────────────────────────────────────────────────────────────

    /// <summary>Builds a table with PK, unique, named-index, and nullable columns.</summary>
    private static TableSchema BuildFullyConstrainedTable()
        => new TableSchema
        {
            Name = "FullTable",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_FullTable" },
                new ColumnSchema { Name = "Email",  ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
                new ColumnSchema { Name = "Name",   ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Name" },
                new ColumnSchema { Name = "Notes",  ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
}

