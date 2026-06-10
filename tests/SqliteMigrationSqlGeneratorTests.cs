using System;
using System.Linq;
using nORM.Configuration;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
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

 // PRAGMA statements are now in PreTransactionDown / PostTransactionDown segments,
 // NOT in the main Down list. This ensures they execute outside the migration transaction.
        Assert.NotNull(sql.PreTransactionDown);
        Assert.NotNull(sql.PostTransactionDown);
        Assert.Equal("PRAGMA foreign_keys=off", sql.PreTransactionDown![0]);
        Assert.Equal("PRAGMA foreign_keys=on", sql.PostTransactionDown![0]);
 // Main Down list must NOT contain PRAGMA statements
        Assert.DoesNotContain(sql.Down, s => s.StartsWith("PRAGMA"));
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

 // PRAGMA statements are in pre/post-transaction segments, NOT in the main Up list.
        Assert.NotNull(sql.PreTransactionUp);
        Assert.NotNull(sql.PostTransactionUp);
        Assert.Equal("PRAGMA foreign_keys=off", sql.PreTransactionUp![0]);
        Assert.Equal("PRAGMA foreign_keys=on", sql.PostTransactionUp![0]);
        Assert.DoesNotContain(sql.Up, s => s.StartsWith("PRAGMA"));

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

    [Fact]
    public void AlteredColumn_RecreateTable_OmitsComputedColumnsFromInsertProjection()
    {
        var table = new TableSchema
        {
            Name = "Person",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "NameLength", ClrType = typeof(int).FullName!, IsNullable = false, ComputedColumnSql = "length(\"Name\")" }
            }
        };

        var oldCol = new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE", StringComparison.Ordinal)
            && s.Contains("\"NameLength\" INTEGER GENERATED ALWAYS AS", StringComparison.Ordinal));
        Assert.Contains(sql.Up, s => s == "INSERT INTO \"__temp__Person\" (\"Id\", \"Name\") SELECT \"Id\", \"Name\" FROM \"Person\"");
        Assert.DoesNotContain(sql.Up, s => s.StartsWith("INSERT INTO", StringComparison.Ordinal)
            && s.Contains("NameLength", StringComparison.Ordinal));
    }

 // Tests for PK / UNIQUE / INDEX DDL generation

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
    public void CreateTable_WithDescendingIndexColumn_EmitsDescendingCreateIndex()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
                new ColumnSchema
                {
                    Name = "Name",
                    ClrType = typeof(string).FullName!,
                    IsNullable = false,
                    Indexes = { new ColumnIndexSchema { Name = "idx_Users_Name_Desc", IsDescending = true } }
                }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Users_Name_Desc") && s.Contains("\"Name\" DESC"));
    }

    [Fact]
    public void CreateTable_WithPostgresNullsNotDistinctIndex_Throws()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
                new ColumnSchema
                {
                    Name = "Name",
                    ClrType = typeof(string).FullName!,
                    IsNullable = true,
                    Indexes = { new ColumnIndexSchema { Name = "IX_Users_Name", IsUnique = true, NullsNotDistinct = true } }
                }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));
        Assert.Contains("NULLS NOT DISTINCT", ex.Message);
    }

    [Fact]
    public void CreateTable_WithExplicitNullSortOrder_Throws()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
                new ColumnSchema
                {
                    Name = "Name",
                    ClrType = typeof(string).FullName!,
                    IsNullable = true,
                    Indexes = { new ColumnIndexSchema { Name = "IX_Users_Name", NullSortOrder = IndexNullSortOrder.First } }
                }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));
        Assert.Contains("NULLS FIRST/LAST", ex.Message);
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

 // ─── AddRecreate preserves PK/UNIQUE/INDEX through ALTER ───────

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
 // Foreign key pragma wrapping is in pre/post-transaction segments, not in main Down list.
        Assert.NotNull(sql.PreTransactionDown);
        Assert.Contains(sql.PreTransactionDown!, s => s == "PRAGMA foreign_keys=off");
        Assert.NotNull(sql.PostTransactionDown);
        Assert.Contains(sql.PostTransactionDown!, s => s == "PRAGMA foreign_keys=on");
        Assert.DoesNotContain(sql.Down, s => s.StartsWith("PRAGMA"));
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

 // ─── Identifier escaping with double-quotes ───────────────────────

    [Fact]
    public void CreateTable_EscapesTableNameWithDoubleQuote()
    {
        var table = new TableSchema
        {
            Name = "He\"llo",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains("\"He\"\"llo\"", sql.Up[0]);
    }

    [Fact]
    public void CreateTable_EscapesColumnNameWithDoubleQuote()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns = { new ColumnSchema { Name = "co\"l", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains("\"co\"\"l\"", sql.Up[0]);
    }

 // ─── NOT NULL + DefaultValue ──────────────────────────────────────

    [Fact]
    public void AddColumn_NotNull_WithDefaultValue_EmitsDefault()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var newCol = new ColumnSchema { Name = "Status", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0" };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("NOT NULL DEFAULT 0", sql.Up[0]);
    }

    [Fact]
    public void AddColumn_NotNull_WithoutDefaultValue_ThrowsInvalidOperationException()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var newCol = new ColumnSchema { Name = "Status", ClrType = typeof(int).FullName!, IsNullable = false };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var ex = Assert.Throws<InvalidOperationException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));
        Assert.Contains("Status", ex.Message);
        Assert.Contains("DefaultValue", ex.Message);
    }

    [Fact]
    public void AddColumn_Nullable_DoesNotRequireDefaultValue()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var newCol = new ColumnSchema { Name = "Notes", ClrType = typeof(string).FullName!, IsNullable = true };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, newCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.DoesNotContain("DEFAULT", sql.Up[0]);
    }

    [Fact]
    public void AddColumn_ImplicitUnique_CreatesUniqueIndexAfterAddingColumn()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = true, IsUnique = true }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns[1]));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s == "ALTER TABLE \"T\" ADD COLUMN \"Code\" TEXT NULL");
        Assert.Contains(sql.Up, s => s == "CREATE UNIQUE INDEX \"UQ_T_Code\" ON \"T\" (\"Code\")");
    }

    [Fact]
    public void AddColumn_PrimaryKey_UsesTableRecreationWithDefaultExpression()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Code", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0", IsPrimaryKey = true, IsUnique = true, IndexName = "PK_T" }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns[1]));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.DoesNotContain(sql.Up, s => s.Contains("ADD COLUMN \"Code\"", StringComparison.Ordinal));
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE") && s.Contains("CONSTRAINT \"PK_T\" PRIMARY KEY (\"Code\")", StringComparison.Ordinal));
        Assert.Contains(sql.Up, s => s == "INSERT INTO \"__temp__T\" (\"Name\", \"Code\") SELECT \"Name\", 0 FROM \"T\"");
        Assert.Equal("PRAGMA foreign_keys=off", sql.PreTransactionUp![0]);
        Assert.Equal("PRAGMA foreign_keys=on", sql.PostTransactionUp![0]);
    }

    [Fact]
    public void DroppedColumn_ImplicitUnique_DownRestoresUniqueIndexAfterAddingColumn()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = true, IsUnique = true }
            }
        };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[1]));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Down, s => s == "ALTER TABLE \"T\" ADD COLUMN \"Code\" TEXT NULL");
        Assert.Contains(sql.Down, s => s == "CREATE UNIQUE INDEX \"UQ_T_Code\" ON \"T\" (\"Code\")");
    }

    [Fact]
    public void DroppedColumn_PrimaryKey_DownUsesTableRecreationWithDefaultExpression()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Code", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0", IsPrimaryKey = true, IsUnique = true, IndexName = "PK_T" }
            }
        };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[1]));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.DoesNotContain(sql.Down, s => s.Contains("ADD COLUMN \"Code\"", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE TABLE") && s.Contains("CONSTRAINT \"PK_T\" PRIMARY KEY (\"Code\")", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s == "INSERT INTO \"__temp__T\" (\"Name\", \"Code\") SELECT \"Name\", 0 FROM \"T\"");
        Assert.Equal("PRAGMA foreign_keys=off", sql.PreTransactionDown![0]);
        Assert.Equal("PRAGMA foreign_keys=on", sql.PostTransactionDown![0]);
    }

    [Fact]
    public void AddColumn_PrimaryKeyWithAddedExplicitIndex_DoesNotCreateIndexTwice()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Code", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0", IsPrimaryKey = true, IsUnique = true, IndexName = "PK_T" },
                new ColumnSchema { Name = "Tag", ClrType = typeof(string).FullName!, IsNullable = true, IndexName = "IX_T_Tag" }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns[1]));
        diff.AddedColumns.Add((table, table.Columns[2]));
        diff.AddedIndexes.Add((table, "IX_T_Tag", false, new[] { "Tag" }, new[] { false }));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Single(sql.Up.Where(s => s.StartsWith("CREATE INDEX", StringComparison.Ordinal) && s.Contains("\"IX_T_Tag\"", StringComparison.Ordinal)));
        Assert.DoesNotContain(sql.Down, s => s.StartsWith("CREATE INDEX", StringComparison.Ordinal) && s.Contains("IX_T_Tag", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s == "DROP INDEX IF EXISTS \"IX_T_Tag\"");
    }

    [Fact]
    public void AddColumn_PrimaryKeyWithAddedExpressionIndex_DoesNotCreateExpressionIndexTwice()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Code", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "0", IsPrimaryKey = true, IsUnique = true, IndexName = "PK_T" }
            }
        };
        var expressionIndex = new ExpressionIndexSchema
        {
            Name = "IX_T_LowerName",
            ExpressionSql = "lower(\"Name\")"
        };
        table.ExpressionIndexes.Add(expressionIndex);
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns[1]));
        diff.AddedExpressionIndexes.Add((table, expressionIndex));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Single(sql.Up.Where(s => s.StartsWith("CREATE INDEX", StringComparison.Ordinal) && s.Contains("\"IX_T_LowerName\"", StringComparison.Ordinal)));
    }

    [Fact]
    public void DroppedColumn_WithDroppedExpressionIndex_DoesNotRestoreInvalidExpressionIndexDuringRecreate()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
        var expressionIndex = new ExpressionIndexSchema
        {
            Name = "IX_T_LowerCode",
            ExpressionSql = "lower(\"Code\")"
        };
        table.ExpressionIndexes.Add(expressionIndex);
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, table.Columns[1]));
        diff.DroppedExpressionIndexes.Add((table, expressionIndex));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.DoesNotContain(sql.Up, s => s.Contains("IX_T_LowerCode", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s == "CREATE INDEX \"IX_T_LowerCode\" ON \"T\" (lower(\"Code\"))");
    }

    [Fact]
    public void AlteredColumn_WithChangedExpressionIndex_RecreatesOnlyNewExpressionIndex()
    {
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var oldCol = new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true };
        var newCol = new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false };
        var oldExpression = new ExpressionIndexSchema
        {
            Name = "IX_T_NormalizedName",
            ExpressionSql = "trim(\"Name\")"
        };
        var newExpression = new ExpressionIndexSchema
        {
            Name = "IX_T_NormalizedName",
            ExpressionSql = "lower(\"Name\")"
        };
        table.ExpressionIndexes.Add(newExpression);
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));
        diff.DroppedExpressionIndexes.Add((table, oldExpression));
        diff.AddedExpressionIndexes.Add((table, newExpression));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.DoesNotContain(sql.Up, s => s.StartsWith("DROP INDEX", StringComparison.Ordinal) && s.Contains("IX_T_NormalizedName", StringComparison.Ordinal));
        Assert.Single(sql.Up.Where(s => s == "CREATE INDEX \"IX_T_NormalizedName\" ON \"T\" (lower(\"Name\"))"));
        Assert.Single(sql.Down.Where(s => s == "CREATE INDEX \"IX_T_NormalizedName\" ON \"T\" (trim(\"Name\"))"));
    }

    [Fact]
    public void AlteredColumn_WithChangedNamedIndex_RecreatesOnlyTargetIndexDefinition()
    {
        var oldTable = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true, IndexName = "IX_T_Name" }
            }
        };
        var table = new TableSchema
        {
            Name = "T",
            Columns =
            {
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_T_Name", IsUnique = true }
            }
        };
        var oldCol = new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = true, IndexName = "IX_T_Name" };
        var newCol = new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_T_Name", IsUnique = true };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));
        diff.DroppedIndexes.Add((oldTable, "IX_T_Name"));
        diff.AddedIndexes.Add((table, "IX_T_Name", true, new[] { "Name" }, new[] { false }));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.DoesNotContain(sql.Up, s => s.StartsWith("DROP INDEX", StringComparison.Ordinal) && s.Contains("IX_T_Name", StringComparison.Ordinal));
        Assert.Single(sql.Up.Where(s => s == "CREATE UNIQUE INDEX \"IX_T_Name\" ON \"T\" (\"Name\")"));
        Assert.DoesNotContain(sql.Down, s => s.StartsWith("DROP INDEX", StringComparison.Ordinal) && s.Contains("IX_T_Name", StringComparison.Ordinal));
        Assert.Single(sql.Down.Where(s => s == "CREATE INDEX \"IX_T_Name\" ON \"T\" (\"Name\")"));
    }
}

