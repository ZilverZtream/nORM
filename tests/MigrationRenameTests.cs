using System;
using System.Linq;
using nORM.Mapping;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that a column decorated with [RenameColumn("OldName")] causes the schema differ
/// to emit a RENAME COLUMN operation instead of a destructive DROP + ADD pair.
/// Provider-specific DDL is validated for SQLite, SQL Server, PostgreSQL, and MySQL.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class MigrationRenameTests
{
    // ─── Helpers ──────────────────────────────────────────────────────────

    private static TableSchema MakeTable(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in cols) t.Columns.Add(c);
        return t;
    }

    private static ColumnSchema PkCol(string name) =>
        new() { Name = name, ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = $"PK_{name}" };

    private static ColumnSchema Col(string name, string? clrType = null, bool nullable = true) =>
        new() { Name = name, ClrType = clrType ?? typeof(string).FullName!, IsNullable = nullable };

    private static ColumnSchema RenameCol(string newName, string oldName, string? clrType = null, bool nullable = true) =>
        new() { Name = newName, ClrType = clrType ?? typeof(string).FullName!, IsNullable = nullable, PreviousName = oldName };

    // ─── SchemaDiffer: rename detection ───────────────────────────────────

    [Fact]
    public void Diff_with_RenameColumn_emits_RenamedColumns_not_DropAdd()
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("Orders", PkCol("Id"), Col("TotalCost", typeof(decimal).FullName!)));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("Orders", PkCol("Id"), RenameCol("TotalAmount", "TotalCost", typeof(decimal).FullName!)));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Single(diff.RenamedColumns);
        Assert.Empty(diff.DroppedColumns);
        Assert.Empty(diff.AddedColumns);

        var (table, oldName, newCol) = diff.RenamedColumns[0];
        Assert.Equal("Orders", table.Name);
        Assert.Equal("TotalCost", oldName);
        Assert.Equal("TotalAmount", newCol.Name);
    }

    [Fact]
    public void Diff_without_RenameColumn_still_emits_DropAdd()
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("Orders", PkCol("Id"), Col("TotalCost", typeof(decimal).FullName!)));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("Orders", PkCol("Id"), Col("TotalAmount", typeof(decimal).FullName!)));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Empty(diff.RenamedColumns);
        Assert.Single(diff.DroppedColumns);
        Assert.Single(diff.AddedColumns);
    }

    [Fact]
    public void Diff_RenameColumn_with_missing_old_column_falls_back_to_AddedColumn()
    {
        // If the old column referenced by PreviousName does not exist in the snapshot,
        // the differ treats the column as a plain addition.
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("Orders", PkCol("Id")));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("Orders", PkCol("Id"), RenameCol("TotalAmount", "NonExistentOldName")));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Empty(diff.RenamedColumns);
        Assert.Single(diff.AddedColumns);
    }

    [Fact]
    public void Diff_HasChanges_true_when_only_renames_present()
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("T", PkCol("Id"), Col("OldName")));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("T", PkCol("Id"), RenameCol("NewName", "OldName")));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.True(diff.HasChanges);
        Assert.False(diff.HasDestructiveChanges);
    }

    // ─── SQLite ───────────────────────────────────────────────────────────

    [Fact]
    public void Sqlite_rename_emits_RENAME_COLUMN_not_drop_add()
    {
        var diff = BuildRenameDiff("Orders", "TotalCost", "TotalAmount");
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var up = sql.Up;
        var down = sql.Down;

        Assert.Contains(up, s => s.Contains("RENAME COLUMN") && s.Contains("\"TotalCost\"") && s.Contains("\"TotalAmount\""));
        Assert.DoesNotContain(up, s => s.StartsWith("DROP COLUMN", StringComparison.OrdinalIgnoreCase) || (s.Contains("DROP") && s.Contains("TotalCost")));
        Assert.DoesNotContain(up, s => s.StartsWith("ADD COLUMN", StringComparison.OrdinalIgnoreCase) || (s.Contains("ADD") && s.Contains("TotalAmount")));

        // Down: rename back
        Assert.Contains(down, s => s.Contains("RENAME COLUMN") && s.Contains("\"TotalAmount\"") && s.Contains("\"TotalCost\""));
    }

    [Fact]
    public void Sqlite_rename_sql_well_formed()
    {
        var diff = BuildRenameDiff("Customers", "OldCol", "NewCol");
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var stmt = sql.Up.Single(s => s.Contains("RENAME COLUMN"));
        Assert.Equal("ALTER TABLE \"Customers\" RENAME COLUMN \"OldCol\" TO \"NewCol\"", stmt);
    }

    // ─── SQL Server ───────────────────────────────────────────────────────

    [Fact]
    public void SqlServer_rename_emits_sp_rename_not_drop_add()
    {
        var diff = BuildRenameDiff("Orders", "TotalCost", "TotalAmount");
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var up = sql.Up;
        Assert.Contains(up, s => s.Contains("sp_rename") && s.Contains("TotalCost") && s.Contains("TotalAmount"));
        Assert.DoesNotContain(up, s => s.StartsWith("ALTER TABLE") && s.Contains("DROP COLUMN"));
        Assert.DoesNotContain(up, s => s.StartsWith("ALTER TABLE") && s.Contains("ADD") && s.Contains("TotalAmount"));

        var down = sql.Down;
        Assert.Contains(down, s => s.Contains("sp_rename") && s.Contains("TotalAmount") && s.Contains("TotalCost"));
    }

    [Fact]
    public void SqlServer_rename_sql_well_formed()
    {
        var diff = BuildRenameDiff("Customers", "OldCol", "NewCol");
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var stmt = sql.Up.Single(s => s.Contains("sp_rename"));
        Assert.Equal("EXEC sp_rename 'Customers.OldCol', 'NewCol', 'COLUMN'", stmt);
    }

    // ─── PostgreSQL ───────────────────────────────────────────────────────

    [Fact]
    public void Postgres_rename_emits_RENAME_COLUMN_not_drop_add()
    {
        var diff = BuildRenameDiff("Orders", "TotalCost", "TotalAmount");
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);

        var up = sql.Up;
        Assert.Contains(up, s => s.Contains("RENAME COLUMN") && s.Contains("\"TotalCost\"") && s.Contains("\"TotalAmount\""));
        Assert.DoesNotContain(up, s => s.Contains("DROP COLUMN") && s.Contains("TotalCost"));
        Assert.DoesNotContain(up, s => s.Contains("ADD COLUMN") && s.Contains("TotalAmount"));

        var down = sql.Down;
        Assert.Contains(down, s => s.Contains("RENAME COLUMN") && s.Contains("\"TotalAmount\"") && s.Contains("\"TotalCost\""));
    }

    [Fact]
    public void Postgres_rename_sql_well_formed()
    {
        var diff = BuildRenameDiff("Customers", "OldCol", "NewCol");
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);

        var stmt = sql.Up.Single(s => s.Contains("RENAME COLUMN"));
        Assert.Equal("ALTER TABLE \"Customers\" RENAME COLUMN \"OldCol\" TO \"NewCol\"", stmt);
    }

    // ─── MySQL ────────────────────────────────────────────────────────────

    [Fact]
    public void MySql_rename_emits_RENAME_COLUMN_not_drop_add()
    {
        var diff = BuildRenameDiff("Orders", "TotalCost", "TotalAmount");
        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);

        var up = sql.Up;
        Assert.Contains(up, s => s.Contains("RENAME COLUMN") && s.Contains("`TotalCost`") && s.Contains("`TotalAmount`"));
        Assert.DoesNotContain(up, s => s.Contains("DROP COLUMN") && s.Contains("TotalCost"));
        Assert.DoesNotContain(up, s => s.Contains("ADD COLUMN") && s.Contains("TotalAmount"));

        var down = sql.Down;
        Assert.Contains(down, s => s.Contains("RENAME COLUMN") && s.Contains("`TotalAmount`") && s.Contains("`TotalCost`"));
    }

    [Fact]
    public void MySql_rename_sql_well_formed()
    {
        var diff = BuildRenameDiff("Customers", "OldCol", "NewCol");
        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);

        var stmt = sql.Up.Single(s => s.Contains("RENAME COLUMN"));
        Assert.Equal("ALTER TABLE `Customers` RENAME COLUMN `OldCol` TO `NewCol`", stmt);
    }

    // ─── SchemaSnapshotBuilder: attribute scanning ─────────────────────────

    [Fact]
    public void SchemaSnapshotBuilder_reads_RenameColumn_attribute_from_property()
    {
        // Build a snapshot from an assembly that has a type with [RenameColumn].
        var snap = SchemaSnapshotBuilder.Build(typeof(RenameAttributeEntity).Assembly);
        var table = snap.Tables.FirstOrDefault(t => t.Name == "RenameAttributeOrders");
        Assert.NotNull(table);

        var col = table!.Columns.FirstOrDefault(c => c.Name == "TotalAmount");
        Assert.NotNull(col);
        Assert.Equal("TotalCost", col!.PreviousName);
    }

    [Fact]
    public void End_to_end_snapshot_diff_with_RenameColumn_attribute_entity_emits_rename_sql()
    {
        // Old snapshot: has TotalCost column.
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("RenameAttributeOrders",
            PkCol("Id"),
            Col("TotalCost", typeof(decimal).FullName!, nullable: true)));

        // New snapshot: built from RenameAttributeEntity which has [RenameColumn("TotalCost")] on TotalAmount.
        var newSnap = SchemaSnapshotBuilder.Build(typeof(RenameAttributeEntity).Assembly);
        newSnap.Tables.RemoveAll(t => !string.Equals(t.Name, "RenameAttributeOrders", StringComparison.Ordinal));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Single(diff.RenamedColumns);
        Assert.Empty(diff.DroppedColumns);
        Assert.Empty(diff.AddedColumns);

        // Verify SQLite DDL. The old snapshot's hand-built Id differs from the builder's Id
        // (identity/unique/index-name), so the table is also recreated. When a table is recreated
        // the rename is folded into the recreate's INSERT ... SELECT (reading the old "TotalCost"
        // into the new "TotalAmount") instead of a standalone ALTER ... RENAME COLUMN, which would
        // fail against the already-rebuilt table. Accept either mechanism.
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        var standaloneRename = sql.Up.Any(s =>
            s.Contains("RENAME COLUMN") && s.Contains("TotalCost") && s.Contains("TotalAmount"));
        var foldedIntoRecreate = sql.Up.Any(s =>
            s.StartsWith("INSERT INTO", StringComparison.Ordinal)
            && s.Contains("\"TotalAmount\"") && s.Contains("\"TotalCost\""));
        Assert.True(standaloneRename || foldedIntoRecreate,
            "The [RenameColumn] rename must be applied either as a standalone RENAME COLUMN or folded into the recreate's INSERT ... SELECT.");
    }

    // ─── Helper ───────────────────────────────────────────────────────────

    private static SchemaDiff BuildRenameDiff(string tableName, string oldColName, string newColName)
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable(tableName,
            PkCol("Id"),
            Col(oldColName, typeof(decimal).FullName!, nullable: true)));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable(tableName,
            PkCol("Id"),
            RenameCol(newColName, oldColName, typeof(decimal).FullName!, nullable: true)));

        return SchemaDiffer.Diff(oldSnap, newSnap);
    }
}

// ─── Entity used by attribute-scanning tests ──────────────────────────────────

[System.ComponentModel.DataAnnotations.Schema.Table("RenameAttributeOrders")]
file sealed class RenameAttributeEntity
{
    [System.ComponentModel.DataAnnotations.Key]
    public int Id { get; set; }

    [RenameColumn("TotalCost")]
    public decimal? TotalAmount { get; set; }
}
