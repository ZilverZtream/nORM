using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies FK constraint awareness in SchemaDiff computation and SQL generation
/// across all four migration SQL generators.
/// </summary>
public class MigrationFkTests
{
 // ── Helpers ──────────────────────────────────────────────────────────────

    private static ForeignKeySchema MakeFk(string constraint, string[] depCols, string principalTable,
        string[]? refCols = null, string onDelete = "NO ACTION", string onUpdate = "NO ACTION") =>
        new ForeignKeySchema
        {
            ConstraintName = constraint,
            DependentColumns = depCols,
            PrincipalTable = principalTable,
            PrincipalColumns = refCols ?? new[] { "Id" },
            OnDelete = onDelete,
            OnUpdate = onUpdate
        };

    private static TableSchema MakeTable(string name, IEnumerable<ForeignKeySchema>? fks = null) =>
        new TableSchema
        {
            Name = name,
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true },
                new ColumnSchema { Name = "FkCol", ClrType = typeof(int).FullName!, IsNullable = true }
            },
            ForeignKeys = fks?.ToList() ?? new List<ForeignKeySchema>()
        };

 // ── SchemaDiffer FK detection tests ──────────────────────────────────────

 /// <summary>
 /// A new FK on an existing table must appear in AddedForeignKeys.
 /// </summary>
    [Fact]
    public void Diff_NewFkOnExistingTable_AppearsInAddedForeignKeys()
    {
        var fk = MakeFk("FK_Child_Parent_FkCol", new[] { "FkCol" }, "Parent");
        var oldSnap = new SchemaSnapshot { Tables = { MakeTable("Child") } };
        var newSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { fk }) } };

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Single(diff.AddedForeignKeys);
        Assert.Equal("FK_Child_Parent_FkCol", diff.AddedForeignKeys[0].ForeignKey.ConstraintName);
        Assert.Empty(diff.DroppedForeignKeys);
    }

 /// <summary>
 /// A removed FK on an existing table must appear in DroppedForeignKeys.
 /// </summary>
    [Fact]
    public void Diff_RemovedFkOnExistingTable_AppearsInDroppedForeignKeys()
    {
        var fk = MakeFk("FK_Child_Parent_FkCol", new[] { "FkCol" }, "Parent");
        var oldSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { fk }) } };
        var newSnap = new SchemaSnapshot { Tables = { MakeTable("Child") } };

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Empty(diff.AddedForeignKeys);
        Assert.Single(diff.DroppedForeignKeys);
        Assert.Equal("FK_Child_Parent_FkCol", diff.DroppedForeignKeys[0].ForeignKey.ConstraintName);
    }

 /// <summary>
 /// When an FK changes (e.g. OnDelete changes), it must appear in both
 /// DroppedForeignKeys (old definition) and AddedForeignKeys (new definition).
 /// </summary>
    [Fact]
    public void Diff_AlteredFk_AppearsInBothDroppedAndAdded()
    {
        var oldFk = MakeFk("FK_Child_Parent_FkCol", new[] { "FkCol" }, "Parent", onDelete: "NO ACTION");
        var newFk = MakeFk("FK_Child_Parent_FkCol", new[] { "FkCol" }, "Parent", onDelete: "CASCADE");
        var oldSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { oldFk }) } };
        var newSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { newFk }) } };

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Single(diff.DroppedForeignKeys);
        Assert.Single(diff.AddedForeignKeys);
        Assert.Equal("CASCADE", diff.AddedForeignKeys[0].ForeignKey.OnDelete);
        Assert.Equal("NO ACTION", diff.DroppedForeignKeys[0].ForeignKey.OnDelete);
    }

 /// <summary>
 /// A diff with only FK changes must report HasChanges = true.
 /// </summary>
    [Fact]
    public void Diff_FkOnlyChange_HasChanges_IsTrue()
    {
        var fk = MakeFk("FK_Child_Parent_FkCol", new[] { "FkCol" }, "Parent");
        var oldSnap = new SchemaSnapshot { Tables = { MakeTable("Child") } };
        var newSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { fk }) } };

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.True(diff.HasChanges);
    }

 /// <summary>
 /// Unchanged FKs must not appear in AddedForeignKeys or DroppedForeignKeys.
 /// </summary>
    [Fact]
    public void Diff_UnchangedFk_DoesNotAppearInDiff()
    {
        var fk = MakeFk("FK_Child_Parent_FkCol", new[] { "FkCol" }, "Parent");
        var oldSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { fk }) } };
        var newSnap = new SchemaSnapshot { Tables = { MakeTable("Child", new[] { fk }) } };

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Empty(diff.AddedForeignKeys);
        Assert.Empty(diff.DroppedForeignKeys);
    }

 // ── CREATE TABLE inline FK tests (all 4 generators) ──────────────────────

 /// <summary>
 /// SQLite CREATE TABLE for a new table with an FK must include the inline CONSTRAINT … FOREIGN KEY clause.
 /// </summary>
    [Fact]
    public void Sqlite_CreateTable_InlinesForeignKey()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog", new[] { "Id" });
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new SqliteMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("CONSTRAINT \"FK_Post_Blog_BlogId\"", sql.Up[0]);
        Assert.Contains("FOREIGN KEY", sql.Up[0]);
        Assert.Contains("REFERENCES \"Blog\"", sql.Up[0]);
    }

 /// <summary>
 /// SQL Server CREATE TABLE for a new table with an FK must include the inline CONSTRAINT … FOREIGN KEY clause.
 /// </summary>
    [Fact]
    public void SqlServer_CreateTable_InlinesForeignKey()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog", new[] { "Id" });
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new SqlServerMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("CONSTRAINT [FK_Post_Blog_BlogId]", sql.Up[0]);
        Assert.Contains("FOREIGN KEY", sql.Up[0]);
        Assert.Contains("REFERENCES [Blog]", sql.Up[0]);
    }

 /// <summary>
 /// MySQL CREATE TABLE for a new table with an FK must include the inline CONSTRAINT … FOREIGN KEY clause.
 /// </summary>
    [Fact]
    public void MySql_CreateTable_InlinesForeignKey()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog", new[] { "Id" });
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new MySqlMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("CONSTRAINT `FK_Post_Blog_BlogId`", sql.Up[0]);
        Assert.Contains("FOREIGN KEY", sql.Up[0]);
        Assert.Contains("REFERENCES `Blog`", sql.Up[0]);
    }

 /// <summary>
 /// Postgres CREATE TABLE for a new table with an FK must include the inline CONSTRAINT … FOREIGN KEY clause.
 /// </summary>
    [Fact]
    public void Postgres_CreateTable_InlinesForeignKey()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog", new[] { "Id" });
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new PostgresMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("CONSTRAINT \"FK_Post_Blog_BlogId\"", sql.Up[0]);
        Assert.Contains("FOREIGN KEY", sql.Up[0]);
        Assert.Contains("REFERENCES \"Blog\"", sql.Up[0]);
    }

 // ── ON DELETE / ON UPDATE clause tests ───────────────────────────────────

 /// <summary>
 /// ON DELETE CASCADE must appear in the FK constraint SQL when set.
 /// </summary>
    [Fact]
    public void SqlServer_CreateTable_FkWithCascade_EmitsCascadeClause()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog", onDelete: "CASCADE");
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new SqlServerMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Contains("ON DELETE CASCADE", sql.Up[0]);
    }

 /// <summary>
 /// NO ACTION (default) must NOT emit an ON DELETE clause (reduces noise).
 /// </summary>
    [Fact]
    public void SqlServer_CreateTable_FkNoAction_NoOnDeleteClause()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog", onDelete: "NO ACTION");
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var gen = new SqlServerMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.DoesNotContain("ON DELETE", sql.Up[0]);
    }

 // ── ALTER TABLE ADD / DROP FK tests ──────────────────────────────────────

 /// <summary>
 /// SQL Server AddedForeignKeys → ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY;
 /// Down → ALTER TABLE … DROP CONSTRAINT.
 /// </summary>
    [Fact]
    public void SqlServer_AddFk_EmitsAlterTableAdd_DownDropsConstraint()
    {
        var table = MakeTable("Post");
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));

        var gen = new SqlServerMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("ALTER TABLE [Post] ADD", sql.Up[0]);
        Assert.Contains("CONSTRAINT [FK_Post_Blog_BlogId]", sql.Up[0]);
        Assert.Single(sql.Down);
        Assert.Contains("ALTER TABLE [Post] DROP CONSTRAINT [FK_Post_Blog_BlogId]", sql.Down[0]);
    }

 /// <summary>
 /// SQL Server DroppedForeignKeys → ALTER TABLE … DROP CONSTRAINT;
 /// Down → ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY.
 /// </summary>
    [Fact]
    public void SqlServer_DropFk_EmitsAlterTableDrop_DownAddsConstraint()
    {
        var table = MakeTable("Post");
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var diff = new SchemaDiff();
        diff.DroppedForeignKeys.Add((table, fk));

        var gen = new SqlServerMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("ALTER TABLE [Post] DROP CONSTRAINT [FK_Post_Blog_BlogId]", sql.Up[0]);
        Assert.Single(sql.Down);
        Assert.Contains("ALTER TABLE [Post] ADD", sql.Down[0]);
        Assert.Contains("CONSTRAINT [FK_Post_Blog_BlogId]", sql.Down[0]);
    }

 /// <summary>
 /// MySQL uses DROP FOREIGN KEY (not DROP CONSTRAINT) for FK removal.
 /// </summary>
    [Fact]
    public void MySql_DropFk_EmitsDropForeignKey()
    {
        var table = MakeTable("Post");
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var diff = new SchemaDiff();
        diff.DroppedForeignKeys.Add((table, fk));

        var gen = new MySqlMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("DROP FOREIGN KEY `FK_Post_Blog_BlogId`", sql.Up[0]);
 // Down must use ADD CONSTRAINT syntax
        Assert.Single(sql.Down);
        Assert.Contains("ADD CONSTRAINT", sql.Down[0]);
    }

 /// <summary>
 /// MySQL AddedForeignKeys Down path must use DROP FOREIGN KEY.
 /// </summary>
    [Fact]
    public void MySql_AddFk_Down_EmitsDropForeignKey()
    {
        var table = MakeTable("Post");
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));

        var gen = new MySqlMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Down);
        Assert.Contains("DROP FOREIGN KEY `FK_Post_Blog_BlogId`", sql.Down[0]);
    }

 /// <summary>
 /// Postgres uses DROP CONSTRAINT (same as SQL Server).
 /// </summary>
    [Fact]
    public void Postgres_DropFk_EmitsDropConstraint()
    {
        var table = MakeTable("Post");
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var diff = new SchemaDiff();
        diff.DroppedForeignKeys.Add((table, fk));

        var gen = new PostgresMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Up);
        Assert.Contains("DROP CONSTRAINT \"FK_Post_Blog_BlogId\"", sql.Up[0]);
    }

 /// <summary>
 /// SQLite AddedForeignKeys must use table recreation (CREATE temp + INSERT + DROP + RENAME).
 /// PRAGMA foreign_keys=off/on must appear in pre/post transaction segments.
 /// </summary>
    [Fact]
    public void Sqlite_AddFk_UsesTableRecreation_WithPragmaInPrePost()
    {
        var table = MakeTable("Post");
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));

        var gen = new SqliteMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

 // Must use table recreation sequence
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE \"__temp__Post\""));
        Assert.Contains(sql.Up, s => s.StartsWith("INSERT INTO"));
        Assert.Contains(sql.Up, s => s == "DROP TABLE \"Post\"");
        Assert.Contains(sql.Up, s => s.Contains("RENAME TO \"Post\""));

 // PRAGMA must be in pre/post segments, NOT inline in Up/Down
        Assert.DoesNotContain(sql.Up, s => s.Contains("PRAGMA"));
        Assert.NotNull(sql.PreTransactionUp);
        Assert.Contains("PRAGMA foreign_keys=off", sql.PreTransactionUp!);
        Assert.NotNull(sql.PostTransactionUp);
        Assert.Contains("PRAGMA foreign_keys=on", sql.PostTransactionUp!);
    }

 // ── DroppedTables Down recreation includes FKs ───────────────────────────

 /// <summary>
 /// When a table with FKs is dropped, the Down recreation must include the FK constraints.
 /// </summary>
    [Fact]
    public void SqlServer_DroppedTable_Down_IncludesForeignKey()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var gen = new SqlServerMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

 // Down must recreate with the FK
        Assert.Single(sql.Down);
        Assert.Contains("CONSTRAINT [FK_Post_Blog_BlogId]", sql.Down[0]);
        Assert.Contains("FOREIGN KEY", sql.Down[0]);
    }

 /// <summary>
 /// SQLite dropped table Down recreation must include inline FK constraints.
 /// </summary>
    [Fact]
    public void Sqlite_DroppedTable_Down_IncludesForeignKey()
    {
        var fk = MakeFk("FK_Post_Blog_BlogId", new[] { "BlogId" }, "Blog");
        var table = MakeTable("Post", new[] { fk });
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var gen = new SqliteMigrationSqlGenerator();
        var sql = gen.GenerateSql(diff);

        Assert.Single(sql.Down);
        Assert.Contains("CONSTRAINT \"FK_Post_Blog_BlogId\"", sql.Down[0]);
        Assert.Contains("FOREIGN KEY", sql.Down[0]);
    }
}
