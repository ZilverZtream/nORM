using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Migration round-trip tests: verify that generating, applying, and rolling back migrations
/// produces correct DDL that preserves all constraints (PK, UNIQUE, INDEX) through the cycle.
///
/// These tests use SqliteMigrationSqlGenerator directly (no live DB connection required).
/// </summary>
public class MigrationRoundTripTests
{
    // ─── Helper: build a snapshot with specific schema ────────────────────

    private static SchemaSnapshot BuildSnapshot(params TableSchema[] tables)
    {
        var snap = new SchemaSnapshot();
        snap.Tables.AddRange(tables);
        return snap;
    }

    private static TableSchema BlogTable => new TableSchema
    {
        Name = "Blog",
        Columns =
        {
            new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
            new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false },
            new ColumnSchema { Name = "Slug",   ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
            new ColumnSchema { Name = "Rating", ClrType = typeof(int).FullName!,    IsNullable = true, IndexName = "idx_Blog_Rating" }
        }
    };

    // ─── Initial CREATE migration has correct DDL ─────────────────────────

    [Fact]
    public void InitialCreate_GeneratesMigration_ContainsPrimaryKey()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BlogTable);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("\"Id\"", createStmt);
    }

    [Fact]
    public void InitialCreate_GeneratesMigration_ContainsUniqueConstraint()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BlogTable);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("\"Slug\"", createStmt);
    }

    [Fact]
    public void InitialCreate_GeneratesMigration_ContainsCreateIndex()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BlogTable);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Blog_Rating") && s.Contains("\"Rating\""));
    }

    // ─── After ALTER (nullability change): constraints still present ──────

    [Fact]
    public void AlterNullability_RecreatedTable_StillHasPrimaryKey()
    {
        // Simulate: "before" snapshot has Rating as nullable, "after" as NOT NULL
        var before = BuildSnapshot(BlogTable);
        var afterTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Slug",   ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
                new ColumnSchema { Name = "Rating", ClrType = typeof(int).FullName!,    IsNullable = false, IndexName = "idx_Blog_Rating" }
            }
        };
        var after = BuildSnapshot(afterTable);

        var diff = SchemaDiffer.Diff(before, after);
        Assert.True(diff.HasChanges, "Changing Rating from nullable to NOT NULL should create a diff");

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // After ALTER via table recreation, PRIMARY KEY must still be present
        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
    }

    [Fact]
    public void AlterNullability_RecreatedTable_StillHasUniqueConstraint()
    {
        var before = BuildSnapshot(BlogTable);
        var afterTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Slug",   ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
                new ColumnSchema { Name = "Rating", ClrType = typeof(int).FullName!,    IsNullable = false, IndexName = "idx_Blog_Rating" }
            }
        };
        var after = BuildSnapshot(afterTable);
        var diff = SchemaDiffer.Diff(before, after);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("\"Slug\"", createStmt);
    }

    [Fact]
    public void AlterNullability_RecreatedTable_StillHasNamedIndex()
    {
        var before = BuildSnapshot(BlogTable);
        var afterTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Slug",   ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
                new ColumnSchema { Name = "Rating", ClrType = typeof(int).FullName!,    IsNullable = false, IndexName = "idx_Blog_Rating" }
            }
        };
        var after = BuildSnapshot(afterTable);
        var diff = SchemaDiffer.Diff(before, after);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Blog_Rating"));
    }

    // ─── DOWN migration also preserves constraints ─────────────────────────

    [Fact]
    public void AlterNullability_DownMigration_AlsoPreservesAllConstraints()
    {
        var before = BuildSnapshot(BlogTable);
        var afterTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Slug",   ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
                new ColumnSchema { Name = "Rating", ClrType = typeof(int).FullName!,    IsNullable = false, IndexName = "idx_Blog_Rating" }
            }
        };
        var after = BuildSnapshot(afterTable);
        var diff = SchemaDiffer.Diff(before, after);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // DOWN migration restores Rating back to nullable — table recreation must preserve PK/UNIQUE/INDEX
        var downCreate = sql.Down.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", downCreate);
        Assert.Contains("UNIQUE", downCreate);
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Blog_Rating"));
    }

    // ─── Snapshot diff: add column ────────────────────────────────────────

    [Fact]
    public void SnapshotDiff_AddedProperty_GeneratesAddColumnSql()
    {
        var before = BuildSnapshot(new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true },
                new ColumnSchema { Name = "Title", ClrType = typeof(string).FullName!, IsNullable = false }
            }
        });
        var after = BuildSnapshot(new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",      ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true },
                new ColumnSchema { Name = "Title",    ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Summary",  ClrType = typeof(string).FullName!, IsNullable = true }
            }
        });

        var diff = SchemaDiffer.Diff(before, after);
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("ADD COLUMN") && s.Contains("\"Summary\""));
    }

    // ─── Snapshot diff: drop column ────────────────────────────────────────

    [Fact]
    public void SnapshotDiff_DroppedProperty_GeneratesTableRecreation()
    {
        var before = BuildSnapshot(new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",      ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Post" },
                new ColumnSchema { Name = "Title",    ClrType = typeof(string).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Summary",  ClrType = typeof(string).FullName!, IsNullable = true }
            }
        });
        var after = BuildSnapshot(new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Post" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false }
            }
        });

        var diff = SchemaDiffer.Diff(before, after);
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // SQLite DROP COLUMN uses table-recreation workaround
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__Post"));
        Assert.Contains(sql.Up, s => s.StartsWith("DROP TABLE") && s.Contains("Post"));
        Assert.Contains(sql.Up, s => s.Contains("RENAME TO"));
        // "Summary" must not appear in the new table definition
        var tempCreate = sql.Up.First(s => s.StartsWith("CREATE TABLE") && s.Contains("__temp__Post"));
        Assert.DoesNotContain("Summary", tempCreate);
    }
}
