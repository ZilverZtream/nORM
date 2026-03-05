using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
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

    // ─── Gate A integration: full lifecycle with closed-connection runner ─

    [Fact]
    public async Task GateA_FullLifecycle_ClosedConnection_HasPending_ApplyMigrations_HasPendingFalse()
    {
        // Gate A integration: verify that HasPendingMigrationsAsync works on a closed connection
        // and that the full lifecycle (check → apply → check again) works correctly.
        // This exercises the Gate A fix through realistic usage.
        var assembly = typeof(SqliteMigrationRunnerTests).Assembly;
        var cn = new SqliteConnection("Data Source=:memory:");
        // Connection is CLOSED — each runner method must open it automatically.

        try
        {
            await using var runner = new SqliteMigrationRunner(cn, assembly);

            // Step 1: Check HasPending on closed connection — must auto-open and return true
            Assert.Equal(ConnectionState.Closed, cn.State);
            var hasPending = await runner.HasPendingMigrationsAsync();
            Assert.True(hasPending, "Should have pending migrations before first apply");
            Assert.Equal(ConnectionState.Open, cn.State);

            // Step 2: GetPendingMigrations — already open, but method must handle that too
            var pendingNames = await runner.GetPendingMigrationsAsync();
            Assert.NotEmpty(pendingNames);

            // Step 3: Apply all migrations
            await runner.ApplyMigrationsAsync();

            // Step 4: HasPending must now return false (all applied)
            hasPending = await runner.HasPendingMigrationsAsync();
            Assert.False(hasPending, "HasPendingMigrationsAsync must return false after applying all");

            // Step 5: GetPendingMigrations must return empty array
            var remaining = await runner.GetPendingMigrationsAsync();
            Assert.Empty(remaining);
        }
        finally
        {
            await cn.DisposeAsync();
        }
    }

    [Fact]
    public async Task GateA_FullLifecycle_OpenConnection_WorksIdentically()
    {
        // Gate A regression guard: the full lifecycle must also work when the connection
        // is already open (no regression from the guard added for closed connections).
        var assembly = typeof(SqliteMigrationRunnerTests).Assembly;
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();  // Pre-opened connection

        await using var runner = new SqliteMigrationRunner(cn, assembly);

        var hasBefore = await runner.HasPendingMigrationsAsync();
        Assert.True(hasBefore);

        await runner.ApplyMigrationsAsync();

        var hasAfter = await runner.HasPendingMigrationsAsync();
        Assert.False(hasAfter);

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

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
