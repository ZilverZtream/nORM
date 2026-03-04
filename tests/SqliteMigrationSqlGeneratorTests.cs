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
}

