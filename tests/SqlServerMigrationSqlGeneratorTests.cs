using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// MIG-1: Validates that SqlServerMigrationSqlGenerator emits PRIMARY KEY, UNIQUE,
/// and CREATE INDEX DDL statements when table creation includes such columns.
/// </summary>
public class SqlServerMigrationSqlGeneratorTests
{
    private static TableSchema BuildTable(string name, params ColumnSchema[] columns)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in columns) t.Columns.Add(c);
        return t;
    }

    [Fact]
    public void CreateTable_WithPkColumn_EmitsPrimaryKeyConstraint()
    {
        var table = BuildTable("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
            new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("PRIMARY KEY", createStmt);
        Assert.Contains("[Id]", createStmt);
    }

    [Fact]
    public void CreateTable_WithUniqueNonPkColumn_EmitsUniqueConstraint()
    {
        var table = BuildTable("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
            new ColumnSchema { Name = "Email", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("UNIQUE", createStmt);
        Assert.Contains("[Email]", createStmt);
    }

    [Fact]
    public void CreateTable_WithIndexColumn_EmitsSeparateCreateIndex()
    {
        var table = BuildTable("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
            new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Users_Name" }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE INDEX") && s.Contains("idx_Users_Name") && s.Contains("[Users]") && s.Contains("[Name]"));
    }

    [Fact]
    public void CreateTable_WithoutConstraints_DoesNotEmitConstraintClauses()
    {
        var table = BuildTable("Plain",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false },
            new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = true }
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.DoesNotContain("PRIMARY KEY", createStmt);
        Assert.DoesNotContain("UNIQUE", createStmt);
        Assert.DoesNotContain(sql.Up, s => s.StartsWith("CREATE INDEX"));
    }

    [Fact]
    public void DropTable_DownMigration_JustDropsTable()
    {
        var table = BuildTable("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true }
        );
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("DROP TABLE") && s.Contains("Users"));
    }
}
