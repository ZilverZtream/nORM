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
}

