using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SQLite's ALTER TABLE ADD COLUMN cannot add a STORED generated column (only VIRTUAL). The migration
/// generator emitted the ADD-COLUMN form regardless, so adding — or rolling back the drop of — a STORED
/// computed column produced DDL that SQLite rejects at apply time. A STORED computed column must route
/// through the table recreate (whose CREATE TABLE accepts STORED), preserving the existing data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SqliteMigrationStoredComputedColumnApplyTests
{
    private static TableSchema TableWithComputed(bool stored)
    {
        return new TableSchema
        {
            Name = "ScmT",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsUnique = true },
                new ColumnSchema { Name = "Qty", ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Doubled", ClrType = typeof(int).FullName!, ComputedColumnSql = "Qty * 2", IsStoredComputedColumn = stored }
            }
        };
    }

    private static void ApplyAndAssert(bool stored)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE ScmT (Id INTEGER PRIMARY KEY, Qty INTEGER NOT NULL);" +
                "INSERT INTO ScmT (Id, Qty) VALUES (1, 5), (2, 9);";
            cmd.ExecuteNonQuery();
        }

        var table = TableWithComputed(stored);
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, table.Columns.Single(c => c.Name == "Doubled")));

        var statements = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        var empty = System.Linq.Enumerable.Empty<string>();
        foreach (var sql in (statements.PreTransactionUp ?? empty).Concat(statements.Up ?? empty).Concat(statements.PostTransactionUp ?? empty))
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        using (var check = cn.CreateCommand())
        {
            // The existing rows survived and the computed column is populated.
            check.CommandText = "SELECT Doubled FROM ScmT WHERE Id = 1;";
            Assert.Equal(10L, (long)check.ExecuteScalar()!);
            check.CommandText = "SELECT Qty FROM ScmT WHERE Id = 2;";
            Assert.Equal(9L, (long)check.ExecuteScalar()!);
            check.CommandText = "SELECT COUNT(*) FROM ScmT;";
            Assert.Equal(2L, (long)check.ExecuteScalar()!);
        }
    }

    [Fact]
    public void Adding_a_stored_computed_column_applies_and_preserves_data()
        => ApplyAndAssert(stored: true);

    [Fact]
    public void Adding_a_virtual_computed_column_still_applies()
        => ApplyAndAssert(stored: false);
}
