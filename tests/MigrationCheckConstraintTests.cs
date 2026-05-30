using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class MigrationCheckConstraintTests
{
    [Theory]
    [MemberData(nameof(Generators))]
    public void AddedTable_EmitsCheckConstraint(IMigrationSqlGenerator generator, string _)
    {
        var table = Table("ConstrainedProduct");
        table.CheckConstraints.Add(new CheckConstraintSchema
        {
            ConstraintName = "CK_ConstrainedProduct_Price",
            Sql = "Price >= 0"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("CK_ConstrainedProduct_Price", sql);
        Assert.Contains("CHECK", sql);
        Assert.Contains("Price >= 0", sql);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void ExistingTable_CheckConstraintDiff_IsReversible(IMigrationSqlGenerator generator, string provider)
    {
        var table = Table("ConstrainedProduct");
        var check = new CheckConstraintSchema
        {
            ConstraintName = "CK_ConstrainedProduct_Price",
            Sql = "Price >= 0"
        };
        table.CheckConstraints.Add(check);
        var diff = new SchemaDiff();
        diff.AddedCheckConstraints.Add((table, check));

        var statements = generator.GenerateSql(diff);
        var up = string.Join(" ", statements.Up);
        var down = string.Join(" ", statements.Down);

        if (provider == "sqlite")
        {
            Assert.Contains("__temp__ConstrainedProduct", up);
            Assert.Contains("CHECK", up);
            Assert.Contains("__temp__ConstrainedProduct", down);
        }
        else
        {
            Assert.Contains("ALTER TABLE", up);
            Assert.Contains("ADD", up);
            Assert.Contains("CHECK", up);
            Assert.Contains("DROP", down);
            Assert.Contains("CK_ConstrainedProduct_Price", down);
        }
    }

    [Fact]
    public void SchemaDiffer_DetectsAddedChangedAndDroppedCheckConstraints()
    {
        var oldTable = Table("ConstrainedProduct");
        oldTable.CheckConstraints.Add(new CheckConstraintSchema
        {
            ConstraintName = "CK_ConstrainedProduct_Price",
            Sql = "Price >= 0"
        });
        var newTable = Table("ConstrainedProduct");
        newTable.CheckConstraints.Add(new CheckConstraintSchema
        {
            ConstraintName = "CK_ConstrainedProduct_Price",
            Sql = "Price > 0"
        });
        newTable.CheckConstraints.Add(new CheckConstraintSchema
        {
            ConstraintName = "CK_ConstrainedProduct_Name",
            Sql = "length(Name) > 0"
        });

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Contains(diff.DroppedCheckConstraints, item => item.CheckConstraint.ConstraintName == "CK_ConstrainedProduct_Price");
        Assert.Contains(diff.AddedCheckConstraints, item => item.CheckConstraint.ConstraintName == "CK_ConstrainedProduct_Price" && item.CheckConstraint.Sql == "Price > 0");
        Assert.Contains(diff.AddedCheckConstraints, item => item.CheckConstraint.ConstraintName == "CK_ConstrainedProduct_Name");
    }

    [Fact]
    public void SqliteLive_AddTableWithCheckConstraint_EnforcesConstraint()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var table = Table("ConstrainedProduct");
        table.CheckConstraints.Add(new CheckConstraintSchema
        {
            ConstraintName = "CK_ConstrainedProduct_Price",
            Sql = "Price >= 0"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        foreach (var statement in new SqliteMigrationSqlGenerator().GenerateSql(diff).Up)
            Execute(cn, statement);

        Execute(cn, "INSERT INTO ConstrainedProduct (Id, Price, Name) VALUES (1, 1.25, 'ok')");
        var ex = Assert.Throws<SqliteException>(() =>
            Execute(cn, "INSERT INTO ConstrainedProduct (Id, Price, Name) VALUES (2, -1, 'bad')"));
        Assert.Contains("CHECK", ex.Message);
    }

    public static TheoryData<IMigrationSqlGenerator, string> Generators() => new()
    {
        { new SqliteMigrationSqlGenerator(), "sqlite" },
        { new SqlServerMigrationSqlGenerator(), "sqlserver" },
        { new MySqlMigrationSqlGenerator(), "mysql" },
        { new PostgresMigrationSqlGenerator(), "postgres" }
    };

    private static TableSchema Table(string name) => new()
    {
        Name = name,
        Columns =
        {
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsUnique = true },
            new ColumnSchema { Name = "Price", ClrType = typeof(decimal).FullName!, IsNullable = false },
            new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false }
        }
    };

    private static void Execute(SqliteConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
