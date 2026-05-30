using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class MigrationComputedColumnTests
{
    [Theory]
    [MemberData(nameof(Generators))]
    public void AddedTable_EmitsComputedColumn(IMigrationSqlGenerator generator, string provider)
    {
        var table = Table("ComputedProduct");
        table.Columns.Add(new ColumnSchema
        {
            Name = "Total",
            ClrType = typeof(decimal).FullName!,
            ComputedColumnSql = "Price * Quantity",
            IsStoredComputedColumn = provider is "postgres"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("Total", sql);
        Assert.Contains("Price * Quantity", sql);
        Assert.Contains(provider == "sqlserver" ? " AS " : "GENERATED ALWAYS AS", sql);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AddedComputedColumn_UsesProviderGeneratedColumnSyntax(IMigrationSqlGenerator generator, string provider)
    {
        var table = Table("ComputedProduct");
        var total = new ColumnSchema
        {
            Name = "Total",
            ClrType = typeof(decimal).FullName!,
            ComputedColumnSql = "Price * Quantity",
            IsStoredComputedColumn = provider is "postgres"
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, total));

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("ADD", sql);
        Assert.Contains("Total", sql);
        Assert.Contains("Price * Quantity", sql);
    }

    [Fact]
    public void SchemaDiffer_DetectsComputedColumnExpressionChange()
    {
        var oldTable = Table("ComputedProduct");
        oldTable.Columns.Add(new ColumnSchema
        {
            Name = "Total",
            ClrType = typeof(decimal).FullName!,
            ComputedColumnSql = "Price * Quantity"
        });
        var newTable = Table("ComputedProduct");
        newTable.Columns.Add(new ColumnSchema
        {
            Name = "Total",
            ClrType = typeof(decimal).FullName!,
            ComputedColumnSql = "(Price * Quantity) - Discount"
        });

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        var altered = Assert.Single(diff.AlteredColumns);
        Assert.Equal("Total", altered.NewColumn.Name);
        Assert.Equal("(Price * Quantity) - Discount", altered.NewColumn.ComputedColumnSql);
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
            new ColumnSchema { Name = "Quantity", ClrType = typeof(int).FullName!, IsNullable = false },
            new ColumnSchema { Name = "Discount", ClrType = typeof(decimal).FullName!, IsNullable = false, DefaultValue = "0" }
        }
    };
}
