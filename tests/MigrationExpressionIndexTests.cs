using System;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class MigrationExpressionIndexTests
{
    [Theory]
    [MemberData(nameof(SupportedGenerators))]
    public void SupportedGenerators_EmitExpressionIndex(IMigrationSqlGenerator generator, string provider)
    {
        var table = Table("ExpressionIndexedCustomer");
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = provider == "postgres" ? "lower(\"Email\")" : "lower(Email)",
            FilterSql = provider == "postgres" ? "\"Email\" IS NOT NULL" : "Email IS NOT NULL"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("IX_ExpressionIndexedCustomer_LowerEmail", sql);
        Assert.Contains("lower", sql);
        Assert.Contains("WHERE", sql);
    }

    [Theory]
    [MemberData(nameof(UnsupportedGenerators))]
    public void UnsupportedGenerators_ThrowActionableExpressionIndexError(IMigrationSqlGenerator generator, string provider)
    {
        var table = Table("ExpressionIndexedCustomer");
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "lower(Email)"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => generator.GenerateSql(diff));
        Assert.Contains("expression index", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(provider, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SchemaDiffer_DetectsExpressionIndexChanges()
    {
        var oldTable = Table("ExpressionIndexedCustomer");
        oldTable.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "lower(Email)"
        });
        var newTable = Table("ExpressionIndexedCustomer");
        newTable.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "upper(Email)"
        });

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Single(diff.DroppedExpressionIndexes);
        var added = Assert.Single(diff.AddedExpressionIndexes);
        Assert.Equal("upper(Email)", added.ExpressionIndex.ExpressionSql);
    }

    public static TheoryData<IMigrationSqlGenerator, string> SupportedGenerators() => new()
    {
        { new SqliteMigrationSqlGenerator(), "sqlite" },
        { new PostgresMigrationSqlGenerator(), "postgres" }
    };

    public static TheoryData<IMigrationSqlGenerator, string> UnsupportedGenerators() => new()
    {
        { new SqlServerMigrationSqlGenerator(), "SQL Server" },
        { new MySqlMigrationSqlGenerator(), "MySQL" }
    };

    private static TableSchema Table(string name) => new()
    {
        Name = name,
        Columns =
        {
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsUnique = true },
            new ColumnSchema { Name = "Email", ClrType = typeof(string).FullName!, IsNullable = false }
        }
    };
}
