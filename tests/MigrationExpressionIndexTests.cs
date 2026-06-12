using System;
using nORM.Configuration;
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
            ExpressionSql = provider switch
            {
                "postgres" => "lower(\"Email\")",
                "mysql" => "(LOWER(`Email`))",
                _ => "lower(Email)"
            },
            FilterSql = provider == "mysql"
                ? null
                : provider == "postgres" ? "\"Email\" IS NOT NULL" : "Email IS NOT NULL"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("IX_ExpressionIndexedCustomer_LowerEmail", sql);
        Assert.Contains("lower", sql, StringComparison.OrdinalIgnoreCase);
        if (provider == "mysql")
            Assert.DoesNotContain("WHERE", sql);
        else
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
    public void PostgresGenerator_EmitsExpressionIndexFacets()
    {
        var table = Table("ExpressionIndexedCustomer");
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "lower(\"Email\")",
            IsUnique = true,
            FilterSql = "\"Email\" IS NOT NULL",
            IncludedColumnNames = new[] { "Score" },
            NullSortOrder = IndexNullSortOrder.First,
            NullsNotDistinct = true
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = string.Join(" ", new PostgresMigrationSqlGenerator().GenerateSql(diff).Up);

        Assert.Contains("CREATE UNIQUE INDEX", sql);
        Assert.Contains("(lower(\"Email\") NULLS FIRST)", sql);
        Assert.Contains("INCLUDE (\"Score\")", sql);
        Assert.Contains("NULLS NOT DISTINCT", sql);
        Assert.Contains("WHERE \"Email\" IS NOT NULL", sql);
    }

    [Theory]
    [MemberData(nameof(ExpressionFacetUnsupportedGenerators))]
    public void UnsupportedGenerators_WithExpressionIndexFacets_Throw(IMigrationSqlGenerator generator, string provider)
    {
        var table = Table("ExpressionIndexedCustomer");
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "lower(Email)",
            IncludedColumnNames = new[] { "Score" }
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => generator.GenerateSql(diff));
        Assert.Contains(provider, ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INCLUDE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySqlGenerator_WithFilteredExpressionIndex_Throws()
    {
        var table = Table("ExpressionIndexedCustomer");
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "(LOWER(`Email`))",
            FilterSql = "`Email` IS NOT NULL"
        });
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => new MySqlMigrationSqlGenerator().GenerateSql(diff));
        Assert.Contains("filtered indexes", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    [Fact]
    public void SchemaDiffer_DetectsExpressionIndexFacetChanges()
    {
        var oldTable = Table("ExpressionIndexedCustomer");
        oldTable.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "lower(Email)",
            IsUnique = true
        });
        var newTable = Table("ExpressionIndexedCustomer");
        newTable.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_ExpressionIndexedCustomer_LowerEmail",
            ExpressionSql = "lower(Email)",
            IsUnique = true,
            IncludedColumnNames = new[] { "Score" },
            NullSortOrder = IndexNullSortOrder.Last,
            NullsNotDistinct = true
        });

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Single(diff.DroppedExpressionIndexes);
        var added = Assert.Single(diff.AddedExpressionIndexes);
        Assert.Equal(new[] { "Score" }, added.ExpressionIndex.IncludedColumnNames);
        Assert.Equal(IndexNullSortOrder.Last, added.ExpressionIndex.NullSortOrder);
        Assert.True(added.ExpressionIndex.NullsNotDistinct);
    }

    public static TheoryData<IMigrationSqlGenerator, string> SupportedGenerators() => new()
    {
        { new SqliteMigrationSqlGenerator(), "sqlite" },
        { new PostgresMigrationSqlGenerator(), "postgres" },
        { new MySqlMigrationSqlGenerator(), "mysql" }
    };

    public static TheoryData<IMigrationSqlGenerator, string> UnsupportedGenerators() => new()
    {
        { new SqlServerMigrationSqlGenerator(), "SQL Server" }
    };

    public static TheoryData<IMigrationSqlGenerator, string> ExpressionFacetUnsupportedGenerators() => new()
    {
        { new SqliteMigrationSqlGenerator(), "SQLite" },
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
