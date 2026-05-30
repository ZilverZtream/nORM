using System;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class MigrationCollationTests
{
    [Theory]
    [MemberData(nameof(Generators))]
    public void AddedTable_EmitsColumnCollation(IMigrationSqlGenerator generator, string provider, string collation)
    {
        var table = Table("CollatedCustomer", collation);
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("COLLATE", sql);
        Assert.Contains(provider == "postgres" ? $"\"{collation}\"" : collation, sql);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AddedColumn_EmitsColumnCollation(IMigrationSqlGenerator generator, string provider, string collation)
    {
        var table = Table("CollatedCustomer", collation);
        var column = new ColumnSchema
        {
            Name = "DisplayName",
            ClrType = typeof(string).FullName!,
            IsNullable = true,
            Collation = collation
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, column));

        var sql = string.Join(" ", generator.GenerateSql(diff).Up);

        Assert.Contains("COLLATE", sql);
        Assert.Contains(provider == "postgres" ? $"\"{collation}\"" : collation, sql);
    }

    [Fact]
    public void SchemaDiffer_DetectsCollationChange()
    {
        var oldTable = Table("CollatedCustomer", "NOCASE");
        var newTable = Table("CollatedCustomer", "BINARY");

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        var altered = Assert.Single(diff.AlteredColumns);
        Assert.Equal("Name", altered.NewColumn.Name);
        Assert.Equal("BINARY", altered.NewColumn.Collation);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void InvalidCollationIdentifier_ThrowsBeforeDdlInterpolation(IMigrationSqlGenerator generator, string provider, string collation)
    {
        Assert.False(string.IsNullOrWhiteSpace(provider));
        Assert.False(string.IsNullOrWhiteSpace(collation));
        var table = Table("CollatedCustomer", "NOCASE;DROP TABLE X");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        Assert.Throws<ArgumentException>(() => generator.GenerateSql(diff));
    }

    public static TheoryData<IMigrationSqlGenerator, string, string> Generators() => new()
    {
        { new SqliteMigrationSqlGenerator(), "sqlite", "NOCASE" },
        { new SqlServerMigrationSqlGenerator(), "sqlserver", "Latin1_General_100_CI_AS" },
        { new MySqlMigrationSqlGenerator(), "mysql", "utf8mb4_0900_ai_ci" },
        { new PostgresMigrationSqlGenerator(), "postgres", "C" }
    };

    private static TableSchema Table(string name, string collation) => new()
    {
        Name = name,
        Columns =
        {
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsUnique = true },
            new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false, Collation = collation }
        }
    };
}
