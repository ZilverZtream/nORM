using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SchemaDiffer must detect a change to the explicit store type (HasColumnType) on an existing column,
/// so incremental migrations don't silently miss it (schema drift). A StoreType change must also
/// re-emit the column type through each provider's ALTER/MODIFY path.
/// </summary>
[Trait("Category", "Fast")]
public class MigrationColumnMetadataDriftTests
{
    private static SchemaSnapshot Snap(string? storeType = null, string? comment = null)
    {
        var table = new TableSchema { Name = "Widgets" };
        table.Columns.Add(new ColumnSchema
        {
            Name = "Score",
            ClrType = typeof(decimal).FullName!,
            IsNullable = true,
            StoreType = storeType,
            Comment = comment,
        });
        return new SchemaSnapshot { Tables = { table } };
    }

    [Fact]
    public void StoreType_change_produces_altered_column()
    {
        var diff = SchemaDiffer.Diff(Snap(storeType: "DECIMAL(18,2)"), Snap(storeType: "DECIMAL(20,4)"));
        Assert.Single(diff.AlteredColumns);
    }

    [Fact]
    public void StoreType_change_only_case_does_not_drift()
    {
        var diff = SchemaDiffer.Diff(Snap(storeType: "DECIMAL(18,2)"), Snap(storeType: "decimal(18,2)"));
        Assert.Empty(diff.AlteredColumns);
    }

    [Theory]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    public void StoreType_change_reemits_new_type_on_alter(string provider)
    {
        var diff = SchemaDiffer.Diff(Snap(storeType: "DECIMAL(18,2)"), Snap(storeType: "DECIMAL(20,4)"));
        IMigrationSqlGenerator gen = provider switch
        {
            "postgres" => new PostgresMigrationSqlGenerator(),
            "sqlserver" => new SqlServerMigrationSqlGenerator(),
            "mysql" => new MySqlMigrationSqlGenerator(),
            _ => throw new System.ArgumentOutOfRangeException(nameof(provider)),
        };
        var sql = gen.GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("DECIMAL(20,4)"));
    }

    [Fact]
    public void Unchanged_store_type_does_not_drift()
    {
        var diff = SchemaDiffer.Diff(Snap(storeType: "JSONB"), Snap(storeType: "JSONB"));
        Assert.Empty(diff.AlteredColumns);
    }
}
