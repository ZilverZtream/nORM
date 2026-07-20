using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SchemaDiffer must detect changes to the explicit store type (HasColumnType) and column comment
/// (HasComment) on an existing column, so incremental migrations don't silently miss them (schema drift).
/// A StoreType change re-emits the column type through each provider's ALTER/MODIFY path; a Comment change
/// re-emits the provider's native comment DDL (COMMENT ON / MODIFY … COMMENT / sp_*extendedproperty /
/// SQLite table recreate) on the up path and restores the prior comment on the down path.
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

    // ─── HasComment drift ─────────────────────────────────────────────────────

    [Fact]
    public void Comment_change_produces_altered_column()
    {
        var diff = SchemaDiffer.Diff(Snap(comment: "old note"), Snap(comment: "new note"));
        Assert.Single(diff.AlteredColumns);
    }

    [Fact]
    public void Comment_is_case_sensitive_for_drift()
    {
        // Comment text is user-facing; "Note" and "note" are different comments.
        var diff = SchemaDiffer.Diff(Snap(comment: "Note"), Snap(comment: "note"));
        Assert.Single(diff.AlteredColumns);
    }

    [Fact]
    public void Unchanged_comment_does_not_drift()
    {
        var diff = SchemaDiffer.Diff(Snap(comment: "the payload"), Snap(comment: "the payload"));
        Assert.Empty(diff.AlteredColumns);
    }

    [Fact]
    public void Postgres_comment_change_emits_comment_on_up_and_restores_on_down()
    {
        var diff = SchemaDiffer.Diff(Snap(comment: "old note"), Snap(comment: "new note"));
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("COMMENT ON COLUMN") && s.Contains("IS 'new note'"));
        Assert.Contains(sql.Down, s => s.Contains("COMMENT ON COLUMN") && s.Contains("IS 'old note'"));
    }

    [Fact]
    public void Postgres_comment_removed_emits_is_null()
    {
        var diff = SchemaDiffer.Diff(Snap(comment: "old note"), Snap(comment: null));
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("COMMENT ON COLUMN") && s.EndsWith("IS NULL"));
    }

    [Fact]
    public void MySql_comment_change_emits_modify_with_comment()
    {
        var diff = SchemaDiffer.Diff(Snap(comment: "old note"), Snap(comment: "new note"));
        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("MODIFY COLUMN") && s.Contains("COMMENT 'new note'"));
        Assert.Contains(sql.Down, s => s.Contains("MODIFY COLUMN") && s.Contains("COMMENT 'old note'"));
    }

    [Theory]
    [InlineData(null, "new note", "sp_addextendedproperty", "N'new note'")]     // add
    [InlineData("old note", "new note", "sp_updateextendedproperty", "N'new note'")] // update
    [InlineData("old note", null, "sp_dropextendedproperty", null)]              // drop
    public void SqlServer_comment_transition_uses_the_right_extended_property_proc(string? oldC, string? newC, string proc, string? valueFragment)
    {
        var diff = SchemaDiffer.Diff(Snap(comment: oldC), Snap(comment: newC));
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains(proc) && s.Contains("MS_Description"));
        if (valueFragment != null)
            Assert.Contains(sql.Up, s => s.Contains(valueFragment));
        else
            Assert.DoesNotContain(sql.Up, s => s.Contains(proc) && s.Contains("@value="));
    }

    [Fact]
    public void Sqlite_comment_change_recreates_table_with_new_comment()
    {
        var diff = SchemaDiffer.Diff(Snap(comment: "old note"), Snap(comment: "new note"));
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("/* new note */"));
    }
}
