using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// M1: Verifies that SchemaDiffer.Diff() detects DefaultValue changes and that migration
/// generators emit the appropriate SQL to apply and roll back those changes.
/// </summary>
public class MigrationDefaultValueDriftTests
{
    private static TableSchema MakeTable(string tableName, string colName, string? defaultValue)
    {
        var table = new TableSchema { Name = tableName };
        table.Columns.Add(new ColumnSchema
        {
            Name = colName,
            ClrType = typeof(int).FullName!,
            IsNullable = true,
            DefaultValue = defaultValue
        });
        return table;
    }

    private static (SchemaSnapshot old, SchemaSnapshot @new) MakeSnapshots(string? oldDefault, string? newDefault)
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("Widgets", "Score", oldDefault));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("Widgets", "Score", newDefault));

        return (oldSnap, newSnap);
    }

    // ─── Diff predicate tests ─────────────────────────────────────────────────

    [Fact]
    public void SchemaDiff_DefaultValueAdded_ProducesAlteredColumn()
    {
        var (old, @new) = MakeSnapshots(null, "0");
        var diff = SchemaDiffer.Diff(old, @new);
        Assert.Single(diff.AlteredColumns);
        Assert.Equal("Score", diff.AlteredColumns[0].NewColumn.Name);
    }

    [Fact]
    public void SchemaDiff_DefaultValueRemoved_ProducesAlteredColumn()
    {
        var (old, @new) = MakeSnapshots("0", null);
        var diff = SchemaDiffer.Diff(old, @new);
        Assert.Single(diff.AlteredColumns);
    }

    [Fact]
    public void SchemaDiff_DefaultValueUnchanged_NoDrift()
    {
        var (old, @new) = MakeSnapshots("42", "42");
        var diff = SchemaDiffer.Diff(old, @new);
        Assert.Empty(diff.AlteredColumns);
    }

    [Fact]
    public void SchemaDiff_DefaultValueChanged_ProducesAlteredColumn()
    {
        var (old, @new) = MakeSnapshots("0", "42");
        var diff = SchemaDiffer.Diff(old, @new);
        Assert.Single(diff.AlteredColumns);
    }

    [Fact]
    public void SchemaDiff_BothNullDefaults_NoDrift()
    {
        var (old, @new) = MakeSnapshots(null, null);
        var diff = SchemaDiffer.Diff(old, @new);
        Assert.Empty(diff.AlteredColumns);
    }

    // ─── SQL generator tests ──────────────────────────────────────────────────

    [Fact]
    public void Postgres_DefaultValueAdded_EmitsSetDefault()
    {
        var (old, @new) = MakeSnapshots(null, "0");
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("SET DEFAULT 0"));
    }

    [Fact]
    public void Postgres_DefaultValueRemoved_EmitsDropDefault()
    {
        var (old, @new) = MakeSnapshots("0", null);
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("DROP DEFAULT"));
    }

    [Fact]
    public void Postgres_DefaultValueChanged_EmitsSetDefault()
    {
        var (old, @new) = MakeSnapshots("0", "42");
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("SET DEFAULT 42"));
        // Down must restore old value
        Assert.Contains(sql.Down, s => s.Contains("SET DEFAULT 0"));
    }

    [Fact]
    public void MySQL_DefaultValueAdded_EmitsModifyColumnWithDefault()
    {
        var (old, @new) = MakeSnapshots(null, "0");
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("MODIFY COLUMN") && s.Contains("DEFAULT 0"));
    }

    [Fact]
    public void MySQL_DefaultValueRemoved_EmitsModifyColumnWithoutDefault()
    {
        var (old, @new) = MakeSnapshots("0", null);
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("MODIFY COLUMN") && !s.Contains("DEFAULT"));
    }

    [Fact]
    public void SqlServer_DefaultValueAdded_EmitsAddConstraint()
    {
        var (old, @new) = MakeSnapshots(null, "0");
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains("ADD CONSTRAINT") && s.Contains("DEFAULT (0)"));
    }

    [Fact]
    public void SqlServer_DefaultValueRemoved_EmitsDropConstraintOnly()
    {
        var (old, @new) = MakeSnapshots("0", null);
        var diff = SchemaDiffer.Diff(old, @new);
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);
        // Should have the DECLARE/DROP block but no ADD CONSTRAINT for the up direction
        Assert.Contains(sql.Up, s => s.Contains("DROP CONSTRAINT") || s.Contains("IF @__df_"));
        Assert.DoesNotContain(sql.Up, s => s.Contains("ADD CONSTRAINT") && s.Contains("DEFAULT"));
    }
}
