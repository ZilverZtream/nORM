using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SQL-text contract for temporal companion DDL on the three server migration generators
/// (SQL Server / PostgreSQL / MySQL). Their ALTER statements do not drop the versioning
/// triggers, but the triggers enumerate the main table's column list, so a column-set change
/// must re-emit them from the post-change schema and mirror the change onto the
/// <c>&lt;Table&gt;_History</c> companion in lock-step - otherwise the trigger INSERTs fail or
/// silently exclude the new column from history. Behavioural verification runs on the live
/// provider suites; this pins the emitted DDL shape.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ServerTemporalMigrationDdlContractTests
{
    private static TableSchema BuildTable(string name, bool withExtra, bool temporal = true, string? tenantColumn = null)
    {
        var t = new TableSchema { Name = name, IsTemporal = temporal, TenantColumnName = tenantColumn };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema { Name = "V", ClrType = typeof(int).FullName!, IsNullable = false });
        if (tenantColumn != null)
            t.Columns.Add(new ColumnSchema { Name = tenantColumn, ClrType = typeof(int).FullName!, IsNullable = false });
        if (withExtra)
            t.Columns.Add(new ColumnSchema { Name = "Extra", ClrType = typeof(int).FullName!, IsNullable = true });
        return t;
    }

    private static SchemaDiff AddExtraDiff(string table, bool temporal = true, string? tenantColumn = null)
        => SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(table, withExtra: false, temporal, tenantColumn) } },
            new SchemaSnapshot { Tables = { BuildTable(table, withExtra: true, temporal, tenantColumn) } });

    public static IEnumerable<object[]> Generators()
    {
        yield return new object[] { new SqlServerMigrationSqlGenerator(), "TemporalUpdate" };
        yield return new object[] { new PostgresMigrationSqlGenerator(), "_TemporalFunction" };
        yield return new object[] { new MySqlMigrationSqlGenerator(), "_au" };
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Added_column_mirrors_onto_history_and_regenerates_triggers(IMigrationSqlGenerator generator, string triggerMarker)
    {
        var sql = generator.GenerateSql(AddExtraDiff("TsrvAdd"));

        // Up: history gains the column and the trigger DDL enumerates it.
        Assert.Contains(sql.Up, s => s.Contains("TsrvAdd_History", StringComparison.Ordinal)
            && s.Contains("ADD", StringComparison.OrdinalIgnoreCase)
            && s.Contains("Extra", StringComparison.Ordinal));
        var upTrigger = sql.Up.Where(s => s.Contains(triggerMarker, StringComparison.Ordinal)
            && s.StartsWith("CREATE", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(upTrigger);
        // At least one regenerated body enumerates the new column (PostgreSQL's trigger
        // ATTACHMENT statement carries no column list; only its function body does).
        Assert.Contains(upTrigger, s => s.Contains("Extra", StringComparison.Ordinal));

        // Down: history loses the column and the trigger DDL no longer references it.
        Assert.Contains(sql.Down, s => s.Contains("TsrvAdd_History", StringComparison.Ordinal)
            && s.Contains("DROP COLUMN", StringComparison.OrdinalIgnoreCase)
            && s.Contains("Extra", StringComparison.Ordinal));
        var downTrigger = sql.Down.Where(s => s.Contains(triggerMarker, StringComparison.Ordinal)
            && s.StartsWith("CREATE", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(downTrigger);
        Assert.All(downTrigger, s => Assert.DoesNotContain("Extra", s, StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Non_temporal_tables_get_no_history_or_trigger_ddl(IMigrationSqlGenerator generator, string triggerMarker)
    {
        var sql = generator.GenerateSql(AddExtraDiff("TsrvPlain", temporal: false));

        Assert.DoesNotContain(sql.Up, s => s.Contains("_History", StringComparison.Ordinal));
        Assert.DoesNotContain(sql.Up, s => s.Contains(triggerMarker, StringComparison.Ordinal));
        Assert.DoesNotContain(sql.Down, s => s.Contains("_History", StringComparison.Ordinal));
        Assert.DoesNotContain(sql.Down, s => s.Contains(triggerMarker, StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Regenerated_triggers_keep_the_tenant_predicate(IMigrationSqlGenerator generator, string triggerMarker)
    {
        var sql = generator.GenerateSql(AddExtraDiff("TsrvTen", tenantColumn: "TenantId"));

        var upTrigger = sql.Up.Where(s => s.Contains(triggerMarker, StringComparison.Ordinal)
            && s.StartsWith("CREATE", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(upTrigger);
        // The history-close condition must scope to the tenant in every regenerated trigger
        // that closes rows (update/delete bodies reference OLD/deleted).
        Assert.Contains(upTrigger, s => s.Contains("TenantId", StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Added_temporal_table_creates_history_and_triggers(IMigrationSqlGenerator generator, string triggerMarker)
    {
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot(),
            new SchemaSnapshot { Tables = { BuildTable("TsrvNew", withExtra: false) } });
        var sql = generator.GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE", StringComparison.Ordinal)
            && s.Contains("TsrvNew_History", StringComparison.Ordinal)
            && s.Contains("__ValidFrom", StringComparison.Ordinal));
        Assert.Contains(sql.Up, s => s.Contains(triggerMarker, StringComparison.Ordinal)
            && s.StartsWith("CREATE", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s.Contains("TsrvNew_History", StringComparison.Ordinal)
            && s.StartsWith("DROP TABLE", StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Dropped_temporal_table_drops_history_and_down_restores_it(IMigrationSqlGenerator generator, string triggerMarker)
    {
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable("TsrvGone", withExtra: false) } },
            new SchemaSnapshot());
        var sql = generator.GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.Contains("TsrvGone_History", StringComparison.Ordinal)
            && s.StartsWith("DROP TABLE", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s.StartsWith("CREATE TABLE", StringComparison.Ordinal)
            && s.Contains("TsrvGone_History", StringComparison.Ordinal));
        Assert.Contains(sql.Down, s => s.Contains(triggerMarker, StringComparison.Ordinal)
            && s.StartsWith("CREATE", StringComparison.Ordinal));
    }
}
