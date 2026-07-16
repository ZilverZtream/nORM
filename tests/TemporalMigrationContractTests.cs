using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract: migrations on trigger-emulated temporal tables keep versioning ALIVE and the history
/// companion in lock-step. A SQLite recreate previously dropped the three versioning triggers -
/// every post-migration write silently skipped history until some future context re-ran the
/// bootstrap, leaving versions permanently missing from the chain - and a plain ADD COLUMN left
/// the triggers enumerating the old column list, silently excluding the new column from history
/// forever. These tests drive real schema diffs through the SQLite generator against live data
/// and pin: versioning continues IMMEDIATELY after Up (no bootstrap, no missing version), the
/// history schema mirrors the main table (SQL Server system-versioning parity), AsOf spans the
/// migration, and Down restores the pre-migration temporal machinery.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalMigrationContractTests
{
    private static TableSchema BuildTable(string name, bool withExtra, string extraName = "Extra")
    {
        var t = new TableSchema { Name = name, IsTemporal = true };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema { Name = "V", ClrType = typeof(int).FullName!, IsNullable = false });
        if (withExtra) t.Columns.Add(new ColumnSchema { Name = extraName, ClrType = typeof(int).FullName!, IsNullable = true });
        return t;
    }

    private static void Apply(SqliteConnection cn, IEnumerable<string>? statements)
    {
        foreach (var statement in statements ?? Enumerable.Empty<string>())
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = statement;
            cmd.ExecuteNonQuery();
        }
    }

    private static void ApplyUp(SqliteConnection cn, MigrationSqlStatements sql)
    {
        Apply(cn, sql.PreTransactionUp);
        Apply(cn, sql.Up);
        Apply(cn, sql.PostTransactionUp);
    }

    private static void ApplyDown(SqliteConnection cn, MigrationSqlStatements sql)
    {
        Apply(cn, sql.PreTransactionDown);
        Apply(cn, sql.Down);
        Apply(cn, sql.PostTransactionDown);
    }

    private static long Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    private static string TriggerSql(SqliteConnection cn, string triggerName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT sql FROM sqlite_master WHERE type='trigger' AND name='{triggerName}';";
        return (string?)cmd.ExecuteScalar() ?? "";
    }

    // ── DROP COLUMN (recreate) ────────────────────────────────────────────────────────────

    [Table("TmigDrop")]
    private class DropRowBefore
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public int? Extra { get; set; }
    }

    [Table("TmigDrop")]
    private class DropRowAfter
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Fact]
    public async Task Recreate_migration_keeps_versioning_alive_with_no_missing_version()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmigDrop (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Extra INTEGER NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<DropRowBefore>() };
        opts.EnableTemporalVersioning();
        using var seedCtx = new DbContext(cn, new SqliteProvider(), opts);
        var row = new DropRowBefore { Id = 1, V = 1, Extra = 7 };
        seedCtx.Add(row);
        await seedCtx.SaveChangesAsync();
        await Task.Delay(150);
        var betweenV1V2 = DateTime.UtcNow;
        await Task.Delay(150);
        row.V = 2;
        await seedCtx.SaveChangesAsync();

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable("TmigDrop", withExtra: true) } },
            new SchemaSnapshot { Tables = { BuildTable("TmigDrop", withExtra: false) } });
        ApplyUp(cn, new SqliteMigrationSqlGenerator().GenerateSql(diff));

        // The versioning triggers survived the recreate and reflect the NEW schema.
        var aiSql = TriggerSql(cn, "TmigDrop_ai");
        Assert.False(string.IsNullOrEmpty(aiSql), "insert trigger missing after recreate migration");
        Assert.DoesNotContain("Extra", aiSql, StringComparison.OrdinalIgnoreCase);

        // THE core contract: the very next write is versioned - no bootstrap, no missing version.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE TmigDrop SET V = 3 WHERE Id = 1;";
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
        Assert.Equal(3, Scalar(cn, "SELECT COUNT(*) FROM TmigDrop_History;"));
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM TmigDrop_History WHERE V = 3;"));

        // History was recreated in lock-step: the dropped column is gone from it.
        Assert.Equal(0, Scalar(cn,
            "SELECT COUNT(*) FROM pragma_table_info('TmigDrop_History') WHERE name = 'Extra';"));
        // The surviving columns' historical values are intact.
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM TmigDrop_History WHERE V = 1;"));

        // AsOf spans the migration through a post-migration model.
        var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<DropRowAfter>() };
        opts2.EnableTemporalVersioning();
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);
        var old = await ctx2.Query<DropRowAfter>().AsOf(betweenV1V2).Where(r => r.Id == 1).ToListAsync();
        Assert.Equal(1, old.Single().V);
    }

    // ── ADD COLUMN (plain ALTER) ──────────────────────────────────────────────────────────

    [Table("TmigAdd")]
    private class AddRowBefore
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("TmigAdd")]
    private class AddRowAfter
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public int? W { get; set; }
    }

    [Fact]
    public async Task Added_column_values_are_recorded_in_history_from_the_first_write()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmigAdd (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<AddRowBefore>() };
        opts.EnableTemporalVersioning();
        using var seedCtx = new DbContext(cn, new SqliteProvider(), opts);
        var row = new AddRowBefore { Id = 1, V = 1 };
        seedCtx.Add(row);
        await seedCtx.SaveChangesAsync();
        await Task.Delay(150);
        var betweenV1V2 = DateTime.UtcNow;
        await Task.Delay(150);
        row.V = 2;
        await seedCtx.SaveChangesAsync();

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable("TmigAdd", withExtra: false) } },
            new SchemaSnapshot { Tables = { BuildTable("TmigAdd", withExtra: true, extraName: "W") } });
        ApplyUp(cn, new SqliteMigrationSqlGenerator().GenerateSql(diff));

        // History gained the column in lock-step; the pre-migration rows carry NULL.
        Assert.Equal(1, Scalar(cn,
            "SELECT COUNT(*) FROM pragma_table_info('TmigAdd_History') WHERE name = 'W';"));
        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM TmigAdd_History WHERE W IS NULL;"));

        // The triggers were re-emitted from the NEW schema: the very next write records W.
        Assert.Contains("W", TriggerSql(cn, "TmigAdd_ai"), StringComparison.Ordinal);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE TmigAdd SET V = 3, W = 42 WHERE Id = 1;";
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM TmigAdd_History WHERE V = 3 AND W = 42;"));

        // AsOf both sides of the migration through the post-migration model.
        var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<AddRowAfter>() };
        opts2.EnableTemporalVersioning();
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);
        var old = (await ctx2.Query<AddRowAfter>().AsOf(betweenV1V2).Where(r => r.Id == 1).ToListAsync()).Single();
        Assert.Equal(1, old.V);
        Assert.Null(old.W);
        var current = (await ctx2.Query<AddRowAfter>().AsOf(DateTime.UtcNow.AddSeconds(1)).Where(r => r.Id == 1).ToListAsync()).Single();
        Assert.Equal(3, current.V);
        Assert.Equal(42, current.W);
    }

    // ── RENAME COLUMN ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Renamed_column_renames_in_history_and_versioning_continues()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmigRen (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Old INTEGER NULL)";
            cmd.ExecuteNonQuery();
        }
        // Seed history through raw DDL + a bootstrap-equivalent trigger set, then two versions.
        var oldTable = BuildTable("TmigRen", withExtra: true, extraName: "Old");
        var newTable = BuildTable("TmigRen", withExtra: true, extraName: "Renamed");
        newTable.Columns.Single(c => c.Name == "Renamed").PreviousName = "Old";

        var optsSeed = new DbContextOptions { OnModelCreating = mb => mb.Entity<RenRowBefore>() };
        optsSeed.EnableTemporalVersioning();
        using var seedCtx = new DbContext(cn, new SqliteProvider(), optsSeed);
        var row = new RenRowBefore { Id = 1, V = 1, Old = 5 };
        seedCtx.Add(row);
        await seedCtx.SaveChangesAsync();
        await Task.Delay(150);
        row.V = 2;
        await seedCtx.SaveChangesAsync();

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });
        ApplyUp(cn, new SqliteMigrationSqlGenerator().GenerateSql(diff));

        // History followed the rename and kept its data under the new name.
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM pragma_table_info('TmigRen_History') WHERE name = 'Old';"));
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM pragma_table_info('TmigRen_History') WHERE name = 'Renamed';"));
        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM TmigRen_History WHERE Renamed = 5;"));

        // Triggers reference the new name; the next write is versioned.
        Assert.Contains("Renamed", TriggerSql(cn, "TmigRen_au"), StringComparison.Ordinal);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE TmigRen SET V = 3 WHERE Id = 1;";
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
        Assert.Equal(3, Scalar(cn, "SELECT COUNT(*) FROM TmigRen_History;"));
    }

    [Table("TmigRen")]
    private class RenRowBefore
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public int? Old { get; set; }
    }

    // ── DOWN ──────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Down_restores_history_shape_and_versioning_continues()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmigDown (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Extra INTEGER NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<DownRow>() };
        opts.EnableTemporalVersioning();
        using var seedCtx = new DbContext(cn, new SqliteProvider(), opts);
        var row = new DownRow { Id = 1, V = 1, Extra = 9 };
        seedCtx.Add(row);
        await seedCtx.SaveChangesAsync();
        await Task.Delay(150);
        row.V = 2;
        await seedCtx.SaveChangesAsync();

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable("TmigDown", withExtra: true) } },
            new SchemaSnapshot { Tables = { BuildTable("TmigDown", withExtra: false) } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        ApplyUp(cn, sql);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE TmigDown SET V = 3 WHERE Id = 1;";
            cmd.ExecuteNonQuery();
        }

        ApplyDown(cn, sql);

        // History has the column back (values were destroyed by Up - restored as NULL fill).
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM pragma_table_info('TmigDown_History') WHERE name = 'Extra';"));
        // Triggers were regenerated from the restored schema and versioning continues.
        Assert.Contains("Extra", TriggerSql(cn, "TmigDown_au"), StringComparison.Ordinal);
        var before = Scalar(cn, "SELECT COUNT(*) FROM TmigDown_History;");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE TmigDown SET V = 4, Extra = 1 WHERE Id = 1;";
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
        Assert.Equal(before + 1, Scalar(cn, "SELECT COUNT(*) FROM TmigDown_History;"));
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM TmigDown_History WHERE V = 4 AND Extra = 1;"));
    }

    [Table("TmigDown")]
    private class DownRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public int? Extra { get; set; }
    }

    // ── ADDED / DROPPED temporal tables ───────────────────────────────────────────────────

    [Fact]
    public void Added_temporal_table_creates_history_and_triggers_in_the_same_migration()
    {
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot(),
            new SchemaSnapshot { Tables = { BuildTable("TmigNew", withExtra: false) } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        ApplyUp(cn, sql);

        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='TmigNew_History';"));
        Assert.Equal(3, Scalar(cn, "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'TmigNew!_a%' ESCAPE '!';"));

        // Versioning works from the very first write.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO TmigNew (Id, V) VALUES (1, 1);";
            cmd.ExecuteNonQuery();
        }
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM TmigNew_History WHERE V = 1 AND __Operation = 'I';"));

        // Down removes the history companion along with the table.
        ApplyDown(cn, sql);
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('TmigNew', 'TmigNew_History');"));
    }

    [Fact]
    public void Dropped_temporal_table_drops_history_and_down_restores_both()
    {
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable("TmigGone", withExtra: false) } },
            new SchemaSnapshot());
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmigGone (Id INTEGER PRIMARY KEY AUTOINCREMENT, V INTEGER NOT NULL);" +
                              "CREATE TABLE TmigGone_History (__VersionId INTEGER PRIMARY KEY AUTOINCREMENT, __ValidFrom TEXT NOT NULL, __ValidTo TEXT NOT NULL, __Operation TEXT NOT NULL, Id INTEGER NOT NULL, V INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        ApplyUp(cn, sql);
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('TmigGone', 'TmigGone_History');"));

        ApplyDown(cn, sql);
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='TmigGone_History';"));
        Assert.Equal(3, Scalar(cn, "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'TmigGone!_a%' ESCAPE '!';"));
    }

    // ── Tenant-scoped temporal tables ─────────────────────────────────────────────────────

    [Fact]
    public void Regenerated_triggers_keep_the_tenant_predicate_in_the_history_close()
    {
        TableSchema Tenant(bool withExtra)
        {
            var t = BuildTable("TmigTen", withExtra);
            t.TenantColumnName = "TenantId";
            t.Columns.Add(new ColumnSchema { Name = "TenantId", ClrType = typeof(int).FullName!, IsNullable = false });
            return t;
        }
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { Tenant(true) } },
            new SchemaSnapshot { Tables = { Tenant(false) } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // Both directions' regenerated update triggers must scope the history close to the
        // tenant - the history table is not PK-unique, so a lost tenant predicate could close
        // another tenant's open history row.
        var upTrigger = sql.Up.Single(s => s.StartsWith("CREATE TRIGGER", StringComparison.Ordinal) && s.Contains("TmigTen_au", StringComparison.Ordinal));
        Assert.Contains("\"TenantId\" = OLD.\"TenantId\"", upTrigger, StringComparison.Ordinal);
        var downTrigger = sql.Down.Single(s => s.StartsWith("CREATE TRIGGER", StringComparison.Ordinal) && s.Contains("TmigTen_au", StringComparison.Ordinal));
        Assert.Contains("\"TenantId\" = OLD.\"TenantId\"", downTrigger, StringComparison.Ordinal);
    }

    // ── Non-temporal tables stay untouched ────────────────────────────────────────────────

    [Fact]
    public void Non_temporal_tables_get_no_history_or_trigger_ddl()
    {
        TableSchema Plain(bool withExtra)
        {
            var t = BuildTable("TmigPlain", withExtra);
            t.IsTemporal = false;
            return t;
        }
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { Plain(true) } },
            new SchemaSnapshot { Tables = { Plain(false) } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.DoesNotContain(sql.Up, s => s.Contains("_History", StringComparison.Ordinal));
        Assert.DoesNotContain(sql.Up, s => s.Contains("TRIGGER", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(sql.Down, s => s.Contains("_History", StringComparison.Ordinal));
        Assert.DoesNotContain(sql.Down, s => s.Contains("TRIGGER", StringComparison.OrdinalIgnoreCase));
    }
}
