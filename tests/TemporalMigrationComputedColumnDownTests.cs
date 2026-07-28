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
/// Dropping and rolling back a NOT NULL computed column on a temporal table. The history stores a computed
/// column as a plain NOT NULL value column, so the Down restored it via ALTER TABLE ADD COLUMN with an empty
/// DEFAULT (invalid — "incomplete input"); routing it through the history recreate fixed that but exposed a
/// second issue: the surviving versioning triggers reference the history table, so SQLite's schema-aware
/// rename during the recreate failed to re-validate them ("no such table"). Dropping the triggers before the
/// recreate resolves it. Verified by APPLYING both directions to a real SQLite database.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalMigrationComputedColumnDownTests
{
    [Table("TmigCmp")]
    private sealed class CmpRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        [DatabaseGenerated(DatabaseGeneratedOption.Computed)] public int Doubled { get; set; }
    }

    private static TableSchema BuildTable(bool withComputed)
    {
        var t = new TableSchema { Name = "TmigCmp", IsTemporal = true };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema { Name = "V", ClrType = typeof(int).FullName!, IsNullable = false });
        if (withComputed)
            t.Columns.Add(new ColumnSchema { Name = "Doubled", ClrType = typeof(int).FullName!, IsNullable = false, ComputedColumnSql = "V * 2", IsStoredComputedColumn = false });
        return t;
    }

    private static void Apply(SqliteConnection cn, IEnumerable<string>? statements)
    {
        foreach (var s in statements ?? Enumerable.Empty<string>())
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = s;
            cmd.ExecuteNonQuery();
        }
    }

    private static long Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Drop_and_restore_of_a_computed_column_applies_in_both_directions()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmigCmp (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, " +
                              "Doubled INTEGER GENERATED ALWAYS AS (V * 2) VIRTUAL NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<CmpRow>() };
        opts.EnableTemporalVersioning();
        using var seedCtx = new DbContext(cn, new SqliteProvider(), opts);
        seedCtx.Add(new CmpRow { Id = 1, V = 5 });
        await seedCtx.SaveChangesAsync();
        Assert.True(Scalar(cn, "SELECT COUNT(*) FROM TmigCmp_History;") >= 1);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withComputed: true) } },
            new SchemaSnapshot { Tables = { BuildTable(withComputed: false) } }));

        // Up: drop the computed column from the main + history tables.
        Apply(cn, sql.PreTransactionUp); Apply(cn, sql.Up); Apply(cn, sql.PostTransactionUp);
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM pragma_table_info('TmigCmp_History') WHERE name = 'Doubled';"));

        // Down: restore it — previously threw on an empty-DEFAULT ADD COLUMN, then on the trigger re-validation.
        Apply(cn, sql.PreTransactionDown); Apply(cn, sql.Down); Apply(cn, sql.PostTransactionDown);
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM pragma_table_info('TmigCmp_History') WHERE name = 'Doubled';"));
        Assert.True(Scalar(cn, "SELECT COUNT(*) FROM TmigCmp_History;") >= 1);

        // Versioning is still alive after the Down: a new write records a version (the triggers were re-created).
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO TmigCmp (V) VALUES (9)";
            cmd.ExecuteNonQuery();
        }
        Assert.True(Scalar(cn, "SELECT COUNT(*) FROM TmigCmp_History WHERE V = 9;") >= 1);
    }
}
