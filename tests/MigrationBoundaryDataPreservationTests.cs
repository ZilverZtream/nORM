using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for migration data preservation over BOUNDARY values (migration matrix cell:
/// ADD nullable column x type family).
///
/// The migration fuzzer covers random values of common types; this cell pins the boundary shapes it
/// does not reach: a 17th-significant-digit decimal, an offset-suffixed DateTimeOffset, a canonical
/// Guid, an astral-pair (emoji) string, a binary blob, and an in-range ulong stored as signed-64.
/// Adding a nullable column must leave every stored byte of every existing row untouched (the new
/// column reads NULL), and the Down migration - which DROPS the column via SQLite's full table
/// recreate - must rebuild the table with the same rows byte-for-byte.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MigrationBoundaryDataPreservationTests
{
    private static TableSchema BuildTable(bool withExtra)
    {
        var t = new TableSchema { Name = "MigBoundary" };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema { Name = "Dec", ClrType = typeof(decimal).FullName!, IsNullable = false });
        t.Columns.Add(new ColumnSchema { Name = "Dto", ClrType = typeof(DateTimeOffset).FullName!, IsNullable = false });
        t.Columns.Add(new ColumnSchema { Name = "G", ClrType = typeof(Guid).FullName!, IsNullable = false });
        t.Columns.Add(new ColumnSchema { Name = "S", ClrType = typeof(string).FullName!, IsNullable = false });
        t.Columns.Add(new ColumnSchema { Name = "B", ClrType = typeof(byte[]).FullName!, IsNullable = false });
        t.Columns.Add(new ColumnSchema { Name = "U", ClrType = typeof(long).FullName!, IsNullable = false });
        if (withExtra)
            t.Columns.Add(new ColumnSchema { Name = "Extra", ClrType = typeof(int).FullName!, IsNullable = true });
        return t;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void Apply(SqliteConnection cn, IEnumerable<string>? pre, IReadOnlyList<string> body, IEnumerable<string>? post)
    {
        foreach (var s in pre ?? Enumerable.Empty<string>()) Exec(cn, s);
        foreach (var s in body) Exec(cn, s);
        foreach (var s in post ?? Enumerable.Empty<string>()) Exec(cn, s);
    }

    private static List<object?[]> Snapshot(SqliteConnection cn)
    {
        var rows = new List<object?[]>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Dec, Dto, G, S, B, U FROM MigBoundary ORDER BY Id;";
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var vals = new object?[7];
            for (int i = 0; i < 7; i++) vals[i] = r.GetValue(i);
            rows.Add(vals);
        }
        return rows;
    }

    private static void AssertSnapshotsEqual(List<object?[]> a, List<object?[]> b)
    {
        Assert.Equal(a.Count, b.Count);
        for (int i = 0; i < a.Count; i++)
            for (int j = 0; j < a[i].Length; j++)
            {
                if (a[i][j] is byte[] ba)
                    Assert.True(ba.AsSpan().SequenceEqual((byte[])b[i][j]!));
                else
                    Assert.Equal(a[i][j], b[i][j]);
            }
    }

    private static SqliteConnection CreateSeededBaseline(out List<object?[]> before)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var gen = new SqliteMigrationSqlGenerator();
        Apply(cn, null, gen.GenerateSql(new SchemaDiff { AddedTables = { BuildTable(withExtra: false) } }).Up, null);

        using (var ins = cn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO MigBoundary (Dec, Dto, G, S, B, U) VALUES (@d, @o, @g, @s, @b, @u);";
            ins.Parameters.AddWithValue("@d", "1.00000000000000005");
            ins.Parameters.AddWithValue("@o", "2020-06-15 12:00:00+05:00");
            ins.Parameters.AddWithValue("@g", "a1b2c3d4-e5f6-4708-9a0b-c1d2e3f4a5b6");
            ins.Parameters.AddWithValue("@s", "café \U0001F600 it's \"quoted\"");
            ins.Parameters.AddWithValue("@b", new byte[] { 0, 1, 255, 128, 42 });
            ins.Parameters.AddWithValue("@u", 9_000_000_000_000_000_000L);
            ins.ExecuteNonQuery();
        }
        before = Snapshot(cn);
        return cn;
    }

    [Fact]
    public void Add_not_null_column_without_default_fails_loud_at_generation()
    {
        // The generator refuses to invent a backfill: an ADD NOT NULL with no DefaultValue is a
        // usage error caught at SQL-GENERATION time with an actionable message, never a silent
        // implicit backfill (and never a runtime constraint failure halfway through a migration).
        var target = BuildTable(withExtra: false);
        target.Columns.Add(new ColumnSchema { Name = "Extra", ClrType = typeof(int).FullName!, IsNullable = false });
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } },
            new SchemaSnapshot { Tables = { target } });

        var ex = Assert.Throws<InvalidOperationException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));
        Assert.Contains("DefaultValue", ex.Message);
    }

    [Fact]
    public void Add_not_null_column_with_default_backfills_and_preserves_boundary_data()
    {
        using var cn = CreateSeededBaseline(out var before);

        // Up: ADD a NOT NULL int column WITH an explicit default - existing rows get the default.
        var target = BuildTable(withExtra: false);
        target.Columns.Add(new ColumnSchema { Name = "Extra", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "7" });
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } },
            new SchemaSnapshot { Tables = { target } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        AssertSnapshotsEqual(before, Snapshot(cn));   // prior columns byte-exact
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM MigBoundary WHERE Extra = 7;";
            Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));   // explicit-default backfill
        }

        // Down: DROP it - full recreate again, data byte-exact, column gone.
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        AssertSnapshotsEqual(before, Snapshot(cn));
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM pragma_table_info('MigBoundary') WHERE name = 'Extra';";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));
        }
    }

    [Fact]
    public void Drop_column_preserves_remaining_boundary_data_and_down_restores_the_column()
    {
        using var cn = CreateSeededBaseline(out _);

        // Give the baseline an extra droppable column with a value, then snapshot the KEEPER columns.
        Exec(cn, "ALTER TABLE MigBoundary ADD COLUMN Extra INTEGER NULL;");
        Exec(cn, "UPDATE MigBoundary SET Extra = 77;");
        var keepersBefore = Snapshot(cn);   // Snapshot reads only the keeper columns

        // Up: DROP the column (SQLite full-table recreate of the keepers).
        var withExtra = BuildTable(withExtra: true);
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { withExtra } },
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        AssertSnapshotsEqual(keepersBefore, Snapshot(cn));   // keepers byte-exact through the recreate
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM pragma_table_info('MigBoundary') WHERE name = 'Extra';";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));
        }

        // Down: the column comes back (nullable -> NULL per the documented contract: dropped-column
        // data is gone; re-added columns read NULL) and the keepers stay byte-exact.
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        AssertSnapshotsEqual(keepersBefore, Snapshot(cn));
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM MigBoundary WHERE Extra IS NULL;";
            Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));
        }
    }

    [Fact]
    public void Add_nullable_column_preserves_boundary_data_up_and_down()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Create the baseline table through the migration pipeline itself.
        var gen = new SqliteMigrationSqlGenerator();
        var baseline = BuildTable(withExtra: false);
        Apply(cn, null, gen.GenerateSql(new SchemaDiff { AddedTables = { baseline } }).Up, null);

        // Seed boundary values in the exact stored (provider) forms.
        var blob = new byte[] { 0, 1, 255, 128, 42 };
        using (var ins = cn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO MigBoundary (Dec, Dto, G, S, B, U) VALUES (@d, @o, @g, @s, @b, @u);";
            ins.Parameters.AddWithValue("@d", "1.00000000000000005");                       // 17-digit canonical text
            ins.Parameters.AddWithValue("@o", "2020-06-15 12:00:00+05:00");                 // offset-suffixed DTO text
            ins.Parameters.AddWithValue("@g", "a1b2c3d4-e5f6-4708-9a0b-c1d2e3f4a5b6");      // canonical Guid text
            ins.Parameters.AddWithValue("@s", "café \U0001F600 it's \"quoted\"");        // astral + quotes
            ins.Parameters.AddWithValue("@b", blob);
            ins.Parameters.AddWithValue("@u", 9_000_000_000_000_000_000L);                  // ulong in-range signed form
            ins.ExecuteNonQuery();
        }

        var before = Snapshot(cn);
        Assert.Single(before);

        // Up: ADD nullable column.
        var withExtra = BuildTable(withExtra: true);
        var upDiff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } },
            new SchemaSnapshot { Tables = { withExtra } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(upDiff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        // Existing bytes untouched; new column NULL.
        AssertSnapshotsEqual(before, Snapshot(cn));
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM MigBoundary WHERE Extra IS NULL;";
            Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));
        }

        // Down: DROP the column - the SQLite full-table recreate must preserve every byte.
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        AssertSnapshotsEqual(before, Snapshot(cn));
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM pragma_table_info('MigBoundary') WHERE name = 'Extra';";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));   // column gone after Down
        }
    }
}
