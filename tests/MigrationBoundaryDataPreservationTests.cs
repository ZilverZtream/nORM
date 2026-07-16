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
    public void Rename_column_keeps_data_byte_exact_both_directions()
    {
        using var cn = CreateSeededBaseline(out var before);

        // Up: rename S -> Label (PreviousName drives the rename through the recreate).
        var target = BuildTable(withExtra: false);
        var sCol = target.Columns.Single(c => c.Name == "S");
        sCol.Name = "Label";
        sCol.PreviousName = "S";
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } },
            new SchemaSnapshot { Tables = { target } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        // Data byte-exact under the new name (compare per-column: S's values now live in Label).
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT Dec, Dto, G, Label, B, U FROM MigBoundary ORDER BY Id;";
            using var r = c.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(before[0][1], r.GetValue(0));
            Assert.Equal(before[0][2], r.GetValue(1));
            Assert.Equal(before[0][3], r.GetValue(2));
            Assert.Equal(before[0][4], r.GetValue(3));   // S's value, now in Label
            Assert.True(((byte[])before[0][5]!).AsSpan().SequenceEqual((byte[])r.GetValue(4)));
            Assert.Equal(before[0][6], r.GetValue(5));
        }

        // Down: rename back; the original snapshot query must succeed and match byte-for-byte.
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        AssertSnapshotsEqual(before, Snapshot(cn));
    }

    [Fact]
    public void Combined_add_rename_drop_round_trip_restores_schema_and_data()
    {
        using var cn = CreateSeededBaseline(out _);

        // Give the baseline a droppable column so one diff can add + rename + drop at once.
        Exec(cn, "ALTER TABLE MigBoundary ADD COLUMN Extra INTEGER NULL;");
        Exec(cn, "UPDATE MigBoundary SET Extra = 77;");
        var keepersBefore = Snapshot(cn);
        var schemaBefore = SchemaInfo(cn);

        var oldTable = BuildTable(withExtra: true);
        var newTable = BuildTable(withExtra: false);          // drops Extra
        var sCol = newTable.Columns.Single(c => c.Name == "S");
        sCol.Name = "Label";                                   // renames S -> Label
        sCol.PreviousName = "S";
        newTable.Columns.Add(new ColumnSchema { Name = "Added", ClrType = typeof(string).FullName!, IsNullable = true });

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);

        // After the full round trip: schema identical (names/types/notnull/pk) except the dropped
        // column's value (documented contract: dropped data is gone, restored column reads NULL).
        Assert.Equal(schemaBefore, SchemaInfo(cn));
        var after = Snapshot(cn);
        AssertSnapshotsEqual(keepersBefore, after);
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM MigBoundary WHERE Extra IS NULL;";
            Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));
        }
    }

    [Fact]
    public void Unique_index_add_enforces_and_drop_removes_with_data_untouched()
    {
        using var cn = CreateSeededBaseline(out var before);

        // Up: add a UNIQUE index on U.
        var target = BuildTable(withExtra: false);
        target.Columns.Single(c => c.Name == "U").Indexes.Add(
            new ColumnIndexSchema { Name = "IX_MigBoundary_U", IsUnique = true, Order = 0 });
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } },
            new SchemaSnapshot { Tables = { target } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        AssertSnapshotsEqual(before, Snapshot(cn));   // data untouched by index add

        // The unique constraint is ENFORCED: a duplicate U fails loud.
        var dup = cn.CreateCommand();
        dup.CommandText = "INSERT INTO MigBoundary (Dec, Dto, G, S, B, U) VALUES ('1','2020-01-01 00:00:00+00:00','g','s',x'00',9000000000000000000);";
        Assert.Throws<SqliteException>(() => dup.ExecuteNonQuery());

        // Down: index gone, data untouched.
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        AssertSnapshotsEqual(before, Snapshot(cn));
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM pragma_index_list('MigBoundary') WHERE name = 'IX_MigBoundary_U';";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));
        }
    }

    [Fact]
    public void Foreign_key_add_enforces_and_drop_relaxes_with_data_untouched()
    {
        using var cn = CreateSeededBaseline(out var before);
        Exec(cn, "PRAGMA foreign_keys = ON;");

        // Child table without an FK, seeded with a row pointing at the existing parent (Id 1).
        var childNoFk = new TableSchema { Name = "MigBoundaryChild" };
        childNoFk.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        childNoFk.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName! });
        var gen = new SqliteMigrationSqlGenerator();
        Apply(cn, null, gen.GenerateSql(new SchemaDiff { AddedTables = { childNoFk } }).Up, null);
        Exec(cn, "INSERT INTO MigBoundaryChild (ParentId) VALUES (1);");

        // Up: add the FK (child recreate).
        var childWithFk = new TableSchema { Name = "MigBoundaryChild" };
        childWithFk.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        childWithFk.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName! });
        childWithFk.ForeignKeys.Add(new ForeignKeySchema
        {
            ConstraintName = "FK_MigBoundaryChild_MigBoundary_ParentId",
            DependentColumns = new[] { "ParentId" },
            PrincipalTable = "MigBoundary",
            PrincipalColumns = new[] { "Id" },
        });
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false), childNoFk } },
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false), childWithFk } });
        var sql = gen.GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        AssertSnapshotsEqual(before, Snapshot(cn));   // parent data untouched

        // FK ENFORCED: an orphan insert fails loud.
        var orphan = cn.CreateCommand();
        orphan.CommandText = "INSERT INTO MigBoundaryChild (ParentId) VALUES (99999);";
        Assert.Throws<SqliteException>(() => orphan.ExecuteNonQuery());

        // Down: FK gone - the same orphan insert is allowed; data untouched.
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        Exec(cn, "INSERT INTO MigBoundaryChild (ParentId) VALUES (99999);");
        AssertSnapshotsEqual(before, Snapshot(cn));
    }

    [Fact]
    public void Default_value_column_add_backfills_and_applies_to_new_inserts()
    {
        using var cn = CreateSeededBaseline(out var before);

        // Up: add a nullable column WITH a default.
        var target = BuildTable(withExtra: false);
        target.Columns.Add(new ColumnSchema { Name = "Extra", ClrType = typeof(int).FullName!, IsNullable = true, DefaultValue = "42" });
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { BuildTable(withExtra: false) } },
            new SchemaSnapshot { Tables = { target } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        AssertSnapshotsEqual(before, Snapshot(cn));   // prior columns byte-exact

        // Existing row got the default; a new insert without Extra also honours it.
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM MigBoundary WHERE Extra = 42;";
            Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));
        }
        Exec(cn, "INSERT INTO MigBoundary (Dec, Dto, G, S, B, U) VALUES ('2','2021-01-01 00:00:00+00:00','g2','s2',x'01',5);");
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM MigBoundary WHERE Extra = 42;";
            Assert.Equal(2, Convert.ToInt32(c.ExecuteScalar()));
        }

        // Down: column dropped, original data intact.
        Exec(cn, "DELETE FROM MigBoundary WHERE Id > 1;");
        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        AssertSnapshotsEqual(before, Snapshot(cn));
    }

    private static string SchemaInfo(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT name, type, [notnull], pk FROM pragma_table_info('MigBoundary') ORDER BY name;";
        using var r = cmd.ExecuteReader();
        var sb = new System.Text.StringBuilder();
        while (r.Read())
            sb.Append(r.GetString(0)).Append(':').Append(r.GetString(1)).Append(':')
              .Append(r.GetInt64(2)).Append(':').Append(r.GetInt64(3)).Append(';');
        return sb.ToString();
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
