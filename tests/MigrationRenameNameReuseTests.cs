using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Guards SQLite schema migration against silent data loss / corruption for column interactions the existing
/// data-preservation fuzzer excludes: rename+retype of the SAME column, and a NEW column whose name RECYCLES a
/// renamed-away old column's name (the reused-name column must be a genuine add, NULL/its default — not a copy
/// of the renamed-away data — and must actually be created). Each test seeds distinguishable rows, applies the
/// generated migration SQL against a real :memory: SQLite connection, then reads RAW rows and asserts every
/// value landed in the right column.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MigrationRenameNameReuseTests
{
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

    private static List<object?[]> ReadAll(SqliteConnection cn, string sql)
    {
        var rows = new List<object?[]>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var vals = new object?[r.FieldCount];
            r.GetValues(vals);
            rows.Add(vals);
        }
        return rows;
    }

    private static ColumnSchema Col(string name, Type clr, bool nullable = true, bool pk = false, bool identity = false, string? previousName = null)
        => new ColumnSchema
        {
            Name = name,
            ClrType = clr.FullName!,
            IsNullable = nullable,
            IsPrimaryKey = pk,
            IsIdentity = identity,
            PreviousName = previousName,
        };

    // ─────────────────────────────────────────────────────────────────────────────
    // FINDING CANDIDATE 1: a NEW column whose name equals a RENAMED-AWAY old column.
    // Model: rename A -> B (B keeps A's data), AND add a brand-new column also named A.
    // The differ (SchemaSnapshot.Differ.cs) matches the new "A" against the still-present
    // old "A" (consumed by the rename) instead of treating it as ADDED, so the recreate's
    // INSERT..SELECT reads old "A" into BOTH B and the new A. New A should be NULL.
    // ─────────────────────────────────────────────────────────────────────────────
    [Fact]
    public void Rename_then_add_column_reusing_old_name_corrupts_new_column_when_retyped()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var gen = new SqliteMigrationSqlGenerator();

        var oldTable = new TableSchema { Name = "T" };
        oldTable.Columns.Add(Col("Id", typeof(int), nullable: false, pk: true, identity: true));
        oldTable.Columns.Add(Col("A", typeof(int)));
        Apply(cn, null, gen.GenerateSql(new SchemaDiff { AddedTables = { oldTable } }).Up, null);

        Exec(cn, "INSERT INTO T (A) VALUES (100);");
        Exec(cn, "INSERT INTO T (A) VALUES (200);");

        // New model: rename A->B (int), add a brand-new column named A but as TEXT.
        var newTable = new TableSchema { Name = "T" };
        newTable.Columns.Add(Col("Id", typeof(int), nullable: false, pk: true, identity: true));
        newTable.Columns.Add(Col("B", typeof(int), previousName: "A"));
        newTable.Columns.Add(Col("A", typeof(string)));   // brand-new column, reuses the freed name

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });
        var sql = gen.GenerateSql(diff);

        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        var rows = ReadAll(cn, "SELECT Id, B, A FROM T ORDER BY Id;");
        Assert.Equal(2, rows.Count);
        // B must hold the renamed old-A data.
        Assert.Equal(100L, rows[0][1]);
        Assert.Equal(200L, rows[1][1]);
        // The BRAND-NEW column A must be NULL. If it instead holds a copy of old A, that
        // is silent corruption of the new column.
        Assert.True(rows[0][2] is DBNull, $"new column A row0 expected NULL, got {rows[0][2]} ({rows[0][2]?.GetType().Name})");
        Assert.True(rows[1][2] is DBNull, $"new column A row1 expected NULL, got {rows[1][2]} ({rows[1][2]?.GetType().Name})");
    }

    // Same recycle scenario but the new column has the SAME type as the old one — no
    // recreate is triggered, so the new column is never created at all (silent schema drift).
    [Fact]
    public void Rename_then_add_column_reusing_old_name_same_type_creates_the_new_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var gen = new SqliteMigrationSqlGenerator();

        var oldTable = new TableSchema { Name = "T" };
        oldTable.Columns.Add(Col("Id", typeof(int), nullable: false, pk: true, identity: true));
        oldTable.Columns.Add(Col("A", typeof(int)));
        Apply(cn, null, gen.GenerateSql(new SchemaDiff { AddedTables = { oldTable } }).Up, null);

        Exec(cn, "INSERT INTO T (A) VALUES (100);");

        var newTable = new TableSchema { Name = "T" };
        newTable.Columns.Add(Col("Id", typeof(int), nullable: false, pk: true, identity: true));
        newTable.Columns.Add(Col("B", typeof(int), previousName: "A"));
        newTable.Columns.Add(Col("A", typeof(int)));   // brand-new column, same type, reuses name

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });
        var sql = gen.GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        // The model declares columns Id, B, A. After Up the table must contain all three.
        var cols = ReadAll(cn, "SELECT name FROM pragma_table_info('T') ORDER BY name;")
            .Select(r => (string)r[0]!).ToList();
        Assert.Contains("A", cols);
        Assert.Contains("B", cols);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Untested surface: rename A->B AND change its type in the same migration.
    // Expect: B holds old A's values (coerced), round-trips on Down.
    // ─────────────────────────────────────────────────────────────────────────────
    [Fact]
    public void Rename_and_retype_same_column_preserves_data_up_and_down()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var gen = new SqliteMigrationSqlGenerator();

        var oldTable = new TableSchema { Name = "T" };
        oldTable.Columns.Add(Col("Id", typeof(int), nullable: false, pk: true, identity: true));
        oldTable.Columns.Add(Col("A", typeof(int)));
        oldTable.Columns.Add(Col("Keep", typeof(string)));
        Apply(cn, null, gen.GenerateSql(new SchemaDiff { AddedTables = { oldTable } }).Up, null);

        Exec(cn, "INSERT INTO T (A, Keep) VALUES (100, 'x');");
        Exec(cn, "INSERT INTO T (A, Keep) VALUES (200, 'y');");

        var newTable = new TableSchema { Name = "T" };
        newTable.Columns.Add(Col("Id", typeof(int), nullable: false, pk: true, identity: true));
        newTable.Columns.Add(Col("B", typeof(long), previousName: "A"));   // rename + retype int->long
        newTable.Columns.Add(Col("Keep", typeof(string)));

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });
        var sql = gen.GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        var up = ReadAll(cn, "SELECT Id, B, Keep FROM T ORDER BY Id;");
        Assert.Equal(100L, up[0][1]);
        Assert.Equal(200L, up[1][1]);
        Assert.Equal("x", up[0][2]);
        Assert.Equal("y", up[1][2]);

        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        var down = ReadAll(cn, "SELECT Id, A, Keep FROM T ORDER BY Id;");
        Assert.Equal(100L, down[0][1]);
        Assert.Equal(200L, down[1][1]);
        Assert.Equal("x", down[0][2]);
        Assert.Equal("y", down[1][2]);
    }
}
