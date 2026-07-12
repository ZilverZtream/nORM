using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// When one migration changes the same SQLite table in several recreate-requiring ways (e.g. an
/// altered column AND an added CHECK constraint), each change category emitted its own full table
/// recreation. On the Down path those recreations clobbered one another — each rebuilt the table
/// from the full post-diff schema except for the single dimension it was reverting — so the last
/// recreate won and the other reverts were silently lost. The generated Down must restore the
/// complete pre-migration schema.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SqliteMigrationMultiRecreateRevertTests
{
    private static ColumnSchema Col(string name, Type clr, bool pk = false, bool nullable = false, bool identity = false)
        => new()
        {
            Name = name,
            ClrType = clr.FullName!,
            IsPrimaryKey = pk,
            IsIdentity = identity,
            IsNullable = nullable
        };

    private static TableSchema Product(bool nameNullable, bool withPriceCheck)
    {
        var t = new TableSchema
        {
            Name = "Product",
            Columns =
            {
                Col("Id", typeof(int), pk: true, identity: true),
                Col("Price", typeof(decimal)),
                Col("Name", typeof(string), nullable: nameNullable)
            }
        };
        if (withPriceCheck)
            t.CheckConstraints.Add(new CheckConstraintSchema { ConstraintName = "CK_Product_Price", Sql = "Price > 0" });
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

    private static (int NotNull, int RowCount, bool HasCheck) InspectName(SqliteConnection cn)
    {
        int notnull;
        using (var info = cn.CreateCommand())
        {
            info.CommandText = "SELECT \"notnull\" FROM pragma_table_info('Product') WHERE name = 'Name'";
            notnull = Convert.ToInt32(info.ExecuteScalar());
        }
        int rows;
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM Product";
            rows = Convert.ToInt32(c.ExecuteScalar());
        }
        bool hasCheck;
        using (var s = cn.CreateCommand())
        {
            s.CommandText = "SELECT sql FROM sqlite_master WHERE type='table' AND name='Product'";
            hasCheck = ((string)s.ExecuteScalar()!).Contains("CHECK", StringComparison.OrdinalIgnoreCase);
        }
        return (notnull, rows, hasCheck);
    }

    [Fact]
    public void Down_restores_both_altered_column_and_dropped_check_when_one_table_recreated_twice()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Baseline schema: Name NOT NULL, no CHECK.
        foreach (var s in new SqliteMigrationSqlGenerator()
                     .GenerateSql(new SchemaDiff { AddedTables = { Product(nameNullable: false, withPriceCheck: false) } }).Up)
            Exec(cn, s);
        Exec(cn, "INSERT INTO Product (Price, Name) VALUES (5, 'keep')");

        // Migration: make Name NULLABLE (altered column) AND add a CHECK constraint — both force a
        // full table recreate, and both touch the same table.
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { Product(nameNullable: false, withPriceCheck: false) } },
            new SchemaSnapshot { Tables = { Product(nameNullable: true, withPriceCheck: true) } });
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);
        var afterUp = InspectName(cn);
        Assert.Equal(0, afterUp.NotNull);   // Name is now nullable
        Assert.True(afterUp.HasCheck);      // CHECK present
        Assert.Equal(1, afterUp.RowCount);  // data preserved

        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        var afterDown = InspectName(cn);

        Assert.Equal(1, afterDown.RowCount);   // data preserved through the rollback
        Assert.False(afterDown.HasCheck);      // CHECK reverted
        Assert.Equal(1, afterDown.NotNull);    // BUG: 0 — the column alteration was clobbered and not reverted
    }

    private static ColumnSchema RenamedCol(string newName, string previousName, Type clr, bool nullable)
        => new() { Name = newName, PreviousName = previousName, ClrType = clr.FullName!, IsNullable = nullable };

    [Fact]
    public void Rename_folded_into_recreate_preserves_data_both_directions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Baseline: OldName TEXT, Flag NOT NULL.
        var baseline = new TableSchema
        {
            Name = "Gadget",
            Columns =
            {
                Col("Id", typeof(int), pk: true, identity: true),
                Col("OldName", typeof(string)),
                Col("Flag", typeof(int))
            }
        };
        foreach (var s in new SqliteMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { baseline } }).Up)
            Exec(cn, s);
        Exec(cn, "INSERT INTO Gadget (OldName, Flag) VALUES ('hello', 1)");

        // Migration: rename OldName -> NewName AND alter Flag to nullable. The alter forces a table
        // recreate; the rename must fold into it (read OldName into NewName), not emit a standalone
        // RENAME that the rebuilt table would reject.
        var newTable = new TableSchema
        {
            Name = "Gadget",
            Columns =
            {
                Col("Id", typeof(int), pk: true, identity: true),
                RenamedCol("NewName", "OldName", typeof(string), nullable: false),
                Col("Flag", typeof(int), nullable: true)
            }
        };
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { baseline } },
            new SchemaSnapshot { Tables = { newTable } });
        Assert.Single(diff.RenamedColumns);
        Assert.Single(diff.AlteredColumns);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);

        // The renamed column carries its data across the recreate.
        using (var q = cn.CreateCommand())
        {
            q.CommandText = "SELECT NewName FROM Gadget WHERE Id = 1";
            Assert.Equal("hello", (string)q.ExecuteScalar()!);
        }

        Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);

        // Down restores the original column name, still carrying the data.
        using (var q = cn.CreateCommand())
        {
            q.CommandText = "SELECT OldName FROM Gadget WHERE Id = 1";
            Assert.Equal("hello", (string)q.ExecuteScalar()!);
        }
    }
}
