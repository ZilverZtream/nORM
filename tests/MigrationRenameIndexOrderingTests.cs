using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A migration that renames an indexed column rebuilds the index against the
/// NEW column name on Up and the OLD name on Down. The rename statement must
/// therefore precede all index DDL in each direction — with the old order the
/// whole Up failed with "no such column" on every provider.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MigrationRenameIndexOrderingTests
{
    private static SchemaDiff RenamedIndexedColumnDiff()
    {
        var baseline = new TableSchema { Name = "Widget" };
        baseline.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        var c0 = new ColumnSchema { Name = "OldCol", ClrType = typeof(double).FullName!, IsNullable = true };
        c0.Indexes.Add(new ColumnIndexSchema { Name = "IX_Widget_Col", IsUnique = false, Order = 0 });
        baseline.Columns.Add(c0);

        var target = new TableSchema { Name = "Widget" };
        target.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        var c0r = new ColumnSchema { Name = "NewCol", PreviousName = "OldCol", ClrType = typeof(double).FullName!, IsNullable = true };
        c0r.Indexes.Add(new ColumnIndexSchema { Name = "IX_Widget_Col", IsUnique = false, Order = 0 });
        target.Columns.Add(c0r);

        return SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { baseline } },
            new SchemaSnapshot { Tables = { target } });
    }

    private static int FirstIndex(IReadOnlyList<string> statements, Func<string, bool> predicate)
    {
        for (var i = 0; i < statements.Count; i++)
        {
            if (predicate(statements[i]))
                return i;
        }
        return -1;
    }

    public static IEnumerable<object[]> Generators()
    {
        yield return new object[] { "sqlite", (Func<SchemaDiff, MigrationSqlStatements>)(d => new SqliteMigrationSqlGenerator().GenerateSql(d)) };
        yield return new object[] { "postgres", (Func<SchemaDiff, MigrationSqlStatements>)(d => new PostgresMigrationSqlGenerator().GenerateSql(d)) };
        yield return new object[] { "mysql", (Func<SchemaDiff, MigrationSqlStatements>)(d => new MySqlMigrationSqlGenerator().GenerateSql(d)) };
        yield return new object[] { "sqlserver", (Func<SchemaDiff, MigrationSqlStatements>)(d => new SqlServerMigrationSqlGenerator().GenerateSql(d)) };
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void Rename_precedes_index_ddl_in_both_directions(string provider, Func<SchemaDiff, MigrationSqlStatements> generate)
    {
        var sql = generate(RenamedIndexedColumnDiff());

        static bool IsRename(string s) =>
            s.Contains("RENAME COLUMN", StringComparison.OrdinalIgnoreCase)
            || s.Contains("sp_rename", StringComparison.OrdinalIgnoreCase);
        static bool IsCreateIndex(string s) => s.Contains("CREATE INDEX", StringComparison.OrdinalIgnoreCase)
            || s.Contains("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase);

        var upRename = FirstIndex(sql.Up, IsRename);
        var upCreate = FirstIndex(sql.Up, IsCreateIndex);
        if (upRename >= 0 && upCreate >= 0)
        {
            Assert.True(upRename < upCreate,
                $"{provider}: Up rename at {upRename} must precede CREATE INDEX at {upCreate}\n{string.Join("\n", sql.Up)}");
        }

        var downRename = FirstIndex(sql.Down, IsRename);
        var downCreate = FirstIndex(sql.Down, IsCreateIndex);
        if (downRename >= 0 && downCreate >= 0)
        {
            Assert.True(downRename < downCreate,
                $"{provider}: Down rename-back at {downRename} must precede CREATE INDEX at {downCreate}\n{string.Join("\n", sql.Down)}");
        }

        // The shape must actually exercise the hazard on at least one side.
        Assert.True(upRename >= 0 || downRename >= 0, $"{provider}: no rename statement emitted");
    }
}
