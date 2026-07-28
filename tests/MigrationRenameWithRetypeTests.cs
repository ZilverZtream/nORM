using System;
using System.Linq;
using nORM.Mapping;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A [RenameColumn] that ALSO changes the column definition (type / nullability / default / precision / ...)
/// must apply BOTH the rename and the definition change. The differ recorded only the rename and skipped all
/// alter detection, silently dropping the retype: the model said the column should become TEXT/nullable while
/// the database kept the old INTEGER/NOT NULL definition — silent schema drift.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MigrationRenameWithRetypeTests
{
    private static TableSchema MakeTable(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in cols) t.Columns.Add(c);
        return t;
    }

    private static ColumnSchema PkCol(string name) =>
        new() { Name = name, ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = $"PK_{name}" };

    [Fact]
    public void Rename_that_also_changes_type_and_nullability_emits_rename_and_alter()
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("T", PkCol("Id"),
            new ColumnSchema { Name = "Age", ClrType = typeof(int).FullName!, IsNullable = false }));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("T", PkCol("Id"),
            new ColumnSchema { Name = "AgeText", PreviousName = "Age", ClrType = typeof(string).FullName!, IsNullable = true }));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Single(diff.RenamedColumns);   // the rename is still recorded
        Assert.Single(diff.AlteredColumns);    // and the retype/nullability change is applied (was dropped)

        var (_, alteredNew, alteredOld) = diff.AlteredColumns[0];
        Assert.Equal("AgeText", alteredNew.Name);
        Assert.Equal(typeof(string).FullName, alteredNew.ClrType);
        Assert.True(alteredNew.IsNullable);
        Assert.Equal("Age", alteredOld.Name);
    }

    [Fact]
    public void Pure_rename_with_no_definition_change_emits_only_the_rename()
    {
        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(MakeTable("T", PkCol("Id"),
            new ColumnSchema { Name = "Cost", ClrType = typeof(decimal).FullName!, IsNullable = true }));

        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(MakeTable("T", PkCol("Id"),
            new ColumnSchema { Name = "Amount", PreviousName = "Cost", ClrType = typeof(decimal).FullName!, IsNullable = true }));

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);

        Assert.Single(diff.RenamedColumns);
        Assert.Empty(diff.AlteredColumns);   // no spurious alter for an unchanged definition
    }
}
