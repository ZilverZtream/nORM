using System;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The destructive-change classifier must flag a Unicode -> ANSI change, a store-type (HasColumnType)
/// change, and a fixed-length change. Regression: these facets were detected as altered columns but not
/// warned, so a lossy alter (e.g. NVARCHAR -> VARCHAR, which silently substitutes '?' on SQL Server, or
/// nvarchar(200) -> varchar(50), which truncates) shipped without the --force gate.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MigrationDestructiveFacetWarningTests
{
    private static SchemaSnapshot Snapshot(ColumnSchema nameColumn) => new()
    {
        Tables =
        {
            new TableSchema
            {
                Name = "T",
                Columns =
                {
                    new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                    nameColumn
                }
            }
        }
    };

    private static ColumnSchema StringCol(bool? isUnicode = null, bool isFixedLength = false, string? storeType = null) =>
        new()
        {
            Name = "Name",
            ClrType = typeof(string).FullName!,
            IsNullable = false,
            MaxLength = 100,
            IsUnicode = isUnicode,
            IsFixedLength = isFixedLength,
            StoreType = storeType
        };

    [Fact]
    public void Unicode_to_ansi_column_change_is_flagged_destructive()
    {
        var diff = SchemaDiffer.Diff(Snapshot(StringCol(isUnicode: true)), Snapshot(StringCol(isUnicode: false)));
        Assert.True(diff.HasDestructiveChanges);
        Assert.Contains(diff.GetDestructiveChangeWarnings(), w => w.Contains("Unicode to non-Unicode", StringComparison.Ordinal));
    }

    [Fact]
    public void Explicit_store_type_change_is_flagged_destructive()
    {
        var diff = SchemaDiffer.Diff(
            Snapshot(StringCol(storeType: "nvarchar(200)")),
            Snapshot(StringCol(storeType: "varchar(50)")));
        Assert.True(diff.HasDestructiveChanges);
        Assert.Contains(diff.GetDestructiveChangeWarnings(), w => w.Contains("explicit store type", StringComparison.Ordinal));
    }

    [Fact]
    public void Fixed_length_change_is_flagged_destructive()
    {
        var diff = SchemaDiffer.Diff(Snapshot(StringCol(isFixedLength: false)), Snapshot(StringCol(isFixedLength: true)));
        Assert.True(diff.HasDestructiveChanges);
        Assert.Contains(diff.GetDestructiveChangeWarnings(), w => w.Contains("fixed-length", StringComparison.Ordinal));
    }

    [Fact]
    public void Identical_column_facets_are_not_flagged()
    {
        var diff = SchemaDiffer.Diff(
            Snapshot(StringCol(isUnicode: true, storeType: "nvarchar(200)")),
            Snapshot(StringCol(isUnicode: true, storeType: "nvarchar(200)")));
        Assert.False(diff.HasDestructiveChanges);
    }
}
