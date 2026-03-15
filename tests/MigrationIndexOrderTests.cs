using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that schema diff detects composite index column order changes.
/// Before the fix, OrderBy() was applied before SequenceEqual, making (A,B)
/// and (B,A) look identical. Now the declared order is preserved.
/// </summary>
public class MigrationIndexOrderTests
{
    // Shared index name used across tests
    private const string IndexName = "IX_T_Composite";

    // ─── Helper: build a snapshot with a named multi-column index ─────────

    private static SchemaSnapshot BuildSnapshotWithIndex(string tableName, string indexName, params string[] columnNamesInOrder)
    {
        var snap = new SchemaSnapshot();
        var table = new TableSchema { Name = tableName };

        // Add a primary-key column first
        table.Columns.Add(new ColumnSchema
        {
            Name = "Id",
            ClrType = "System.Int32",
            IsPrimaryKey = true,
            IsUnique = true,
            IndexName = "PK_" + tableName
        });

        // Add each index column, sharing the same IndexName to make a composite index.
        // Order they appear in Columns list determines ColumnNames[] in BuildIndexMap.
        foreach (var col in columnNamesInOrder)
        {
            table.Columns.Add(new ColumnSchema
            {
                Name = col,
                ClrType = "System.String",
                IsNullable = true,
                IndexName = indexName
            });
        }

        snap.Tables.Add(table);
        return snap;
    }

    // ─── column order change → drop + add ─────────────────────────

    [Fact]
    public void IndexColumnOrderChange_ProducesDropAndAdd()
    {
        // IX_T(ColA, ColB) → IX_T(ColB, ColA) must be detected as a definition change.
        var before = BuildSnapshotWithIndex("T", IndexName, "ColA", "ColB");
        var after  = BuildSnapshotWithIndex("T", IndexName, "ColB", "ColA");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Single(diff.DroppedIndexes);
        Assert.Single(diff.AddedIndexes);
        Assert.Equal(IndexName, diff.DroppedIndexes[0].IndexName);
        Assert.Equal(IndexName, diff.AddedIndexes[0].IndexName);
    }

    [Fact]
    public void IndexColumnOrderChange_DroppedIndexHasCorrectTable()
    {
        var before = BuildSnapshotWithIndex("Products", IndexName, "CategoryId", "SupplierId");
        var after  = BuildSnapshotWithIndex("Products", IndexName, "SupplierId", "CategoryId");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Single(diff.DroppedIndexes);
        Assert.Equal("Products", diff.DroppedIndexes[0].Table.Name);
    }

    [Fact]
    public void IndexColumnOrderChange_AddedIndexHasNewOrder()
    {
        var before = BuildSnapshotWithIndex("Orders", IndexName, "CustomerId", "ProductId");
        var after  = BuildSnapshotWithIndex("Orders", IndexName, "ProductId", "CustomerId");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Single(diff.AddedIndexes);
        var added = diff.AddedIndexes[0];
        Assert.Equal(new[] { "ProductId", "CustomerId" }, added.ColumnNames);
    }

    // ─── Non-regression: same order → no diff ─────────────────────────────

    [Fact]
    public void SameColumnOrder_ProducesNoDiff()
    {
        // Non-regression: identical index definitions produce no diff entry.
        var before = BuildSnapshotWithIndex("T", IndexName, "ColA", "ColB");
        var after  = BuildSnapshotWithIndex("T", IndexName, "ColA", "ColB");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Empty(diff.DroppedIndexes);
        Assert.Empty(diff.AddedIndexes);
    }

    [Fact]
    public void SameColumnOrder_CaseInsensitive_ProducesNoDiff()
    {
        // Non-regression: case differences in column names are still treated as equal.
        var before = BuildSnapshotWithIndex("T", IndexName, "ColA", "ColB");
        var after  = BuildSnapshotWithIndex("T", IndexName, "cola", "colb");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Empty(diff.DroppedIndexes);
        Assert.Empty(diff.AddedIndexes);
    }

    // ─── Non-regression: added/dropped index still detected ───────────────

    [Fact]
    public void NewIndex_IsDetectedAsAdded()
    {
        // Non-regression: a brand-new index appears in AddedIndexes.
        var before = BuildSnapshotWithIndex("T", "IX_Old", "ColA");
        var after  = BuildSnapshotWithIndex("T", "IX_New", "ColA");
        // After has IX_New; before had IX_Old. Both should appear in their respective lists.

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Contains(diff.AddedIndexes, x => x.IndexName == "IX_New");
        Assert.Contains(diff.DroppedIndexes, x => x.IndexName == "IX_Old");
    }

    [Fact]
    public void ThreeColumnIndex_OrderChange_IsDetected()
    {
        // three-column composite index — (A,B,C) → (A,C,B) must be detected.
        var before = BuildSnapshotWithIndex("T", IndexName, "A", "B", "C");
        var after  = BuildSnapshotWithIndex("T", IndexName, "A", "C", "B");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Single(diff.DroppedIndexes);
        Assert.Single(diff.AddedIndexes);
    }

    [Fact]
    public void ThreeColumnIndex_SameOrder_ProducesNoDiff()
    {
        // Non-regression: three-column composite index with same order produces no diff.
        var before = BuildSnapshotWithIndex("T", IndexName, "X", "Y", "Z");
        var after  = BuildSnapshotWithIndex("T", IndexName, "X", "Y", "Z");

        var diff = SchemaDiffer.Diff(before, after);

        Assert.Empty(diff.DroppedIndexes);
        Assert.Empty(diff.AddedIndexes);
    }
}
