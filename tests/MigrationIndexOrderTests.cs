using System;
using System.Linq;
using nORM.Configuration;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that schema diff detects composite index column order changes.
/// Before the fix, OrderBy() was applied before SequenceEqual, making (A,B)
/// and (B,A) look identical. Now the declared order is preserved.
/// </summary>
[Xunit.Trait("Category", "Fast")]
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

    [Fact]
    public void AllGenerators_AddedIndex_EmitsCreateIndexAndDownDropIndex()
    {
        var table = BuildTableWithNamedIndex("T", "IX_T_Name");
        var diff = new SchemaDiff();
        diff.AddedIndexes.Add((table, "IX_T_Name", false, new[] { "Name" }, new[] { false }));

        foreach (var (generator, createPrefix, dropSql) in IndexSqlExpectations())
        {
            var sql = generator.GenerateSql(diff);

            Assert.Contains(sql.Up, s => s.StartsWith(createPrefix, StringComparison.Ordinal));
            Assert.Contains(dropSql, sql.Down);
        }
    }

    [Fact]
    public void AllGenerators_DroppedIndex_EmitsDropIndexAndDownCreateIndex()
    {
        var table = BuildTableWithNamedIndex("T", "IX_T_Name");
        var diff = new SchemaDiff();
        diff.DroppedIndexes.Add((table, "IX_T_Name"));

        foreach (var (generator, createPrefix, dropSql) in IndexSqlExpectations())
        {
            var sql = generator.GenerateSql(diff);

            Assert.Contains(dropSql, sql.Up);
            Assert.Contains(sql.Down, s => s.StartsWith(createPrefix, StringComparison.Ordinal));
        }
    }

    [Fact]
    public void SqlServerAndPostgres_AddedIndex_ResolvesIncludedColumnsAndFilterFromTable()
    {
        var sqlServerTable = BuildTableWithFilteredIncludedIndex("Orders", "IX_Orders_Code", "[Code] IS NOT NULL");
        var sqlServerDiff = new SchemaDiff();
        sqlServerDiff.AddedIndexes.Add((sqlServerTable, "IX_Orders_Code", false, new[] { "Code" }, new[] { false }));

        var sqlServerUp = string.Join(" ", new SqlServerMigrationSqlGenerator().GenerateSql(sqlServerDiff).Up);
        Assert.Contains("CREATE INDEX [IX_Orders_Code] ON [Orders] ([Code]) INCLUDE ([Description]) WHERE [Code] IS NOT NULL", sqlServerUp);

        var postgresTable = BuildTableWithFilteredIncludedIndex("Orders", "IX_Orders_Code", "\"Code\" IS NOT NULL");
        var postgresDiff = new SchemaDiff();
        postgresDiff.AddedIndexes.Add((postgresTable, "IX_Orders_Code", false, new[] { "Code" }, new[] { false }));

        var postgresUp = string.Join(" ", new PostgresMigrationSqlGenerator().GenerateSql(postgresDiff).Up);
        Assert.Contains("CREATE INDEX \"IX_Orders_Code\" ON \"Orders\" (\"Code\") INCLUDE (\"Description\") WHERE \"Code\" IS NOT NULL", postgresUp);
    }

    [Fact]
    public void SchemaDiffer_DetectsNullsNotDistinctIndexChanges()
    {
        var oldSnapshot = new SchemaSnapshot();
        oldSnapshot.Tables.Add(BuildTableWithNullsNotDistinctIndex("Orders", "IX_Orders_Code", nullsNotDistinct: false));
        var newSnapshot = new SchemaSnapshot();
        newSnapshot.Tables.Add(BuildTableWithNullsNotDistinctIndex("Orders", "IX_Orders_Code", nullsNotDistinct: true));

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.Contains(diff.DroppedIndexes, index => index.IndexName == "IX_Orders_Code");
        Assert.Contains(diff.AddedIndexes, index => index.IndexName == "IX_Orders_Code");
        var postgresUp = string.Join(" ", new PostgresMigrationSqlGenerator().GenerateSql(diff).Up);
        Assert.Contains("CREATE UNIQUE INDEX \"IX_Orders_Code\" ON \"Orders\" (\"Code\") NULLS NOT DISTINCT", postgresUp);
    }

    [Fact]
    public void SchemaDiffer_DetectsNullSortOrderIndexChanges()
    {
        var oldSnapshot = new SchemaSnapshot();
        oldSnapshot.Tables.Add(BuildTableWithNullSortOrderIndex("Orders", "IX_Orders_Code", IndexNullSortOrder.Default));
        var newSnapshot = new SchemaSnapshot();
        newSnapshot.Tables.Add(BuildTableWithNullSortOrderIndex("Orders", "IX_Orders_Code", IndexNullSortOrder.First));

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.Contains(diff.DroppedIndexes, index => index.IndexName == "IX_Orders_Code");
        Assert.Contains(diff.AddedIndexes, index => index.IndexName == "IX_Orders_Code");
        var postgresUp = string.Join(" ", new PostgresMigrationSqlGenerator().GenerateSql(diff).Up);
        Assert.Contains("CREATE INDEX \"IX_Orders_Code\" ON \"Orders\" (\"Code\" NULLS FIRST)", postgresUp);
    }

    [Fact]
    public void Providers_AddedAlternateKeyIndex_IsCreatedBeforeForeignKey()
    {
        foreach (var generator in RelationalIndexFkOrderGenerators())
        {
            var diff = BuildAlternateKeyIndexAndForeignKeyDiff(add: true);
            var up = generator.GenerateSql(diff).Up.ToArray();

            var createIndex = Array.FindIndex(up, s => s.Contains("IX_Users_Code", StringComparison.Ordinal));
            var addForeignKey = Array.FindIndex(up, s => s.Contains("FK_Orders_Users_Code", StringComparison.Ordinal));

            Assert.True(createIndex >= 0, $"{generator.GetType().Name} did not create the alternate-key index.");
            Assert.True(addForeignKey >= 0, $"{generator.GetType().Name} did not add the foreign key.");
            Assert.True(createIndex < addForeignKey, $"{generator.GetType().Name} added the FK before creating the referenced index.");
        }
    }

    [Fact]
    public void Providers_DroppedAlternateKeyIndex_IsRestoredBeforeForeignKeyInDown()
    {
        foreach (var generator in RelationalIndexFkOrderGenerators())
        {
            var diff = BuildAlternateKeyIndexAndForeignKeyDiff(add: false);
            var down = generator.GenerateSql(diff).Down.ToArray();

            var createIndex = Array.FindIndex(down, s => s.Contains("IX_Users_Code", StringComparison.Ordinal));
            var addForeignKey = Array.FindIndex(down, s => s.Contains("FK_Orders_Users_Code", StringComparison.Ordinal));

            Assert.True(createIndex >= 0, $"{generator.GetType().Name} did not restore the alternate-key index.");
            Assert.True(addForeignKey >= 0, $"{generator.GetType().Name} did not restore the foreign key.");
            Assert.True(createIndex < addForeignKey, $"{generator.GetType().Name} restored the FK before recreating the referenced index.");
        }
    }

    private static TableSchema BuildTableWithNamedIndex(string tableName, string indexName)
    {
        var table = new TableSchema { Name = tableName };
        table.Columns.Add(new ColumnSchema { Name = "Id", ClrType = "System.Int32", IsPrimaryKey = true });
        table.Columns.Add(new ColumnSchema { Name = "Name", ClrType = "System.String", IsNullable = false, IndexName = indexName });
        return table;
    }

    private static TableSchema BuildTableWithFilteredIncludedIndex(string tableName, string indexName, string filterSql)
    {
        var table = new TableSchema { Name = tableName };
        table.Columns.Add(new ColumnSchema { Name = "Id", ClrType = "System.Int32", IsPrimaryKey = true });

        var code = new ColumnSchema { Name = "Code", ClrType = "System.String", IsNullable = false };
        code.Indexes.Add(new ColumnIndexSchema { Name = indexName, FilterSql = filterSql });
        table.Columns.Add(code);

        var description = new ColumnSchema { Name = "Description", ClrType = "System.String", IsNullable = true };
        description.Indexes.Add(new ColumnIndexSchema { Name = indexName, IsIncluded = true });
        table.Columns.Add(description);

        return table;
    }

    private static TableSchema BuildTableWithNullsNotDistinctIndex(string tableName, string indexName, bool nullsNotDistinct)
    {
        var table = new TableSchema { Name = tableName };
        table.Columns.Add(new ColumnSchema { Name = "Id", ClrType = "System.Int32", IsPrimaryKey = true });
        var code = new ColumnSchema { Name = "Code", ClrType = "System.String", IsNullable = true };
        code.Indexes.Add(new ColumnIndexSchema { Name = indexName, IsUnique = true, NullsNotDistinct = nullsNotDistinct });
        table.Columns.Add(code);
        return table;
    }

    private static TableSchema BuildTableWithNullSortOrderIndex(string tableName, string indexName, IndexNullSortOrder nullSortOrder)
    {
        var table = new TableSchema { Name = tableName };
        table.Columns.Add(new ColumnSchema { Name = "Id", ClrType = "System.Int32", IsPrimaryKey = true });
        var code = new ColumnSchema { Name = "Code", ClrType = "System.String", IsNullable = true };
        code.Indexes.Add(new ColumnIndexSchema { Name = indexName, NullSortOrder = nullSortOrder });
        table.Columns.Add(code);
        return table;
    }

    private static SchemaDiff BuildAlternateKeyIndexAndForeignKeyDiff(bool add)
    {
        var users = new TableSchema { Name = "Users" };
        users.Columns.Add(new ColumnSchema { Name = "Id", ClrType = "System.Int32", IsPrimaryKey = true });
        users.Columns.Add(new ColumnSchema { Name = "Code", ClrType = "System.String", IsNullable = false, IsUnique = true, IndexName = "IX_Users_Code" });

        var orders = new TableSchema { Name = "Orders" };
        orders.Columns.Add(new ColumnSchema { Name = "Id", ClrType = "System.Int32", IsPrimaryKey = true });
        orders.Columns.Add(new ColumnSchema { Name = "UserCode", ClrType = "System.String", IsNullable = false });
        var foreignKey = new ForeignKeySchema
        {
            ConstraintName = "FK_Orders_Users_Code",
            DependentColumns = new[] { "UserCode" },
            PrincipalTable = "Users",
            PrincipalColumns = new[] { "Code" }
        };

        var diff = new SchemaDiff();
        if (add)
        {
            diff.AddedIndexes.Add((users, "IX_Users_Code", true, new[] { "Code" }, new[] { false }));
            diff.AddedForeignKeys.Add((orders, foreignKey));
        }
        else
        {
            diff.DroppedIndexes.Add((users, "IX_Users_Code"));
            diff.DroppedForeignKeys.Add((orders, foreignKey));
        }

        return diff;
    }

    private static IMigrationSqlGenerator[] RelationalIndexFkOrderGenerators() =>
    [
        new SqlServerMigrationSqlGenerator(),
        new MySqlMigrationSqlGenerator(),
        new PostgresMigrationSqlGenerator()
    ];

    private static (IMigrationSqlGenerator Generator, string CreatePrefix, string DropSql)[] IndexSqlExpectations() =>
    [
        (new SqlServerMigrationSqlGenerator(), "CREATE INDEX [IX_T_Name] ON [T] ([Name])", "DROP INDEX [IX_T_Name] ON [T]"),
        (new MySqlMigrationSqlGenerator(), "CREATE INDEX `IX_T_Name` ON `T` (`Name`)", "DROP INDEX `IX_T_Name` ON `T`"),
        (new PostgresMigrationSqlGenerator(), "CREATE INDEX \"IX_T_Name\" ON \"T\" (\"Name\")", "DROP INDEX \"IX_T_Name\""),
        (new SqliteMigrationSqlGenerator(), "CREATE INDEX \"IX_T_Name\" ON \"T\" (\"Name\")", "DROP INDEX IF EXISTS \"IX_T_Name\"")
    ];
}
