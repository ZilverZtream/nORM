using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Gate D: Tests that topological sort is also applied to modified (UPDATE) entities,
/// not just inserts and deletes. FK constraints fire on UPDATE too — if a dependent row
/// is updated before its principal, FK validation fails on databases with FK enforcement.
/// </summary>
public class FkUpdateOrderingTests
{
    // ─── Schema: Category (principal) → Product (dependent via CategoryId FK) ──

    [Table("FkUpdateCategory")]
    private class FkUpdateCategory
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("FkUpdateProduct")]
    private class FkUpdateProduct
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        // Auto-detected FK: "FkUpdateCategoryId" strips "Id" → ForeignKeyPrincipalTypeName = "FkUpdateCategory"
        public int FkUpdateCategoryId { get; set; }
    }

    private static async Task<SqliteConnection> CreateSchemaAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Enable FK constraints
        await using (var pragma = cn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            await pragma.ExecuteNonQueryAsync();
        }

        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE FkUpdateCategory (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE FkUpdateProduct (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, " +
                "FkUpdateCategoryId INTEGER NOT NULL REFERENCES FkUpdateCategory(Id));";
            await cmd.ExecuteNonQueryAsync();
        }

        return cn;
    }

    private static async Task InsertAsync(SqliteConnection cn, int categoryId, string categoryName,
        int productId, string productName)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO FkUpdateCategory VALUES ({categoryId}, '{categoryName}');" +
            $"INSERT INTO FkUpdateProduct VALUES ({productId}, '{productName}', {categoryId});";
        await cmd.ExecuteNonQueryAsync();
    }

    // ─── Gate D: UPDATE ordering follows principal-first ──────────────────

    [Fact]
    public async Task GateD_ModifyBothPrincipalAndDependent_SaveSucceeds()
    {
        // Gate D: When both principal (Category) and dependent (Product) are modified in the
        // same SaveChanges call, the UPDATE for Category must be issued before the UPDATE
        // for Product. Without topological sorting of modified entities, this can trigger
        // FK constraint violations on databases that enforce FK integrity during UPDATEs.
        await using var cn = await CreateSchemaAsync();
        await InsertAsync(cn, 10, "Electronics", 1, "Widget");

        await using var ctx = new DbContext(cn, new SqliteProvider());

        var cat = new FkUpdateCategory { Id = 10, Name = "Electronics Updated" };
        var prod = new FkUpdateProduct { Id = 1, Name = "Widget Updated", FkUpdateCategoryId = 10 };

        // Add both in "wrong" order — dependent first, then principal
        ctx.Update(prod);
        ctx.Update(cat);

        // Must not throw FK violation — topological sort must reorder UPDATE to principal-first
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync(detectChanges: false));
        Assert.Null(ex);
    }

    [Fact]
    public async Task GateD_ModifyOnlyDependent_SaveSucceeds()
    {
        // Gate D: Modifying only the dependent (no principal change) should still succeed.
        await using var cn = await CreateSchemaAsync();
        await InsertAsync(cn, 20, "Tools", 2, "Hammer");

        await using var ctx = new DbContext(cn, new SqliteProvider());

        var prod = new FkUpdateProduct { Id = 2, Name = "Hammer Pro", FkUpdateCategoryId = 20 };
        ctx.Update(prod);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync(detectChanges: false));
        Assert.Null(ex);
    }

    [Fact]
    public async Task GateD_ModifyOnlyPrincipal_SaveSucceeds()
    {
        // Gate D: Modifying only the principal should always succeed regardless of sort order.
        await using var cn = await CreateSchemaAsync();
        await InsertAsync(cn, 30, "Sports", 3, "Ball");

        await using var ctx = new DbContext(cn, new SqliteProvider());

        var cat = new FkUpdateCategory { Id = 30, Name = "Sports Equipment" };
        ctx.Update(cat);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync(detectChanges: false));
        Assert.Null(ex);
    }

    [Fact]
    public async Task GateD_ThreeLevelChain_ModifyAll_SaveSucceeds()
    {
        // Gate D: A 3-level chain (A → B → C where → denotes FK dependency) must be
        // ordered correctly: principal first → A, then B, then C.
        // For updates, principal-first ordering is: A before B before C.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        await using (var pragma = cn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            await pragma.ExecuteNonQueryAsync();
        }

        await using (var setup = cn.CreateCommand())
        {
            // 3-level: Brand → Category → Product chain
            setup.CommandText =
                "CREATE TABLE FkBrand (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE FkCategory (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, FkBrandId INTEGER NOT NULL REFERENCES FkBrand(Id));" +
                "CREATE TABLE FkProduct3 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, FkCategoryId INTEGER NOT NULL REFERENCES FkCategory(Id));";
            await setup.ExecuteNonQueryAsync();
        }

        await using (var insert = cn.CreateCommand())
        {
            insert.CommandText =
                "INSERT INTO FkBrand VALUES (1, 'Acme');" +
                "INSERT INTO FkCategory VALUES (1, 'Tools', 1);" +
                "INSERT INTO FkProduct3 VALUES (1, 'Hammer', 1);";
            await insert.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Mark all three as modified — in reverse principal order
        ctx.Update(new FkProduct3 { Id = 1, Name = "Hammer Updated", FkCategoryId = 1 });
        ctx.Update(new FkCategory { Id = 1, Name = "Tools Updated", FkBrandId = 1 });
        ctx.Update(new FkBrand { Id = 1, Name = "Acme Updated" });

        // Topological sort must produce: FkBrand → FkCategory → FkProduct3
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync(detectChanges: false));
        Assert.Null(ex);
    }

    [Fact]
    public async Task GateD_MixedInsertModifyDelete_AllSortedCorrectly()
    {
        // Gate D: When a transaction contains adds, modifies, and deletes, all three segments
        // must be sorted. This test verifies the full SaveChangesInternalAsync path.
        await using var cn = await CreateSchemaAsync();

        // Start with two categories and two products
        await using (var insert = cn.CreateCommand())
        {
            insert.CommandText =
                "INSERT INTO FkUpdateCategory VALUES (40, 'Cat40');" +
                "INSERT INTO FkUpdateCategory VALUES (41, 'Cat41');" +
                "INSERT INTO FkUpdateProduct VALUES (40, 'Prod40', 40);" +
                "INSERT INTO FkUpdateProduct VALUES (41, 'Prod41', 41);";
            await insert.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Add a new category-product pair (insert path)
        ctx.Add(new FkUpdateCategory { Id = 42, Name = "Cat42" });
        ctx.Add(new FkUpdateProduct { Id = 42, Name = "Prod42", FkUpdateCategoryId = 42 });

        // Modify an existing pair (update path — Gate D fix)
        ctx.Update(new FkUpdateProduct { Id = 40, Name = "Prod40 Updated", FkUpdateCategoryId = 40 });
        ctx.Update(new FkUpdateCategory { Id = 40, Name = "Cat40 Updated" });

        // Delete an existing pair (delete path — reversed sort)
        ctx.Remove(new FkUpdateProduct { Id = 41, Name = "Prod41", FkUpdateCategoryId = 41 });
        ctx.Remove(new FkUpdateCategory { Id = 41, Name = "Cat41" });

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync(detectChanges: false));
        Assert.Null(ex);
    }

    [Fact]
    public async Task GateD_CyclicModifiedEntities_ThrowsNormConfigurationException()
    {
        // Gate D: Cyclic FK dependencies among modified entities should throw
        // NormConfigurationException with a clear message about the cycle.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // No FK enforcement — just verify the topological sort detects the cycle.
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE CycleA (Id INTEGER PRIMARY KEY, CycleBId INTEGER NOT NULL);" +
                "CREATE TABLE CycleB (Id INTEGER PRIMARY KEY, CycleAId INTEGER NOT NULL);";
            await setup.ExecuteNonQueryAsync();
        }

        await using (var insert = cn.CreateCommand())
        {
            insert.CommandText =
                "INSERT INTO CycleA VALUES (1, 1);" +
                "INSERT INTO CycleB VALUES (1, 1);";
            await insert.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Both entities form a circular FK dependency; topological sort must detect this.
        ctx.Update(new CycleA { Id = 1, CycleBId = 1 });
        ctx.Update(new CycleB { Id = 1, CycleAId = 1 });

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(
            () => ctx.SaveChangesAsync(detectChanges: false));
        Assert.Contains("Circular FK dependency", ex.Message);
    }

    // ─── Helper entity classes for 3-level test ────────────────────────────

    [Table("FkBrand")]
    private class FkBrand
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("FkCategory")]
    private class FkCategory
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int FkBrandId { get; set; }
    }

    [Table("FkProduct3")]
    private class FkProduct3
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int FkCategoryId { get; set; }
    }

    // ─── Helper entity classes for cyclic test ─────────────────────────────

    [Table("CycleA")]
    private class CycleA
    {
        [Key] public int Id { get; set; }
        public int CycleBId { get; set; }
    }

    [Table("CycleB")]
    private class CycleB
    {
        [Key] public int Id { get; set; }
        public int CycleAId { get; set; }
    }
}
