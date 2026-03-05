using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for PRV-1: Bulk insert must handle entities where all columns are DB-generated.
/// </summary>
public class BulkInsertAllGeneratedTests
{
    /// <summary>
    /// Entity where every column is DB-generated (only a single auto-increment PK).
    /// Inserting such an entity requires DEFAULT VALUES syntax.
    /// </summary>
    [Table("AutoOnlyItem")]
    private class AutoOnlyItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
    }

    /// <summary>
    /// Entity with payload columns — standard bulk insert path.
    /// </summary>
    [Table("PayloadItem")]
    private class PayloadItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Quantity { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateAutoOnlyContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AutoOnlyItem (Id INTEGER PRIMARY KEY AUTOINCREMENT);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreatePayloadContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE PayloadItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Quantity INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    /// <summary>
    /// PRV-1: Bulk insert of 3 all-generated-column entities must succeed and insert
    /// 3 rows, each with an auto-generated PK.
    /// </summary>
    [Fact]
    public async Task BulkInsert_AllGeneratedColumns_InsertsRowsWithDefaultValues()
    {
        var (cn, ctx) = CreateAutoOnlyContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var entities = Enumerable.Range(0, 3).Select(_ => new AutoOnlyItem()).ToList();
        var inserted = await ctx.BulkInsertAsync(entities);

        // 3 rows should have been inserted.
        Assert.Equal(3, inserted);

        // Verify 3 rows exist in the database.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM AutoOnlyItem";
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(3L, count);

        // Verify all PKs are positive (auto-generated).
        cmd.CommandText = "SELECT Id FROM AutoOnlyItem ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var ids = new System.Collections.Generic.List<long>();
        while (reader.Read())
            ids.Add(reader.GetInt64(0));

        Assert.Equal(3, ids.Count);
        Assert.All(ids, id => Assert.True(id > 0, $"Expected positive auto-generated PK but got {id}"));
        // All PKs must be distinct.
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    /// <summary>
    /// PRV-1 regression: Entity with payload columns (the normal path) still works correctly.
    /// </summary>
    [Fact]
    public async Task BulkInsert_MixedColumns_NormalPath()
    {
        var (cn, ctx) = CreatePayloadContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var items = Enumerable.Range(1, 5)
            .Select(i => new PayloadItem { Name = $"Item{i}", Quantity = i * 10 })
            .ToList();

        var inserted = await ctx.BulkInsertAsync(items);
        Assert.Equal(5, inserted);

        // Verify all rows are present with correct values.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PayloadItem";
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(5L, count);

        cmd.CommandText = "SELECT SUM(Quantity) FROM PayloadItem";
        var sumQty = (long)cmd.ExecuteScalar()!;
        Assert.Equal(150L, sumQty); // 10+20+30+40+50 = 150
    }
}
