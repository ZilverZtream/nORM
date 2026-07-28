using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// DistinctBy picks one row per key. A global filter (soft-delete) must apply BEFORE the per-key dedup, or a
/// key whose top-ordered row is filtered out disappears entirely even though a visible sibling exists — a
/// silent row loss. Verifies the filtered, deduped result still contains that key (sourced from its
/// non-deleted sibling), matching the LINQ-to-Objects oracle (filter, then DistinctBy).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DistinctByGlobalFilterTests
{
    [Table("DbgfItem_Test")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string GroupKey { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE DbgfItem_Test (Id INTEGER PRIMARY KEY, GroupKey TEXT NOT NULL, IsDeleted INTEGER NOT NULL);" +
                // Group A's only VISIBLE row is Id=3 (the two lower-Id rows are soft-deleted). Group B: Id=4.
                "INSERT INTO DbgfItem_Test VALUES (1, 'A', 1), (2, 'A', 1), (3, 'A', 0), (4, 'B', 0);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Item>(i => !i.IsDeleted);
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void DistinctBy_applies_global_filter_before_dedup()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var keys = ctx.Query<Item>()
            .OrderBy(i => i.Id)
            .DistinctBy(i => i.GroupKey)
            .ToList()
            .Select(i => i.GroupKey)
            .OrderBy(k => k)
            .ToArray();

        // Both groups must survive: A from its only visible row (Id=3), B from Id=4.
        Assert.Equal(new[] { "A", "B" }, keys);
    }
}
