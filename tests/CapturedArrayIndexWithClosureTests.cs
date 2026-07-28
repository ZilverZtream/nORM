using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A captured array-index in a predicate (x.Age == arr[i]) is folded to its element and bound as a fixed
/// parameter, but the parameter-value extractor still collects the captured ARRAY as a compiled-param value.
/// Without reserving an _unused compiled-param slot for that array (as the Contains / TimeSpan folds do), the
/// positional execution binder shifts the array onto the FIRST real compiled slot when the predicate contains
/// any other captured scalar — so a following `&amp;&amp; x.Name == nameVar` binds the array to Name and returns the
/// wrong rows (empty for a byte[], which ADO accepts as a BLOB) rather than the correct match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CapturedArrayIndexWithClosureTests
{
    [Table("CaiItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CaiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Age INTEGER NOT NULL);" +
                "INSERT INTO CaiItem VALUES (1, 'Ann', 10), (2, 'Bob', 40), (5, 'Eve', 40);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Captured_byte_array_index_plus_captured_scalar_returns_correct_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var key = new byte[] { 40, 99 };   // key[0] = 40
        var name = "Eve";

        // Age == 40 matches Bob & Eve; && Name == "Eve" narrows to Eve (id 5).
        var ids = ctx.Query<Item>()
            .Where(x => x.Age == key[0] && x.Name == name)
            .ToList()
            .Select(x => x.Id)
            .OrderBy(i => i)
            .ToArray();

        Assert.Equal(new[] { 5 }, ids);
    }

    [Fact]
    public void Captured_int_array_index_plus_captured_scalar_returns_correct_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var ages = new[] { 10, 40, 50 };   // ages[1] = 40
        var name = "Eve";

        var ids = ctx.Query<Item>()
            .Where(x => x.Age == ages[1] && x.Name == name)
            .ToList()
            .Select(x => x.Id)
            .ToArray();

        Assert.Equal(new[] { 5 }, ids);
    }

    [Fact]
    public void Captured_array_index_alone_still_works()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var ages = new[] { 10, 40, 50 };
        var ids = ctx.Query<Item>()
            .Where(x => x.Age == ages[1])
            .ToList()
            .Select(x => x.Id)
            .OrderBy(i => i)
            .ToArray();

        Assert.Equal(new[] { 2, 5 }, ids);
    }
}
