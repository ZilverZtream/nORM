using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// AsAsyncEnumerable (row-by-row streaming) must return the SAME result as the buffered ToListAsync: it was
/// dropping OwnsMany owned collections (owners streamed with empty collections) and ignoring a split
/// projection's client transform (casting the server entity straight to the projection type — a crash, or raw
/// entities when the projection type is a supertype).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsAsyncEnumerableOwnedAndClientProjectionTests
{
    [Owned]
    public sealed class Line
    {
        public string Product { get; set; } = "";
    }

    [Table("AaeOrder_Test")]
    public sealed class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("AaeRow_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public string Csv { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) CreateOwned()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE AaeOrder_Test (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE AaeOrderLine (AaeOrderId INTEGER NOT NULL, Product TEXT NOT NULL);" +
                "INSERT INTO AaeOrder_Test VALUES (1);" +
                "INSERT INTO AaeOrderLine (AaeOrderId, Product) VALUES (1, 'Alpha'), (1, 'Beta');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Order>()
                .OwnsMany<Line>(o => o.Lines, tableName: "AaeOrderLine", foreignKey: "AaeOrderId")
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static (SqliteConnection, DbContext) CreateRows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE AaeRow_Test (Id INTEGER PRIMARY KEY, Csv TEXT NOT NULL);" +
                "INSERT INTO AaeRow_Test VALUES (1, 'a,b,c'), (2, 'x,y');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public async Task Streaming_populates_owned_collections_like_ToListAsync()
    {
        var (cn, ctx) = CreateOwned();
        using var _cn = cn; await using var _ctx = ctx;

        var streamed = new List<int>();
        await foreach (var o in ctx.Query<Order>().AsAsyncEnumerable())
            streamed.Add(o.Lines.Count);

        // Streamed owner must carry its owned rows (2), matching the buffered load.
        Assert.Equal(new[] { 2 }, streamed.ToArray());
    }

    [Fact]
    public async Task Streaming_applies_a_split_client_projection_like_ToListAsync()
    {
        var (cn, ctx) = CreateRows();
        using var _cn = cn; await using var _ctx = ctx;

        var buffered = (await ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Csv.Split(',', StringSplitOptions.None)).ToListAsync())
            .Select(a => string.Join("|", a)).ToList();

        var streamed = new List<string>();
        await foreach (var arr in ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Csv.Split(',', StringSplitOptions.None)).AsAsyncEnumerable())
            streamed.Add(string.Join("|", arr));

        Assert.Equal(buffered, streamed);
        Assert.Equal(new[] { "a|b|c", "x|y" }, streamed.ToArray());
    }
}
