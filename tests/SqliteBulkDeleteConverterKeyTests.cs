using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// SQLite's native entity-collection BulkDeleteAsync must bind the PROVIDER representation of the key
/// (and OCC token) columns, exactly as its own insert path already does. Binding the raw model key against
/// a value-converter key column matches no rows, so the delete silently affects zero rows and the entities
/// survive with no error. A negating int converter (model v stored as -v) makes every miss visible.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SqliteBulkDeleteConverterKeyTests
{
    [Table("SqBulkDelConv_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb(string seed)
    {
        var cs = $"Data Source=file:sqbulkdel_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = $"CREATE TABLE SqBulkDelConv_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL); {seed}";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                var e = mb.Entity<Row>();
                e.Property<int>(p => p.Id).HasConversion(new NegatingConverter());
                e.Property<int>(p => p.Val).HasConversion(new NegatingConverter());
            }
        };
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static int Count(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM SqBulkDelConv_Test";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Native_bulk_delete_matches_converter_keyed_rows()
    {
        // Stored via the converter: Id -5/-6, Val -10/-12.
        var (keeper, ctx) = CreateDb("INSERT INTO SqBulkDelConv_Test VALUES (-5, -10), (-6, -12)");
        using var _ = keeper;
        await using var __ = ctx;

        var deleted = await ctx.BulkDeleteAsync(new[]
        {
            new Row { Id = 5, Val = 10 },
            new Row { Id = 6, Val = 12 },
        });

        Assert.Equal(2, deleted);   // BUG: 0 — raw model key 5/6 matched no -5/-6 row
        Assert.Equal(0, Count(keeper));
    }
}
