using System;
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
/// The SQLite bulk insert/update paths bound raw CLR property values straight to parameters,
/// bypassing the value-converter (<see cref="IValueConverter.ConvertToProvider"/>) that every other
/// write path applies. A converter-mapped column was therefore stored as its raw MODEL value; the
/// read path then applied <c>ConvertFromProvider</c>, producing a silently corrupted value.
/// (A Guid key stores as canonical TEXT under Microsoft.Data.Sqlite either way — that invariant is
/// pinned here too.)
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SqliteBulkGuidConverterBindingTests
{
    // Negates the integer: model 42 ↔ db -42. If ConvertToProvider is skipped on write but
    // ConvertFromProvider is applied on read, the value comes back negated (corrupted).
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private class BcRow
    {
        [Key] public int Id { get; set; }
        public int Points { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BcRow (Id INTEGER PRIMARY KEY, Points INTEGER NOT NULL, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BcRow>().Property(r => r.Points).HasConversion(new NegatingConverter())
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static int RawPoints(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Points FROM BcRow WHERE Id = {id}";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task BulkInsert_applies_value_converter_on_write()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        await ctx.BulkInsertAsync(new[] { new BcRow { Id = 1, Points = 100, Name = "a" } });

        // The converter must run on write: db stores -100, and the round-trip yields 100.
        Assert.Equal(-100, RawPoints(cn, 1));                        // BUG: 100 — raw model value stored
        var read = ctx.Query<BcRow>().First(r => r.Id == 1);
        Assert.Equal(100, read.Points);                             // BUG: -100 — ConvertFromProvider applied to un-converted value
    }

    [Fact]
    public async Task BulkUpdate_applies_value_converter_on_write()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        await ctx.InsertAsync(new BcRow { Id = 1, Points = 10, Name = "orig" });
        await ctx.BulkUpdateAsync(new[] { new BcRow { Id = 1, Points = 200, Name = "upd" } });

        Assert.Equal(-200, RawPoints(cn, 1));                        // BUG: 200 — converter skipped in bulk-update staging
        var read = ctx.Query<BcRow>().First(r => r.Id == 1);
        Assert.Equal(200, read.Points);
    }

    [Table("BgGuid")]
    private class BgGuid
    {
        [Key] public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task BulkInsert_stores_guid_key_as_text_and_row_is_findable()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BgGuid (Id TEXT PRIMARY KEY, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var g = Guid.Parse("11111111-1111-1111-1111-111111111111");
        await ctx.BulkInsertAsync(new[] { new BgGuid { Id = g, Name = "a" } });

        using (var q = cn.CreateCommand())
        {
            q.CommandText = "SELECT typeof(Id) FROM BgGuid LIMIT 1";
            Assert.Equal("text", (string)q.ExecuteScalar()!); // canonical TEXT, not BLOB
        }
        Assert.NotNull(ctx.Query<BgGuid>().Where(x => x.Id == g).FirstOrDefault());
    }
}
