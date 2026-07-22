using System;
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
/// A store-generated convention key on an entity that also has an optimistic-concurrency <c>[Timestamp]</c>
/// token still enforces OCC: the insert generates the key, a stale update (whose token a concurrent writer
/// has already advanced) throws rather than silently clobbering the concurrent change, and a fresh read +
/// update succeeds. Exercises a store-generated key and a concurrency token on the same entity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StoreGeneratedKeyConventionConcurrencyTests
{
    [Table("SgkcRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }          // convention key (store-generated)
        public string Name { get; set; } = "";
        [Timestamp] public byte[] Version { get; set; } = Array.Empty<byte>();
    }

    private static SqliteConnection Db()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SgkcRow (Id INTEGER NOT NULL, Name TEXT NULL, Version BLOB NULL, CONSTRAINT PK_SgkcRow PRIMARY KEY (Id))";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Row>().HasKey(x => x.Id)
    }, ownsConnection: false);

    [Fact]
    public async Task ConventionKeyWithOccToken_GeneratesKey_CatchesStaleUpdate_AllowsFresh()
    {
        await using var cn = Db();
        await using var ctx = Ctx(cn);

        var a = new Row { Name = "a" };
        ctx.Add(a);
        await ctx.SaveChangesAsync();
        Assert.True(a.Id > 0);   // key store-generated

        // A concurrent context advances the row's token.
        await using (var ctx2 = Ctx(cn))
        {
            var loaded = await ctx2.Query<Row>().SingleAsync(r => r.Id == a.Id);
            loaded.Name = "concurrent";
            await ctx2.SaveChangesAsync();
        }

        // The original context now holds a stale token; its update must be rejected, not silently lost.
        a.Name = "stale-writer";
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());

        await using (var ctx3 = Ctx(cn))
            Assert.Equal("concurrent", (await ctx3.Query<Row>().SingleAsync(r => r.Id == a.Id)).Name);

        // A fresh read + update succeeds.
        await using (var ctx4 = Ctx(cn))
        {
            var fresh = await ctx4.Query<Row>().SingleAsync(r => r.Id == a.Id);
            fresh.Name = "final";
            await ctx4.SaveChangesAsync();
            Assert.Equal("final", (await ctx4.Query<Row>().SingleAsync(r => r.Id == a.Id)).Name);
        }
    }
}
