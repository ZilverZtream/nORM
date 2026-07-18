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
/// Pins the EF-parity async add surface: AddAsync / AddRangeAsync track entities in the Added state
/// (keys are assigned at SaveChanges) so EF-source code using them compiles and works.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AddAsyncContractTests
{
    [Table("AaRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE AaRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task AddAsync_tracks_the_entity_and_persists_it()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var entry = await ctx.AddAsync(new Row { Id = 1, Name = "a" });
        Assert.Equal(EntityState.Added, entry.State);

        await ctx.SaveChangesAsync();

        await using var verify = NewCtx(cn);
        Assert.Equal("a", (await verify.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1))!.Name);
    }

    [Fact]
    public async Task AddRangeAsync_tracks_and_persists_all_entities()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        await ctx.AddRangeAsync(new List<Row>
        {
            new() { Id = 1, Name = "a" },
            new() { Id = 2, Name = "b" },
        });

        await ctx.SaveChangesAsync();

        await using var verify = NewCtx(cn);
        Assert.Equal(2, await verify.Query<Row>().CountAsync());
    }
}
