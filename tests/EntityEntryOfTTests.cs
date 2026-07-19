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
/// The strongly-typed <see cref="DbContext.Entry{TEntity}(TEntity)"/> → <see cref="EntityEntry{TEntity}"/>
/// (EF Core parity): typed entity access and lambda-based property selection
/// (<c>Property(e =&gt; e.X)</c>), delegating to the untyped entry and converting implicitly to it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntityEntryOfTTests
{
    [Table("EeoWidget")]
    private class Widget
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
    }, ownsConnection: false);

    private static async Task<(SqliteConnection, DbContext, Widget)> SeedAndTrack()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EeoWidget (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        await using (var seed = Ctx(cn))
        {
            seed.Add(new Widget { Id = 1, A = 10, Name = "orig" });
            await seed.SaveChangesAsync();
        }
        var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        return (cn, ctx, w);
    }

    [Fact]
    public async Task Typed_entity_and_lambda_property_access()
    {
        var (cn, ctx, w) = await SeedAndTrack(); using var _cn = cn; await using var _ctx = ctx;
        w.A = 11;

        EntityEntry<Widget> e = ctx.Entry(w);   // strongly-typed overload
        Assert.Equal(1, e.Entity.Id);            // typed entity, no cast
        Assert.Equal(11, e.Property(x => x.A).CurrentValue);
        Assert.Equal(10, e.Property(x => x.A).OriginalValue);
        Assert.True(e.Property(x => x.A).IsModified);
        Assert.False(e.Property(x => x.Name).IsModified);
    }

    [Fact]
    public async Task Property_lambda_is_modified_can_be_set()
    {
        var (cn, ctx, w) = await SeedAndTrack(); using var _cn = cn; await using var _ctx = ctx;
        w.A = 11; w.Name = "changed";

        ctx.Entry(w).Property(x => x.A).IsModified = false;   // exclude A from the update via lambda
        await ctx.SaveChangesAsync();

        using var read = cn.CreateCommand();
        read.CommandText = "SELECT A, Name FROM EeoWidget WHERE Id = 1";
        using var r = read.ExecuteReader(); r.Read();
        Assert.Equal(10, r.GetInt32(0));         // A excluded → original persisted
        Assert.Equal("changed", r.GetString(1)); // Name still written
    }

    [Fact]
    public async Task Lambda_and_string_property_resolve_the_same_column()
    {
        var (cn, ctx, w) = await SeedAndTrack(); using var _cn = cn; await using var _ctx = ctx;
        var e = ctx.Entry(w);
        Assert.Equal(e.Property("A").Name, e.Property(x => x.A).Name);
    }

    [Fact]
    public async Task Non_property_lambda_throws()
    {
        var (cn, ctx, w) = await SeedAndTrack(); using var _cn = cn; await using var _ctx = ctx;
        Assert.Throws<ArgumentException>(() => ctx.Entry(w).Property(x => x.A + 1));
    }

    [Fact]
    public async Task Converts_implicitly_to_the_untyped_entry()
    {
        var (cn, ctx, w) = await SeedAndTrack(); using var _cn = cn; await using var _ctx = ctx;
        EntityEntry untyped = ctx.Entry(w);   // implicit conversion
        untyped.State = EntityState.Modified;
        Assert.Equal(EntityState.Modified, ctx.Entry(w).State);
        Assert.Same(untyped, ctx.Entry(w).Entry);
    }
}
