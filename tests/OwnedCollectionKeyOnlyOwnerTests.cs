using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Editing an owned collection on an owner that has a key but NO mutable scalar columns must persist (EF Core
/// only touches the child table). nORM's columnless-UPDATE suppression only recognised many-to-many owners, so
/// a key-only OwnsMany owner threw NormConfigurationException ("no mutable columns to update") on any
/// owned-collection add/remove/clear — a legitimate operation blocked.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedCollectionKeyOnlyOwnerTests
{
    [Table("OkoOwner")]
    public class Owner
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public List<Child> Items { get; set; } = new();
    }

    public class Child
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string V { get; set; } = "";
    }

    private static SqliteConnection Keeper()
    {
        var cn = new SqliteConnection($"Data Source=file:oko_{Guid.NewGuid():N}?mode=memory&cache=shared");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE OkoOwner (Id INTEGER PRIMARY KEY AUTOINCREMENT);" +
                          "CREATE TABLE OkoChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, OkoOwnerId INTEGER NOT NULL, V TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Make(SqliteConnection keeper)
    {
        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Owner>().OwnsMany<Child>(o => o.Items, tableName: "OkoChild", foreignKey: "OkoOwnerId")
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static long Count(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Adding_to_owned_collection_on_key_only_owner_persists()
    {
        using var keeper = Keeper();
        int oid;
        await using (var ctx = Make(keeper))
        {
            var o = new Owner { Items = new List<Child> { new Child { V = "a" } } };
            ctx.Add(o);
            await ctx.SaveChangesAsync();
            oid = o.Id;
        }

        await using (var ctx = Make(keeper))
        {
            var o = ((INormQueryable<Owner>)ctx.Query<Owner>()).Include(x => x.Items).ToList().Single(x => x.Id == oid);
            o.Items.Add(new Child { V = "b" });
            await ctx.SaveChangesAsync();   // must not throw "no mutable columns"
        }

        Assert.Equal(2, Count(keeper, $"SELECT COUNT(*) FROM OkoChild WHERE OkoOwnerId = {oid}"));
    }

    [Fact]
    public async Task Removing_from_owned_collection_on_key_only_owner_persists()
    {
        using var keeper = Keeper();
        int oid;
        await using (var ctx = Make(keeper))
        {
            var o = new Owner { Items = new List<Child> { new Child { V = "a" }, new Child { V = "b" } } };
            ctx.Add(o);
            await ctx.SaveChangesAsync();
            oid = o.Id;
        }

        await using (var ctx = Make(keeper))
        {
            var o = ((INormQueryable<Owner>)ctx.Query<Owner>()).Include(x => x.Items).ToList().Single(x => x.Id == oid);
            o.Items.RemoveAt(0);
            await ctx.SaveChangesAsync();
        }

        Assert.Equal(1, Count(keeper, $"SELECT COUNT(*) FROM OkoChild WHERE OkoOwnerId = {oid}"));
    }
}
