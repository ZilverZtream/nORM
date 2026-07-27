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
/// Regression: with <see cref="DbContextOptions.EagerChangeTracking"/> disabled, a query-loaded entity is
/// tracked lazily and its original-value snapshot is deferred. The load → edit → SaveChanges flow must still
/// detect and persist the edit — a lazily-tracked entity whose baseline is captured AFTER the edit would find
/// no diff and silently drop the UPDATE (SaveChanges returns 0, no exception), a silent lost update.
/// </summary>
[Trait("Category", "Fast")]
public class EagerChangeTrackingDataLossTests
{
    [Table("EctItem")]
    private sealed class Item
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Load_edit_save_persists_the_edit(bool eager)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE EctItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        var options = new DbContextOptions { EagerChangeTracking = eager };

        using (var ctx = new DbContext(cn, new SqliteProvider(), options, ownsConnection: false))
            await ctx.InsertAsync(new Item { Id = 1, Name = "orig", Value = 10 });

        using (var ctx = new DbContext(cn, new SqliteProvider(), options, ownsConnection: false))
        {
            var e = ctx.Query<Item>().ToList().Single();   // tracked (lazily when eager=false)
            e.Name = "EDITED";
            e.Value = 999;
            var affected = await ctx.SaveChangesAsync();
            Assert.Equal(1, affected);                     // the edit must be detected and written
        }

        using (var ctx = new DbContext(cn, new SqliteProvider(), options, ownsConnection: false))
        {
            var e = ((INormQueryable<Item>)ctx.Query<Item>()).AsNoTracking().Single();
            Assert.Equal("EDITED", e.Name);
            Assert.Equal(999, e.Value);
        }
    }
}
