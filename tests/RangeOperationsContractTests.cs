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
/// Pins the EF-parity range write APIs: AddRange / UpdateRange / RemoveRange (and the params
/// overloads) each fan out to the single-entity path and persist through one SaveChanges.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class RangeOperationsContractTests
{
    [Table("RoRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static async Task<DbContext> BootstrapAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE RoRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id)
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        await Task.CompletedTask;
        return ctx;
    }

    [Fact]
    public async Task AddRange_then_UpdateRange_then_RemoveRange_persist_in_one_save_each()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // AddRange via IEnumerable.
        ctx.AddRange(new List<Row>
        {
            new() { Id = 1, Name = "a" },
            new() { Id = 2, Name = "b" },
            new() { Id = 3, Name = "c" },
        });
        await ctx.SaveChangesAsync();
        Assert.Equal(3, await ctx.Query<Row>().CountAsync());

        // UpdateRange via array: rename the tracked entities.
        var all = await ctx.Query<Row>().ToListAsync();
        foreach (var r in all) r.Name = r.Name.ToUpperInvariant();
        ctx.UpdateRange(all.ToArray());   // array literal binds T to the element type
        await ctx.SaveChangesAsync();
        var renamed = (await ctx.Query<Row>().AsNoTracking().ToListAsync())
            .OrderBy(r => r.Id).Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "A", "B", "C" }, renamed);

        // RemoveRange via IEnumerable: delete two of them.
        ctx.RemoveRange(all.Where(r => r.Id != 2));
        await ctx.SaveChangesAsync();
        var remaining = (await ctx.Query<Row>().AsNoTracking().ToListAsync()).Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2 }, remaining);
    }

    [Fact]
    public async Task AddRange_array_literal_binds_to_element_type()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // An array literal binds T to Row (element type), not to the collection type — the
        // gotcha the params overload would have introduced.
        ctx.AddRange(new[] { new Row { Id = 10, Name = "x" }, new Row { Id = 11, Name = "y" } });
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { 10, 11 },
            (await ctx.Query<Row>().AsNoTracking().ToListAsync()).Select(r => r.Id).OrderBy(i => i).ToArray());
    }
}
