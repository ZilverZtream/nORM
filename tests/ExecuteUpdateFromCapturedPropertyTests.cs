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
/// ExecuteUpdate's SetProperty value may be a captured PROPERTY, not only a captured field or a
/// literal — e.g. <c>SetProperty(p =&gt; p.Price, request.Price)</c> where the source is a record/DTO
/// property (the natural shape for a web request handler). It must fold to a bound parameter.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ExecuteUpdateFromCapturedPropertyTests
{
    [Table("EucpRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public decimal Price { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed record PriceRequest(decimal Price, string Name);

    private static async Task<DbContext> NewContextAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EucpRow (Id INTEGER PRIMARY KEY, Price TEXT NOT NULL, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(x => x.Id) });
        ctx.Add(new Row { Id = 1, Price = 10m, Name = "before" });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    [Fact]
    public async Task ExecuteUpdate_binds_a_captured_record_property()
    {
        await using var ctx = await NewContextAsync();
        var request = new PriceRequest(42.50m, "after");

        var updated = await ctx.Query<Row>()
            .Where(r => r.Id == 1)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Price, request.Price)
                .SetProperty(r => r.Name, request.Name));

        Assert.Equal(1, updated);
        // AsNoTracking so we re-read the row the set-based UPDATE wrote, not the stale tracked instance.
        var row = await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().FirstAsync(r => r.Id == 1);
        Assert.Equal(42.50m, row.Price);
        Assert.Equal("after", row.Name);
    }
}
