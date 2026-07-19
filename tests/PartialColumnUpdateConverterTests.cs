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
/// A value-converted column that changes must be bound through its converter in a partial-column UPDATE's SET
/// clause — the same as a full update — so the provider stores the converted value and a round-trip returns
/// the model value. A concurrent write to an unchanged column still survives (partial).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PartialColumnUpdateConverterTests
{
    [Table("PcuCvWidget")]
    private class Widget
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }   // stored offset by +1000 via the converter
        public int Other { get; set; }
    }

    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Widget>().HasKey(w => w.Id);
            mb.Entity<Widget>().Property<int>(w => w.Score).HasConversion(new OffsetConverter());
        }
    }, ownsConnection: false);

    [Fact]
    public async Task Changed_converter_column_binds_through_the_converter_in_a_partial_update()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PcuCvWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Other INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        await using (var seed = Ctx(cn))
        {
            seed.Add(new Widget { Id = 1, Score = 5, Other = 20 });   // Score stored as 1005
            await seed.SaveChangesAsync();
        }

        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        w.Score = 50;   // only Score changed → partial UPDATE SET Score

        // Concurrent write to the unchanged Other column must survive the partial update.
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "UPDATE PcuCvWidget SET Other = 99 WHERE Id = 1"; cmd.ExecuteNonQuery(); }

        await ctx.SaveChangesAsync();

        // Raw stored Score must be the converted value (1050), proving the converter ran in the partial SET.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT Score, Other FROM PcuCvWidget WHERE Id = 1";
            using var r = cmd.ExecuteReader(); r.Read();
            Assert.Equal(1050, r.GetInt32(0));   // 50 + 1000 — converter applied
            Assert.Equal(99, r.GetInt32(1));     // concurrent change survived (Other not in SET)
        }

        // And a fresh read round-trips back to the model value.
        await using var verify = Ctx(cn);
        var reloaded = (await verify.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        Assert.Equal(50, reloaded.Score);
        Assert.Equal(99, reloaded.Other);
    }
}
