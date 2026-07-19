using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that a GroupBy key over a value-converter column materializes the MODEL value, not the stored
/// provider value (grouping runs on the stored value server-side; the key handed back is converted, as EF
/// does). The converter is resolved from the ACTUAL GroupBy key selector — not a type-match — so a plain
/// column that shares the key's CLR type (e.g. the int primary key) is never wrongly converted, and it is
/// folded into the materializer cache key so GroupBy(A) and GroupBy(B) with an identical projection shape do
/// not share a materializer.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByConverterKeyContractTests
{
    [Table("GbkOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public int Age { get; set; }
    }

    // Model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GbkOrder (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Age INTEGER NOT NULL);
                INSERT INTO GbkOrder VALUES (1,1005,30),(2,1007,30),(3,1009,40);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Order>().Property<int>(o => o.Score).HasConversion(new OffsetConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static int[] ScoreKeys(DbContext ctx)
        => ctx.Query<Order>().GroupBy(o => o.Score).Select(g => new { g.Key, Count = g.Count() })
            .ToList().Select(x => x.Key).OrderBy(x => x).ToArray();

    private static int[] AgeKeys(DbContext ctx)
        => ctx.Query<Order>().GroupBy(o => o.Age).Select(g => new { g.Key, Count = g.Count() })
            .ToList().Select(x => x.Key).OrderBy(x => x).ToArray();

    [Fact]
    public void Group_key_of_a_converter_column_is_the_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 5, 7, 9 }, ScoreKeys(ctx));   // model, not stored 1005/1007/1009
    }

    [Fact]
    public void Group_key_of_a_plain_column_sharing_the_key_type_is_not_converted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Age (int, no converter) shares int with the converter column Score and the int PK; it must NOT be
        // run through Score's converter.
        Assert.Equal(new[] { 30, 40 }, AgeKeys(ctx));
    }

    [Fact]
    public void Converter_and_plain_group_keys_do_not_share_a_materializer()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Same projection shape (g => new { g.Key, Count }); the group-key converter is part of the
        // materializer cache key, so the two queries must not reuse each other's materializer.
        Assert.Equal(new[] { 5, 7, 9 }, ScoreKeys(ctx));   // converted
        Assert.Equal(new[] { 30, 40 }, AgeKeys(ctx));      // not converted
        Assert.Equal(new[] { 5, 7, 9 }, ScoreKeys(ctx));   // still converted after the plain-key query ran
    }
}
