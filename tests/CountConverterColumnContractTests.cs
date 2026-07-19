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
/// Pins that the Count fast path applies a column's value converter. It bound the RAW model value into the
/// COUNT WHERE clause, so <c>Count(o =&gt; o.Score == 42)</c> counted zero rows when the converter stored 42 as
/// -42 — a silent-wrong count, and one whose result flipped depending on whether an unrelated global filter
/// was configured (which routes past the fast path). The equality path now converts; a bool column with a
/// converter defers to the full pipeline. Covers both async and sync Count.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CountConverterColumnContractTests
{
    public enum Status { Open, Pending, Closed }

    [Table("CcOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public Status Status { get; set; }
        public bool Flag { get; set; }
    }

    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private sealed class StatusConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Status>(value);
    }

    private sealed class BoolYnConverter : ValueConverter<bool, string>
    {
        public override object? ConvertToProvider(bool value) => value ? "Y" : "N";
        public override object? ConvertFromProvider(string value) => value == "Y";
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CcOrder (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Status TEXT NOT NULL, Flag TEXT NOT NULL);
                INSERT INTO CcOrder VALUES (1,1005,'Open','Y'),(2,1007,'Pending','N'),(3,1009,'Closed','Y');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Order>().Property<int>(o => o.Score).HasConversion(new OffsetConverter());
            mb.Entity<Order>().Property<Status>(o => o.Status).HasConversion(new StatusConverter());
            mb.Entity<Order>().Property<bool>(o => o.Flag).HasConversion(new BoolYnConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Async_count_equality_applies_int_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(1, await ctx.Query<Order>().CountAsync(o => o.Score == 7));   // stored 1007
        Assert.Equal(0, await ctx.Query<Order>().CountAsync(o => o.Score == 6));   // no model-6 row
    }

    [Fact]
    public void Sync_count_equality_applies_int_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(1, ctx.Query<Order>().Count(o => o.Score == 9));   // stored 1009
    }

    [Fact]
    public async Task Count_equality_applies_enum_string_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(1, await ctx.Query<Order>().CountAsync(o => o.Status == Status.Pending));
    }

    [Fact]
    public async Task Count_on_bool_converter_column_is_correct()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Flag stored as 'Y'/'N'; two rows are 'Y'. A boolean-literal predicate would miss — must defer.
        Assert.Equal(2, await ctx.Query<Order>().CountAsync(o => o.Flag));
    }

    [Fact]
    public void Full_pipeline_bool_converter_predicate_matches_stored_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // A compound predicate routes to the full pipeline (past the single-member fast path), exercising
        // the bool-member converter emission. Flag stored 'Y'/'N'; a raw `Flag = TRUE` would match nothing.
        Assert.Equal(new[] { 1, 3 }, ctx.Query<Order>().Where(o => o.Flag && o.Id > 0).ToList().Select(o => o.Id).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 2 }, ctx.Query<Order>().Where(o => !o.Flag && o.Id > 0).ToList().Select(o => o.Id).ToArray());
    }
}
