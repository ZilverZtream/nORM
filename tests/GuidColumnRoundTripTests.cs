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
/// A non-key Guid column must round-trip. nORM stores every Guid as a 36-char TEXT string
/// (SqliteProvider.CreateParameter), so a read path that returns the raw string must convert it back —
/// Convert.ChangeType cannot (Guid is not IConvertible), so the materializer needs an explicit case, like
/// the DateTimeOffset/TimeSpan/char cases beside it. Covers tracked, AsNoTracking, and projected reads.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GuidColumnRoundTripTests
{
    [Table("GuidRt")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public Guid Tag { get; set; }
        // A converter column on the same entity flips the materializer to the generic reader path, which
        // reads Tag as a raw string and must still convert it back to Guid.
        public int Score { get; set; }
    }

    // Model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static readonly Guid Sample = new("11112222-3333-4444-5555-666677778888");

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GuidRt (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL, Score INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Row>().HasKey(r => r.Id);
            mb.Entity<Row>().Property<int>(r => r.Score).HasConversion(new OffsetConverter());
        }};
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Row { Id = 1, Tag = Sample, Score = 5 });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    [Fact]
    public void Tracked_read_round_trips_guid()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(Sample, ctx.Query<Row>().First(r => r.Id == 1).Tag);
    }

    [Fact]
    public void AsNoTracking_read_round_trips_guid()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(Sample, ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().First(r => r.Id == 1).Tag);
    }

    [Fact]
    public void Projected_guid_round_trips()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(Sample, ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Tag).First());
    }
}
