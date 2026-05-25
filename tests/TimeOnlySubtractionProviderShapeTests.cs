using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// `TimeOnly - TimeOnly` lowers to a TimeSpan in .NET (the operator wraps
/// around 24h: when t1 lt t2 the result is t1 - t2 + 24h, yielding a value
/// in [0, 24h)). The translator must emit REAL seconds in the same range
/// so the materializer's existing "REAL seconds -> TimeSpan.FromSeconds"
/// path reconstructs the duration correctly across providers.
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlySubtractionProviderShapeTests : TestBase
{
    [Table("TimePairs")]
    public sealed class TimePair
    {
        [Key] public int Id { get; set; }
        public TimeOnly A { get; set; }
        public TimeOnly B { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Postgres)]
    public void Projection_of_TimeOnly_minus_TimeOnly_emits_provider_seconds_form(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<TimePair, TimeSpan>(
            q => q.Select(p => p.A - p.B),
            connection,
            provider);

        // Provider-specific anchor: enough to fail loudly if the subtraction
        // falls back to a literal `A - B` text-subtraction (which silently
        // returns 0 on TEXT-storage and TIME-only-arithmetic on numeric types).
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("substr", sql); // text parse of HH:mm:ss
                Assert.Contains("86400", sql);   // wrap modulus
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("DATEDIFF", sql);
                Assert.Contains("86400", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("EXTRACT(EPOCH", sql);
                Assert.Contains("86400", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("TIME_TO_SEC", sql);
                Assert.Contains("86400", sql);
                break;
        }
    }

    [Fact]
    public void Sqlite_end_to_end_TimeOnly_subtraction_wraps_around_midnight()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TimePairs (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        // Row 1: A=10:30:00, B=09:00:00  ->  +01:30:00
        // Row 2: A=01:00:00, B=23:00:00  ->  +02:00:00 (wrap: 1-23+24)
        // Row 3: A=00:00:00, B=00:00:00  ->  00:00:00
        cmd.CommandText = "INSERT INTO TimePairs (Id, A, B) VALUES (1, '10:30:00', '09:00:00'), (2, '01:00:00', '23:00:00'), (3, '00:00:00', '00:00:00');";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<TimePair>().HasKey(t => t.Id)
        });

        var diffs = ctx.Query<TimePair>().OrderBy(t => t.Id).Select(t => t.A - t.B).ToList();

        Assert.Equal(3, diffs.Count);
        Assert.Equal(new TimeSpan(1, 30, 0), diffs[0]);
        // .NET TimeOnly.op_Subtraction wraps: (01:00 - 23:00) = +02:00:00, not -22:00:00.
        Assert.Equal(new TimeSpan(2, 0, 0), diffs[1]);
        Assert.Equal(TimeSpan.Zero, diffs[2]);
    }
}
