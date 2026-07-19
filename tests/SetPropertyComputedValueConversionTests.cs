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
/// Pins ExecuteUpdate's SetProperty computed form (<c>SetProperty(x => x.Col, x => value)</c>) for values
/// that don't reference the row: a captured constant or a ternary/arithmetic over captured values. These
/// were inlined via <c>IFormattable.ToString</c>, which rendered a DateTime/Guid/TimeSpan unquoted (invalid
/// SQL or lost precision) and an enum as its unquoted name, and never applied the column's value converter.
/// They are now evaluated client-side and bound as converted parameters. Row-referencing computed values
/// (<c>x => x.Counter + 1</c>) still translate to column SQL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetPropertyComputedValueConversionTests
{
    public enum JobStatus { Open, Running, Done }

    [Table("SpJob")]
    public class Job
    {
        [Key] public int Id { get; set; }
        public DateTime ExpiresAt { get; set; }
        public JobStatus Status { get; set; }
        public int Counter { get; set; }
    }

    [Table("SpGuidJob")]
    public class GuidJob
    {
        [Key] public int Id { get; set; }
        public Guid Ref { get; set; }
    }

    private sealed class StatusConverter : ValueConverter<JobStatus, string>
    {
        public override object? ConvertToProvider(JobStatus value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<JobStatus>(value);
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SpJob (Id INTEGER PRIMARY KEY, ExpiresAt TEXT, Status TEXT NOT NULL, Counter INTEGER NOT NULL);
                CREATE TABLE SpGuidJob (Id INTEGER PRIMARY KEY, Ref TEXT NOT NULL);
                INSERT INTO SpGuidJob VALUES (1, '');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Job>().HasKey(j => j.Id);
            mb.Entity<Job>().Property<JobStatus>(j => j.Status).HasConversion(new StatusConverter());
            mb.Entity<GuidJob>().HasKey(j => j.Id);
        }};
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Job { Id = 1, ExpiresAt = new DateTime(2000, 1, 1), Status = JobStatus.Open, Counter = 10 });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    private static string RawScalar(SqliteConnection cn, string sql)
    {
        using var c = cn.CreateCommand();
        c.CommandText = sql;
        return (string)c.ExecuteScalar()!;
    }

    [Fact]
    public async Task Captured_datetime_is_bound_with_full_precision()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var when = new DateTime(2026, 3, 4, 5, 6, 7, 123, DateTimeKind.Unspecified);

        await ctx.Query<Job>().Where(j => j.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(j => j.ExpiresAt, j => when));

        var got = ctx.Query<Job>().AsNoTracking().First(j => j.Id == 1).ExpiresAt;
        Assert.Equal(when, got);
    }

    [Fact]
    public async Task Captured_guid_is_bound_as_a_parameter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var g = new Guid("11112222-3333-4444-5555-666677778888");

        await ctx.Query<GuidJob>().Where(j => j.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(j => j.Ref, j => g));

        // Read the raw stored text and parse: a valid, round-trippable Guid (not corrupt unquoted SQL).
        Assert.Equal(g, Guid.Parse(RawScalar(cn, "SELECT Ref FROM SpGuidJob WHERE Id = 1")));
    }

    [Fact]
    public async Task Captured_enum_through_converter_stores_the_provider_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var target = JobStatus.Running;

        await ctx.Query<Job>().Where(j => j.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(j => j.Status, j => target));

        Assert.Equal("Running", RawScalar(cn, "SELECT Status FROM SpJob WHERE Id = 1"));   // converter applied: name, not int
        Assert.Equal(JobStatus.Running, ctx.Query<Job>().AsNoTracking().First(j => j.Id == 1).Status);
    }

    [Fact]
    public async Task Client_side_enum_ternary_is_evaluated_and_converted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var done = true;

        await ctx.Query<Job>().Where(j => j.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(j => j.Status, j => done ? JobStatus.Done : JobStatus.Open));

        Assert.Equal("Done", RawScalar(cn, "SELECT Status FROM SpJob WHERE Id = 1"));
        Assert.Equal(JobStatus.Done, ctx.Query<Job>().AsNoTracking().First(j => j.Id == 1).Status);
    }

    [Fact]
    public async Task Row_referencing_computed_value_still_translates_to_column_sql()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        // Counter seeded at 10; server-side increment must still work (references the row).
        await ctx.Query<Job>().Where(j => j.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(j => j.Counter, j => j.Counter + 5));

        Assert.Equal(15, ctx.Query<Job>().AsNoTracking().First(j => j.Id == 1).Counter);
    }
}
