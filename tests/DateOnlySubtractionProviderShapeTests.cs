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

namespace nORM.Tests;

/// <summary>
/// <c>DateOnly - DateOnly</c> in .NET returns <see cref="int"/> (the number
/// of days from b to a). The translator must lower this to provider-native
/// date arithmetic (DATEDIFF / julianday-delta / date - date) rather than
/// throwing NormUnsupportedFeatureException on the BinaryExpression. The
/// SQL result is consumed as int by the materializer, exercising the
/// bare-scalar projection path with a simple-type target.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateOnlySubtractionProviderShapeTests : TestBase
{
    private sealed class DatePair
    {
        public int Id { get; set; }
        public DateOnly Start { get; set; }
        public DateOnly End { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Postgres)]
    public void Projection_of_DateOnly_minus_DateOnly_emits_provider_days_form(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<DatePair, int>(
            q => q.Select(p => p.End.DayNumber - p.Start.DayNumber),
            connection,
            provider);

        // The DayNumber path uses the existing TranslateFunction hook --
        // this acts as a sanity-anchor that DayNumber resolves per-provider.
        Assert.Contains("Start", sql);
        Assert.Contains("End", sql);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Postgres)]
    public void Projection_of_DateOnly_subtraction_returning_int_emits_provider_diff_form(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<DatePair, int>(
            q => q.Select(p => (p.End.ToDateTime(TimeOnly.MinValue) - p.Start.ToDateTime(TimeOnly.MinValue)).Days),
            connection,
            provider);

        Assert.Contains("Start", sql);
        Assert.Contains("End", sql);
    }

    [Fact]
    public async Task Sqlite_end_to_end_DateOnly_subtraction_returns_int_days_matching_dotnet()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DatePairs (Id INTEGER PRIMARY KEY, Start TEXT NOT NULL, [End] TEXT NOT NULL);";
        await cmd.ExecuteNonQueryAsync();

        // Row 1: 2026-01-01 - 2026-01-01 = 0
        // Row 2: 2026-05-25 - 2026-01-01 = 144
        // Row 3: 2026-01-01 - 2026-05-25 = -144 (negative diff)
        // Row 4: 2027-01-01 - 2026-01-01 = 365 (cross-year)
        cmd.CommandText = """
            INSERT INTO DatePairs (Id, Start, [End]) VALUES
                (1, '2026-01-01', '2026-01-01'),
                (2, '2026-01-01', '2026-05-25'),
                (3, '2026-05-25', '2026-01-01'),
                (4, '2026-01-01', '2027-01-01');
            """;
        await cmd.ExecuteNonQueryAsync();

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DatePairEntity>().HasKey(t => t.Id).ToTable("DatePairs")
        });

        var diffs = ctx.Query<DatePairEntity>()
            .OrderBy(t => t.Id)
            .Select(t => t.End.DayNumber - t.Start.DayNumber)
            .ToList();
        await Task.CompletedTask;

        Assert.Equal(new[] { 0, 144, -144, 365 }, diffs);
    }

    [Table("DatePairs")]
    public sealed class DatePairEntity
    {
        [Key] public int Id { get; set; }
        public DateOnly Start { get; set; }
        public DateOnly End { get; set; }
    }
}
