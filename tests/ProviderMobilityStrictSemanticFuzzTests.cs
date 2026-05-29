using System;
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

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class ProviderMobilityStrictSemanticFuzzTests
{
    [Fact]
    public async Task Strict_provider_mobility_matches_linq_to_objects_for_common_query_profile()
    {
        var expected = BuildRows(seed: 8675309);

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateSchemaAsync(connection);

        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);
        await ctx.BulkInsertAsync(expected);

        var categories = new[] { "hardware", "books" };
        var page = await ctx.Query<StrictFuzzRow>()
            .Where(row => row.Active && categories.Contains(row.Category) && row.Score >= 20 && row.Amount < 80m)
            .OrderBy(row => row.Category)
            .ThenByDescending(row => row.Score)
            .Skip(1)
            .Take(7)
            .Select(row => new StrictFuzzDto
            {
                Id = row.Id,
                Category = row.Category,
                Label = row.Name + ":" + row.Score,
                Amount = row.Amount
            })
            .ToListAsync();

        var expectedPage = expected
            .Where(row => row.Active && categories.Contains(row.Category) && row.Score >= 20 && row.Amount < 80m)
            .OrderBy(row => row.Category)
            .ThenByDescending(row => row.Score)
            .Skip(1)
            .Take(7)
            .Select(row => new StrictFuzzDto
            {
                Id = row.Id,
                Category = row.Category,
                Label = row.Name + ":" + row.Score,
                Amount = row.Amount
            })
            .ToArray();

        Assert.Equal(expectedPage.Select(row => row.Id), page.Select(row => row.Id));
        Assert.Equal(expectedPage.Select(row => row.Label), page.Select(row => row.Label));

        var groups = await ctx.Query<StrictFuzzRow>()
            .Where(row => row.Active)
            .GroupBy(row => row.Category)
            .Select(group => new StrictFuzzGroupDto
            {
                Category = group.Key,
                Count = group.Count(),
                TotalScore = group.Sum(row => row.Score),
                MaxScore = group.Max(row => row.Score)
            })
            .OrderBy(group => group.Category)
            .ToListAsync();

        var expectedGroups = expected
            .Where(row => row.Active)
            .GroupBy(row => row.Category)
            .Select(group => new StrictFuzzGroupDto
            {
                Category = group.Key,
                Count = group.Count(),
                TotalScore = group.Sum(row => row.Score),
                MaxScore = group.Max(row => row.Score)
            })
            .OrderBy(group => group.Category)
            .ToArray();

        Assert.Equal(expectedGroups.Select(group => group.Category), groups.Select(group => group.Category));
        Assert.Equal(expectedGroups.Select(group => group.Count), groups.Select(group => group.Count));
        Assert.Equal(expectedGroups.Select(group => group.TotalScore), groups.Select(group => group.TotalScore));
        Assert.Equal(expectedGroups.Select(group => group.MaxScore), groups.Select(group => group.MaxScore));

        var compiled = Norm.CompileQuery<DbContext, int, StrictFuzzRow>(
            (db, minScore) => db.Query<StrictFuzzRow>()
                .Where(row => row.Active && row.Score >= minScore)
                .OrderBy(row => row.Id));
        var compiledRows = await compiled(ctx, 90);
        Assert.Equal(
            expected.Where(row => row.Active && row.Score >= 90).OrderBy(row => row.Id).Select(row => row.Id),
            compiledRows.Select(row => row.Id));
    }

    private static List<StrictFuzzRow> BuildRows(int seed)
    {
        var random = new Random(seed);
        var categories = new[] { "hardware", "books", "games", "office" };
        var rows = new List<StrictFuzzRow>(64);
        for (var i = 1; i <= 64; i++)
        {
            var score = random.Next(0, 120);
            rows.Add(new StrictFuzzRow
            {
                Id = i,
                Category = categories[random.Next(categories.Length)],
                Name = "item-" + i.ToString("00"),
                Score = score,
                Amount = decimal.Round((decimal)(random.NextDouble() * 100), 2),
                Active = score % 5 != 0
            });
        }

        return rows;
    }

    private static async Task CreateSchemaAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE StrictFuzzRow (
                Id INTEGER PRIMARY KEY,
                Category TEXT NOT NULL,
                Name TEXT NOT NULL,
                Score INTEGER NOT NULL,
                Amount NUMERIC NOT NULL,
                Active INTEGER NOT NULL
            );
            """;
        await command.ExecuteNonQueryAsync();
    }

    [Table("StrictFuzzRow")]
    public sealed class StrictFuzzRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = "";
        public string Name { get; set; } = "";
        public int Score { get; set; }
        public decimal Amount { get; set; }
        public bool Active { get; set; }
    }

    public sealed class StrictFuzzDto
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
        public string Label { get; set; } = "";
        public decimal Amount { get; set; }
    }

    public sealed class StrictFuzzGroupDto
    {
        public string Category { get; set; } = "";
        public int Count { get; set; }
        public int TotalScore { get; set; }
        public int MaxScore { get; set; }
    }
}
