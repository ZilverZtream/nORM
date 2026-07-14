using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The Aggregate-to-SQL fold rewrite must combine the seed with the SQL-computed
/// extremum for every accumulator type the fold shapes can carry — float folds
/// and comparable non-numeric accumulators (DateTime), with the seed winning or
/// losing per the comparison, not just the int/long/double/decimal quartet.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AggregateFoldAccumulatorTypeTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("AggFold_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public float Score { get; set; }
        public DateTime When { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx, Row[] Rows) CreateDb()
    {
        var cs = $"Data Source=file:aggfold_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AggFold_Test (Id INTEGER PRIMARY KEY, Score REAL NOT NULL, [When] TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var rows = new[]
        {
            new Row { Id = 1, Score = 3.5f, When = new DateTime(2024, 3, 1) },
            new Row { Id = 2, Score = 8.25f, When = new DateTime(2025, 1, 15) },
            new Row { Id = 3, Score = 1.75f, When = new DateTime(2023, 11, 30) },
        };
        return (keeper, ctx, rows);
    }

    private static async Task SeedAsync(DbContext ctx, Row[] rows)
    {
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Float_max_fold_combines_seed_with_the_sql_extremum()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Seed below the data max: the SQL extremum wins.
        var below = ctx.Query<Row>().Aggregate(2.0f, (acc, x) => Math.Max(acc, x.Score));
        Assert.Equal(rows.Aggregate(2.0f, (acc, x) => Math.Max(acc, x.Score)), below);

        // Seed above the data max: the seed wins.
        var above = ctx.Query<Row>().Aggregate(99.5f, (acc, x) => Math.Max(acc, x.Score));
        Assert.Equal(99.5f, above);
    }

    [Fact]
    public async Task Float_min_fold_combines_seed_with_the_sql_extremum()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        var result = ctx.Query<Row>().Aggregate(2.0f, (acc, x) => Math.Min(acc, x.Score));
        Assert.Equal(rows.Aggregate(2.0f, (acc, x) => Math.Min(acc, x.Score)), result);
    }

    [Fact]
    public async Task DateTime_max_fold_compares_the_seed_directly()
    {
        var (keeper, ctx, rows) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;
        await SeedAsync(ctx, rows);

        // Seed below the data max: the SQL extremum wins.
        var floor = new DateTime(2024, 1, 1);
        var latest = ctx.Query<Row>().Select(x => x.When).Aggregate(floor, (acc, w) => w > acc ? w : acc);
        Assert.Equal(rows.Select(x => x.When).Aggregate(floor, (acc, w) => w > acc ? w : acc), latest);

        // Seed above the data max: the seed wins.
        var ceiling = new DateTime(2030, 6, 1);
        var seedWins = ctx.Query<Row>().Select(x => x.When).Aggregate(ceiling, (acc, w) => w > acc ? w : acc);
        Assert.Equal(ceiling, seedWins);
    }
}
