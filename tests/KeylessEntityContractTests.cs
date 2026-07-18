using System;
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
/// Pins EF-parity keyless entities (HasNoKey): a query type with no primary key, materialized from
/// queries (views / read models) but never change-tracked and not savable. The key convention does not
/// apply to a keyless entity even when it has an "Id" property.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class KeylessEntityContractTests
{
    [Table("KeSummary")]
    public class RegionSummary
    {
        public string Region { get; set; } = "";
        public int Total { get; set; }
    }

    // Has an "Id" property that the convention would otherwise adopt as the key.
    [Table("KeReport")]
    public class Report
    {
        public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<RegionSummary>().HasNoKey();
                mb.Entity<Report>().HasNoKey();
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE KeSummary (Region TEXT NOT NULL, Total INTEGER NOT NULL);
            INSERT INTO KeSummary VALUES ('east', 10), ('west', 20), ('east', 5);
            CREATE TABLE KeReport (Id INTEGER NOT NULL, Title TEXT NOT NULL);
            INSERT INTO KeReport VALUES (1, 'a'), (1, 'b');
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Keyless_entity_queries_with_linq()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var totals = await ctx.Query<RegionSummary>()
            .Where(r => r.Total >= 5)
            .OrderByDescending(r => r.Total)
            .ToListAsync();

        Assert.Equal(new[] { 20, 10, 5 }, totals.Select(t => t.Total).ToArray());
        Assert.Equal(35, totals.Sum(t => t.Total));
    }

    [Fact]
    public async Task Keyless_entity_is_not_tracked()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var row = (await ctx.Query<RegionSummary>().Where(r => r.Region == "west").ToListAsync()).Single();

        // Not tracked → no entry, and mutating + saving persists nothing.
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(row));
        row.Total = 999;
        Assert.Equal(0, await ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task HasNoKey_suppresses_the_Id_key_convention()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        // The Report type has an "Id" property that the convention would adopt as a key; HasNoKey
        // opts out, so it stays keyless — proven by it being untracked and a duplicate-Id query
        // returning both rows rather than being identity-collapsed.
        Assert.Empty(ctx.GetMapping(typeof(Report)).KeyColumns);

        var reports = await ctx.Query<Report>().Where(r => r.Id == 1).ToListAsync();
        Assert.Equal(2, reports.Count);
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(reports[0]));
    }
}
