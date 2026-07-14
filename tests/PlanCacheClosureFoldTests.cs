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
/// Some translators fold a closure-captured value into the SQL itself — a
/// StringComparison argument selects between case-folded and binary LIKE
/// shapes, TimeOnly.AddHours bakes the delta, ToString bakes the format.
/// Such plans are execution-specific: a cached plan would replay the FIRST
/// execution's folded value for every later call that captures a different
/// one. These shapes must translate fresh per execution.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PlanCacheClosureFoldTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("PcFold_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task Closure_StringComparison_is_not_replayed_from_the_plan_cache()
    {
        var dbName = $"pcfold_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PcFold_Test (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new Row { Id = 1, Name = "alpha" });
        ctx.Add(new Row { Id = 2, Name = "ALPHA" });
        ctx.Add(new Row { Id = 3, Name = "beta" });
        await ctx.SaveChangesAsync();

        var rows = await ctx.Query<Row>().ToListAsync();

        // The SAME query shape runs with a DIFFERENT captured comparison each
        // time; the translated LIKE shape differs, so each execution must
        // reflect its own capture.
        foreach (var cmp in new[] { StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase, StringComparison.Ordinal })
        {
            var comparison = cmp;
            var prefix = "al";
            var expected = rows.Where(r => r.Name.StartsWith(prefix, comparison))
                .Select(r => r.Id).OrderBy(x => x).ToList();

            var actual = (await ctx.Query<Row>().Where(r => r.Name.StartsWith(prefix, comparison)).ToListAsync())
                .Select(r => r.Id).OrderBy(x => x).ToList();

            Assert.True(expected.SequenceEqual(actual),
                $"cmp={comparison}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }
}
