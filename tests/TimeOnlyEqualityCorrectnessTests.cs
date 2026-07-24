using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A <see cref="TimeOnly"/> equality/ordering must match by value, not by stored TEXT format. Rows 1 and 2
/// are both 12:00:00 (canonical and extra-fraction TEXT). This checks ABSOLUTE correctness (vs the known
/// value answer), not just fast-path-vs-full-translator agreement, so it catches a product-wide gap where
/// ETSV's comparison normalization covers TimeSpan but not TimeOnly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TimeOnlyEqualityCorrectnessTests
{
    [Table("ToEq_Test")]
    public sealed class ToRow
    {
        [Key] public int Id { get; set; }
        [Column("Tval")] public TimeOnly T { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE ToEq_Test (Id INTEGER PRIMARY KEY, Tval TEXT NOT NULL);" +
                "INSERT INTO ToEq_Test VALUES (1,'12:00:00'),(2,'12:00:00.0000000'),(3,'13:30:00');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task TimeOnly_equality_full_translator_matches_by_value()
    {
        using var ctx = NewCtx();
        var noon = new TimeOnly(12, 0, 0);
        var ids = await ctx.Query<ToRow>().Where(r => r.T == noon).Select(r => new { r.Id }).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task TimeOnly_equality_simple_where_matches_by_value()
    {
        using var ctx = NewCtx();
        var noon = new TimeOnly(12, 0, 0);
        var rows = await ctx.Query<ToRow>().Where(r => r.T == noon).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).OrderBy(x => x).ToArray());
    }
}
