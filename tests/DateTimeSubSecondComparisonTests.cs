using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// An ordered comparison (&gt;/&lt;/&gt;=/&lt;=) of a DateTime or DateTimeOffset column must preserve sub-second
/// precision. SQLite's NormalizeDateTimeForCompare wrapped the column operand in datetime(), which renders
/// whole-second text and silently dropped the fraction — so `x.When &gt; cutoff` excluded every row in the same
/// second as the cutoff but with a larger fraction (a NULL-free silent row drop on `CreatedAt &gt; lastSync`
/// style filters). The OrderBy path already leaves DateTime bare and lex-compares the canonical text
/// correctly; the WHERE relational path must match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeSubSecondComparisonTests
{
    [Table("DssRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public DateTime When { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
        public DateTimeOffset OffA { get; set; }
        public DateTimeOffset OffB { get; set; }
    }

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DssRow (Id INTEGER PRIMARY KEY, [When] TEXT NOT NULL, Start TEXT NOT NULL, [End] TEXT NOT NULL, OffA TEXT NOT NULL, OffB TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static readonly DateTime Base = new DateTime(2020, 6, 15, 10, 30, 45, DateTimeKind.Unspecified);

    [Fact]
    public async Task Greater_than_param_preserves_column_subsecond()
    {
        using var ctx = Ctx();
        var when = Base.AddTicks(9_000_000);   // 45.9s
        ctx.Add(new Row { Id = 1, When = when, Start = when, End = when, OffA = when, OffB = when });
        await ctx.SaveChangesAsync();

        var cutoff = Base.AddTicks(5_000_000); // 45.5s
        var got = ctx.Query<Row>().Where(e => e.When > cutoff).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, got);        // 45.9 > 45.5
    }

    [Fact]
    public async Task Column_vs_column_less_than_preserves_subsecond()
    {
        using var ctx = Ctx();
        var start = Base.AddTicks(1_000_000);  // 45.1s
        var end = Base.AddTicks(9_000_000);    // 45.9s
        ctx.Add(new Row { Id = 1, When = start, Start = start, End = end, OffA = start, OffB = end });
        await ctx.SaveChangesAsync();

        var got = ctx.Query<Row>().Where(e => e.Start < e.End).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, got);        // 45.1 < 45.9
    }

    [Fact]
    public async Task DateTimeOffset_column_vs_column_less_than_preserves_subsecond()
    {
        using var ctx = Ctx();
        var a = new DateTimeOffset(Base.AddTicks(1_000_000), TimeSpan.Zero);
        var b = new DateTimeOffset(Base.AddTicks(9_000_000), TimeSpan.Zero);
        ctx.Add(new Row { Id = 1, When = Base, Start = Base, End = Base, OffA = a, OffB = b });
        await ctx.SaveChangesAsync();

        var got = ctx.Query<Row>().Where(e => e.OffA < e.OffB).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, got);
    }
}
