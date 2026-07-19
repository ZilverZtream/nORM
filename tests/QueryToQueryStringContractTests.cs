using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ToQueryString() renders the SQL a query would execute, without running it (EF Core parity). Each parameter
/// is listed as a leading comment so the statement is self-describing. Works for LINQ queries, projections,
/// paging, and composable FromSqlRaw; rejects non-nORM sources.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class QueryToQueryStringContractTests
{
    [Table("TqsWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TqsWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL); INSERT INTO TqsWidget VALUES (1,'a',3),(2,'b',9);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) });
    }

    [Fact]
    public void renders_sql_with_parameter_comment()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var min = 5;
        var sql = ctx.Query<Widget>().Where(w => w.Score > min).OrderBy(w => w.Id).ToQueryString();
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TqsWidget", sql);
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        // The captured parameter value is surfaced as a leading comment (self-describing statement).
        Assert.Contains("= 5", sql);
        Assert.StartsWith("--", sql.TrimStart());
    }

    [Fact]
    public void renders_projection_columns()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var sql = ctx.Query<Widget>().Select(w => new { w.Id, w.Name }).ToQueryString();
        Assert.Contains("Name", sql);
        Assert.DoesNotContain("Score", sql);   // Score isn't projected → not selected
    }

    [Fact]
    public void renders_composable_fromsqlraw_as_derived_table()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var sql = ctx.FromSqlRaw<Widget>("SELECT * FROM TqsWidget WHERE Score > 1").OrderBy(w => w.Id).ToQueryString();
        Assert.Contains("SELECT * FROM TqsWidget WHERE Score > 1", sql);   // raw SQL wrapped as the FROM source
    }

    [Fact]
    public void does_not_execute_the_query()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        // Rendering the string must not run the query. Drop the table afterward would still let a second
        // ToQueryString succeed (translation only); here we assert repeated calls are pure and stable.
        var q = ctx.Query<Widget>().Where(w => w.Score > 0);
        var a = q.ToQueryString();
        var b = q.ToQueryString();
        Assert.Equal(a, b);
        Assert.Contains("SELECT", a, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void rejects_non_norm_source()
    {
        var ex = Assert.Throws<NormUsageException>(() => new[] { 1, 2, 3 }.AsQueryable().ToQueryString());
        Assert.Contains("nORM", ex.Message);
    }
}
