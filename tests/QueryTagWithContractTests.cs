using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the EF-parity <c>TagWith</c> query surface: the tag is prepended to the generated SQL as an
/// injection-safe line comment (for log/APM correlation) without changing results.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class QueryTagWithContractTests
{
    [Table("TwRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class SqlCapture : BaseDbCommandInterceptor
    {
        public SqlCapture() : base(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance) { }
        public string? LastSql;
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            LastSql = command.CommandText;
            return base.ReaderExecutingAsync(command, context, ct);
        }
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            LastSql = command.CommandText;
            return base.ReaderExecuting(command, context);
        }
    }

    private static (SqliteConnection cn, SqlCapture capture, DbContext ctx) Bootstrap(string seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = seed;
            cmd.ExecuteNonQuery();
        }
        var capture = new SqlCapture();
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        opts.CommandInterceptors.Add(capture);
        return (cn, capture, new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false));
    }

    [Fact]
    public async Task TagWith_prepends_the_comment_and_keeps_results_unchanged()
    {
        var (cn, capture, ctx) = Bootstrap(
            "CREATE TABLE TwRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO TwRow VALUES (1,'a'),(2,'b');");
        using var _cn = cn;
        await using var _ctx = ctx;

        var rows = await ctx.Query<Row>().TagWith("nightly-report").Where(r => r.Id == 1).ToListAsync();

        Assert.Single(rows);
        Assert.Equal("a", rows[0].Name);
        Assert.NotNull(capture.LastSql);
        Assert.StartsWith("-- nightly-report", capture.LastSql);
    }

    [Fact]
    public async Task TagWith_is_injection_safe_for_multiline_tags()
    {
        var (cn, capture, ctx) = Bootstrap(
            "CREATE TABLE TwRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO TwRow VALUES (1,'a');");
        using var _cn = cn;
        await using var _ctx = ctx;

        // A tag with a newline and a DROP must be fully commented out, never executed.
        var rows = await ctx.Query<Row>().TagWith("line1\nDROP TABLE TwRow").ToListAsync();

        Assert.Single(rows);                                     // table survived → the DROP was a comment
        Assert.Contains("-- line1", capture.LastSql);
        Assert.Contains("-- DROP TABLE TwRow", capture.LastSql); // commented, not executable
    }
}
