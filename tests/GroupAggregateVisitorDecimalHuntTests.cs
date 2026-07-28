using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: MIN/MAX/SUM/AVG over a TEXT-stored decimal column when the aggregate is emitted
/// through the ExpressionToSqlVisitor group-aggregate path (HAVING predicates and COMPUTED projection
/// bodies) rather than the SelectClauseVisitor / QueryTranslator bare-aggregate path. The visitor path
/// emits a PLAIN MIN(col)/MAX(col)/SUM(col) instead of routing decimal through the full-precision
/// provider hook (DecimalAggregateSql / MinMaxAggregateOperand), so MIN/MAX compare the decimal TEXT
/// lexically ('10.5' &lt; '2.0'). Each test is diffed against the LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupAggregateVisitorDecimalHuntTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("GavItem")]
    public sealed class GavItem
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    private static readonly GavItem[] Seed =
    {
        // Cat "a": min=2.0, max=10.5 (lexical min='10.5', lexical max='9.9')
        new GavItem { Id = 1, Cat = "a", Amount = 10.5m },
        new GavItem { Id = 2, Cat = "a", Amount = 2.0m },
        new GavItem { Id = 3, Cat = "a", Amount = 9.9m },
        // Cat "b": single small value
        new GavItem { Id = 4, Cat = "b", Amount = 1.0m },
    };

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GavItem (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, Amount TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GavItem>().HasKey(i => i.Id)
        });
        foreach (var r in Seed) _ctx.Add(new GavItem { Id = r.Id, Cat = r.Cat, Amount = r.Amount });
        await _ctx.SaveChangesAsync();
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ---- HAVING over a decimal MIN: group "a" has min 2.0, which is NOT > 5, so must be dropped. ----
    [Fact]
    public void Having_Min_over_decimal_filters_numerically()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Min(x => x.Amount) > 5m).Select(g => g.Key).OrderBy(k => k).ToList();
        var norm = _ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Min(x => x.Amount) > 5m).Select(g => g.Key).OrderBy(k => k).ToList();
        Assert.Equal(oracle, norm); // oracle = [] (a.min=2.0 not>5, b.min=1.0 not>5)
    }

    // ---- HAVING over a decimal MAX: group "a" has max 10.5, which IS > 10, so must be kept. ----
    [Fact]
    public void Having_Max_over_decimal_filters_numerically()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Max(x => x.Amount) > 10m).Select(g => g.Key).OrderBy(k => k).ToList();
        var norm = _ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Max(x => x.Amount) > 10m).Select(g => g.Key).OrderBy(k => k).ToList();
        Assert.Equal(oracle, norm); // oracle = ["a"] (a.max=10.5>10)
    }

    // ---- Computed projection body embedding a decimal MIN: value must equal the numeric min. ----
    [Fact]
    public void Computed_projection_Min_over_decimal_returns_numeric_min()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => g.Min(x => x.Amount) + 0m).Single();
        var norm = _ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => g.Min(x => x.Amount) + 0m).ToList().Single();
        Assert.Equal(oracle, norm); // oracle = 2.0
    }

    // ---- Computed projection body embedding a decimal MAX. ----
    [Fact]
    public void Computed_projection_Max_over_decimal_returns_numeric_max()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => g.Max(x => x.Amount) + 0m).Single();
        var norm = _ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => g.Max(x => x.Amount) + 0m).ToList().Single();
        Assert.Equal(oracle, norm); // oracle = 10.5
    }

    // ---- CLEAN-BILL CONTRAST: bare aggregate projection goes through the correct path and must pass. ----
    [Fact]
    public void Bare_projection_Min_over_decimal_is_correct()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => new { M = g.Min(x => x.Amount) }).Single();
        var norm = _ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => new { M = g.Min(x => x.Amount) }).ToList().Single();
        Assert.Equal(oracle.M, norm.M); // 2.0 via the SelectClauseVisitor path
    }

    private sealed class SqlCapture : BaseDbCommandInterceptor
    {
        public SqlCapture() : base(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance) { }
        public string? LastSql;
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            LastSql = command.CommandText;
            return base.ReaderExecuting(command, context);
        }
    }

    [Fact]
    public void Emitted_SQL_for_having_min_max_uses_plain_aggregate()
    {
        var capture = new SqlCapture();
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<GavItem>().HasKey(i => i.Id) };
        opts.CommandInterceptors.Add(capture);
        using var ctx = new DbContext(_cn, new SqliteProvider(), opts, ownsConnection: false);

        _ = ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Min(x => x.Amount) > 5m).Select(g => g.Key).ToList();
        var minSql = capture.LastSql;

        _ = ctx.Query<GavItem>().GroupBy(x => x.Cat).Where(g => g.Max(x => x.Amount) > 10m).Select(g => g.Key).ToList();
        var maxSql = capture.LastSql;

        // The correct emit routes a decimal MIN/MAX through the NORM_DECIMAL collation so the extreme
        // is chosen numerically (see SqliteProvider.DecimalAggregateSql). The buggy emit is a bare
        // MIN("Amount") / MAX("Amount") over the TEXT column, which compares lexically.
        Assert.Contains("NORM_DECIMAL", minSql);   // FAILS FIRST: emits CAST(MIN("Amount") AS REAL)
        Assert.Contains("NORM_DECIMAL", maxSql);
    }
}
