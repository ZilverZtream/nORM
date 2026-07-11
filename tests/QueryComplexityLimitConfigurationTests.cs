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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Query admission limits (join depth, WHERE conditions, parameter count, estimated
/// complexity cost) scale with available system memory by default, which makes
/// admission machine-dependent: the same query can translate on a large developer
/// box and be rejected inside a small container. These tests pin the explicit
/// DbContextOptions overrides that give callers deterministic admission.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class QueryComplexityLimitConfigurationTests
{
    [Table("QclItem")]
    private class QclItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id    { get; set; }
        public int Value { get; set; }
    }

    [Table("QclOther")]
    private class QclOther
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id   { get; set; }
        public int Code { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) BuildContext(DbContextOptions opts)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE QclItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL);
            CREATE TABLE QclOther (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code INTEGER NOT NULL);
            INSERT INTO QclItem (Value) VALUES (2), (3), (7);
            INSERT INTO QclOther (Code) VALUES (2), (3);
            """;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<QclItem>();
            mb.Entity<QclOther>();
        }
    };

    private static string FlattenMessages(Exception ex)
    {
        var messages = new List<string>();
        for (Exception? current = ex; current != null; current = current.InnerException)
            messages.Add(current.Message);
        return string.Join(" | ", messages);
    }

    [Fact]
    public async Task Explicit_where_condition_limit_rejects_predicate_exceeding_it()
    {
        var opts = Options();
        opts.MaxQueryWhereConditions = 1;
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        // Constants unique to this test: admission is a translation-time guard, so a
        // query shape another test already translated would be served from the plan
        // cache without re-entering the analyzer.
        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<QclItem>().Where(e => e.Value > 12 && e.Value < 45).ToListAsync());

        Assert.NotNull(ex);
        Assert.Contains("maximum WHERE conditions of 1", FlattenMessages(ex!), StringComparison.Ordinal);
        Assert.Contains("MaxQueryWhereConditions", FlattenMessages(ex!), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Explicit_where_condition_limit_admits_predicate_within_it()
    {
        var opts = Options();
        opts.MaxQueryWhereConditions = 10;
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        var rows = await ctx.Query<QclItem>().Where(e => e.Value > 1 && e.Value < 5).ToListAsync();

        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Explicit_complexity_cost_ceiling_rejects_query_above_it()
    {
        // A two-condition predicate costs 200 in the analyzer's model
        // (base 100 + 2 conditions x 50), so a ceiling of 120 rejects it.
        // Constants unique to this test: see the WHERE-condition rejection test.
        var opts = Options();
        opts.MaxQueryComplexityCost = 120;
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<QclItem>().Where(e => e.Value > 11 && e.Value < 44).ToListAsync());

        Assert.NotNull(ex);
        Assert.Contains("Query complexity too high", FlattenMessages(ex!), StringComparison.Ordinal);
        Assert.Contains("MaxQueryComplexityCost", FlattenMessages(ex!), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Explicit_join_depth_limit_rejects_query_with_more_joins()
    {
        var opts = Options();
        opts.MaxQueryJoinDepth = 1;
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        var query = ctx.Query<QclItem>()
            .Join(ctx.Query<QclOther>(), i => i.Value, o => o.Code, (i, o) => new { i.Id, o.Code })
            .Join(ctx.Query<QclOther>(), x => x.Code, o => o.Code, (x, o) => new { x.Id, Second = o.Id });

        var ex = await Record.ExceptionAsync(() => query.ToListAsync());

        Assert.NotNull(ex);
        Assert.Contains("maximum join depth of 1", FlattenMessages(ex!), StringComparison.Ordinal);
        Assert.Contains("MaxQueryJoinDepth", FlattenMessages(ex!), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Explicit_parameter_count_limit_rejects_large_local_contains()
    {
        var opts = Options();
        opts.MaxQueryParameterCount = 3;
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        var values = new List<int> { 1, 2, 3, 4, 5 };
        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<QclItem>().Where(e => values.Contains(e.Value)).ToListAsync());

        Assert.NotNull(ex);
        Assert.Contains("maximum parameter count of 3", FlattenMessages(ex!), StringComparison.Ordinal);
        Assert.Contains("MaxQueryParameterCount", FlattenMessages(ex!), StringComparison.Ordinal);
    }

    [Fact]
    public void Explicit_limit_is_enforced_even_when_analysis_was_cached_by_permissive_options()
    {
        // The analyzer caches structural metrics by expression fingerprint; admission
        // must be re-checked per call so options with stricter limits cannot ride a
        // permissive caller's cache entry.
        var analyzer = new nORM.Query.AdaptiveQueryComplexityAnalyzer(new SystemMemoryMonitor());
        var expression = new List<QclItem>().AsQueryable()
            .Where(e => e.Value > 21 && e.Value < 77).Expression;

        var permissive = new DbContextOptions { MaxQueryWhereConditions = 10 };
        var info = analyzer.AnalyzeQuery(expression, permissive);
        Assert.Equal(2, info.WhereConditionCount);

        var strict = new DbContextOptions { MaxQueryWhereConditions = 1 };
        var ex = Record.Exception(() => analyzer.AnalyzeQuery(expression, strict));

        Assert.NotNull(ex);
        Assert.Contains("maximum WHERE conditions of 1", ex!.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Non_positive_explicit_limits_are_rejected(int value)
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxQueryJoinDepth = value);
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxQueryWhereConditions = value);
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxQueryParameterCount = value);
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxQueryComplexityCost = value);
    }

    [Fact]
    public void Null_resets_explicit_limits_to_memory_scaled_defaults()
    {
        var opts = new DbContextOptions
        {
            MaxQueryJoinDepth = 5,
            MaxQueryWhereConditions = 5,
            MaxQueryParameterCount = 5,
            MaxQueryComplexityCost = 5
        };

        opts.MaxQueryJoinDepth = null;
        opts.MaxQueryWhereConditions = null;
        opts.MaxQueryParameterCount = null;
        opts.MaxQueryComplexityCost = null;

        Assert.Null(opts.MaxQueryJoinDepth);
        Assert.Null(opts.MaxQueryWhereConditions);
        Assert.Null(opts.MaxQueryParameterCount);
        Assert.Null(opts.MaxQueryComplexityCost);
    }

    [Fact]
    public void Clone_copies_explicit_query_complexity_limits()
    {
        var opts = new DbContextOptions
        {
            MaxQueryJoinDepth = 11,
            MaxQueryWhereConditions = 22,
            MaxQueryParameterCount = 33,
            MaxQueryComplexityCost = 44
        };

        var clone = opts.Clone();

        Assert.Equal(11, clone.MaxQueryJoinDepth);
        Assert.Equal(22, clone.MaxQueryWhereConditions);
        Assert.Equal(33, clone.MaxQueryParameterCount);
        Assert.Equal(44, clone.MaxQueryComplexityCost);
    }
}
