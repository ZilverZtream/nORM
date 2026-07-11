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
/// Query translation recurses once per expression node, so pathologically deep
/// trees — thousands of chained operators, or hundreds of dynamically registered
/// global filters — used to exhaust the thread stack and kill the process with an
/// uncatchable StackOverflowException. These tests pin the two defenses: global
/// filters compose into a single balanced predicate (logarithmic depth), and the
/// translator fails with a catchable NormQueryException when a user-authored tree
/// is too deep for the remaining stack.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DeeplyNestedQueryTranslationTests
{
    [Table("DnqItem")]
    private class DnqItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id    { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) BuildContext(DbContextOptions opts)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DnqItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL);
            INSERT INTO DnqItem (Value) VALUES (100), (399), (500);
            """;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Deeply_nested_operator_chain_fails_with_query_exception_instead_of_crashing()
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DnqItem>()
        };
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        var query = ctx.Query<DnqItem>();
        for (var i = 0; i < 20_000; i++)
            query = query.Where(e => e.Value >= 0);

        var ex = await Record.ExceptionAsync(() => query.CountAsync());

        Assert.NotNull(ex);
        Assert.Contains("nested too deeply", ex!.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Hundreds_of_global_filters_compose_into_one_balanced_predicate_and_apply_correctly()
    {
        // One nested Where per filter would recurse linearly during translation and
        // overflow the stack in the hundreds; the balanced composition keeps depth
        // logarithmic, so this must translate and filter correctly.
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DnqItem>(),
            MaxQueryWhereConditions = int.MaxValue,
            MaxQueryComplexityCost = int.MaxValue
        };
        for (var i = 0; i < 400; i++)
        {
            var threshold = i;
            opts.AddGlobalFilter<DnqItem>(e => e.Value >= threshold);
        }
        var (cn, ctx) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        // The strictest registered filter is Value >= 399, so of the seeded
        // values 100 / 399 / 500 only the last two survive.
        var rows = await ctx.Query<DnqItem>().ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.True(r.Value >= 399));
    }
}
