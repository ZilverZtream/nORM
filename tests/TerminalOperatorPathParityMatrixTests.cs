using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Table-driven terminal-operator parity matrix across execution paths.
/// Every case runs the same operator through the synchronous LINQ provider path,
/// the asynchronous extension path, and (for row-returning operators) the
/// source-generated materializer path, then compares result — or thrown
/// exception type — against LINQ-to-Objects on the identical row set. The
/// matrix covers empty, single-row, two-row, and multi-row datasets including
/// NULL values, plus ordered, filtered, and paged query shapes.
/// </summary>
[Trait("Category", "Fast")]
public class TerminalOperatorPathParityMatrixTests
{
    [Table("PathParityRow")]
    private class PathParityRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Name { get; set; } = string.Empty;
        public int IntValue { get; set; }
        public double DoubleValue { get; set; }
        public int? NullableInt { get; set; }
        public double? NullableDouble { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE PathParityRow (
                    Id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    CategoryId     INTEGER NOT NULL DEFAULT 0,
                    Name           TEXT    NOT NULL DEFAULT '',
                    IntValue       INTEGER NOT NULL DEFAULT 0,
                    DoubleValue    REAL    NOT NULL DEFAULT 0,
                    NullableInt    INTEGER,
                    NullableDouble REAL
                )";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(SqliteConnection cn, PathParityRow row)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO PathParityRow (CategoryId, Name, IntValue, DoubleValue, NullableInt, NullableDouble)
            VALUES (@c, @n, @i, @d, @ni, @nd)";
        cmd.Parameters.AddWithValue("@c", row.CategoryId);
        cmd.Parameters.AddWithValue("@n", row.Name);
        cmd.Parameters.AddWithValue("@i", row.IntValue);
        cmd.Parameters.AddWithValue("@d", row.DoubleValue);
        cmd.Parameters.AddWithValue("@ni", (object?)row.NullableInt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@nd", (object?)row.NullableDouble ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ── datasets ─────────────────────────────────────────────────────────────

    private static readonly IReadOnlyDictionary<string, PathParityRow[]> Datasets =
        new Dictionary<string, PathParityRow[]>
        {
            ["empty"] = Array.Empty<PathParityRow>(),
            ["one"] = new[]
            {
                new PathParityRow { Id = 1, CategoryId = 1, Name = "alpha", IntValue = 10, DoubleValue = 1.5, NullableInt = 5, NullableDouble = 0.5 },
            },
            ["two"] = new[]
            {
                new PathParityRow { Id = 1, CategoryId = 1, Name = "alpha", IntValue = 10, DoubleValue = 1.5, NullableInt = null, NullableDouble = 2.5 },
                new PathParityRow { Id = 2, CategoryId = 2, Name = "beta",  IntValue = 20, DoubleValue = 2.5, NullableInt = 7,    NullableDouble = null },
            },
            ["multi"] = new[]
            {
                new PathParityRow { Id = 1, CategoryId = 1, Name = "alpha", IntValue = 10, DoubleValue = 1.5,  NullableInt = null, NullableDouble = 2.5 },
                new PathParityRow { Id = 2, CategoryId = 2, Name = "beta",  IntValue = 20, DoubleValue = 2.5,  NullableInt = 7,    NullableDouble = null },
                new PathParityRow { Id = 3, CategoryId = 1, Name = "gamma", IntValue = 20, DoubleValue = -3.5, NullableInt = 3,    NullableDouble = 1.25 },
                new PathParityRow { Id = 4, CategoryId = 3, Name = "beta",  IntValue = -5, DoubleValue = 0.0,  NullableInt = null, NullableDouble = null },
                new PathParityRow { Id = 5, CategoryId = 1, Name = "delta", IntValue = 0,  DoubleValue = 4.25, NullableInt = 11,   NullableDouble = -0.75 },
            },
        };

    // ── operator cases ───────────────────────────────────────────────────────

    private sealed record OperatorCase(
        string Name,
        Func<IQueryable<PathParityRow>, object?> Sync,
        Func<IQueryable<PathParityRow>, Task<object?>> Async,
        Func<IEnumerable<PathParityRow>, object?> Oracle);

    // Row-returning results are normalized to the row Id so entity identity
    // differences between paths never mask a semantic comparison.
    private static object? NormalizeResult(object? result)
        => result is PathParityRow r ? r.Id : result;

    private static readonly OperatorCase[] Cases =
    {
        new("Count",
            q => q.Count(),
            async q => await q.CountAsync(),
            rows => rows.Count()),
        new("Count(predicate)",
            q => q.Count(x => x.CategoryId == 1),
            async q => await q.CountAsync(x => x.CategoryId == 1),
            rows => rows.Count(x => x.CategoryId == 1)),
        new("LongCount",
            q => q.LongCount(),
            async q => await q.LongCountAsync(),
            rows => rows.LongCount()),
        new("Any",
            q => q.Any(),
            async q => await q.AnyAsync(),
            rows => rows.Any()),
        new("Any(predicate)",
            q => q.Any(x => x.IntValue > 15),
            async q => await q.AnyAsync(x => x.IntValue > 15),
            rows => rows.Any(x => x.IntValue > 15)),
        new("All(predicate)",
            q => q.All(x => x.IntValue >= 0),
            async q => await q.AllAsync(x => x.IntValue >= 0),
            rows => rows.All(x => x.IntValue >= 0)),
        new("Sum(int)",
            q => q.Sum(x => x.IntValue),
            async q => await q.SumAsync(x => x.IntValue),
            rows => rows.Sum(x => x.IntValue)),
        new("Sum(nullable int)",
            q => q.Sum(x => x.NullableInt),
            async q => await q.SumAsync(x => x.NullableInt),
            rows => rows.Sum(x => x.NullableInt)),
        new("Min(int)",
            q => q.Min(x => x.IntValue),
            async q => await q.MinAsync(x => x.IntValue),
            rows => rows.Min(x => x.IntValue)),
        new("Max(int)",
            q => q.Max(x => x.IntValue),
            async q => await q.MaxAsync(x => x.IntValue),
            rows => rows.Max(x => x.IntValue)),
        new("Min(nullable int)",
            q => q.Min(x => x.NullableInt),
            async q => await q.MinAsync(x => x.NullableInt),
            rows => rows.Min(x => x.NullableInt)),
        new("Max(nullable int)",
            q => q.Max(x => x.NullableInt),
            async q => await q.MaxAsync(x => x.NullableInt),
            rows => rows.Max(x => x.NullableInt)),
        new("Average(double)",
            q => q.Average(x => x.DoubleValue),
            async q => await q.AverageAsync(x => x.DoubleValue),
            rows => rows.Average(x => x.DoubleValue)),
        new("Average(nullable double)",
            q => q.Average(x => x.NullableDouble),
            async q => await q.AverageAsync(x => x.NullableDouble),
            rows => rows.Average(x => x.NullableDouble)),
        new("First(ordered)",
            q => q.OrderBy(x => x.Name).ThenBy(x => x.Id).First(),
            async q => await q.OrderBy(x => x.Name).ThenBy(x => x.Id).FirstAsync(),
            rows => rows.OrderBy(x => x.Name).ThenBy(x => x.Id).First()),
        new("FirstOrDefault(ordered)",
            q => q.OrderBy(x => x.Name).ThenBy(x => x.Id).FirstOrDefault(),
            async q => await q.OrderBy(x => x.Name).ThenBy(x => x.Id).FirstOrDefaultAsync(),
            rows => rows.OrderBy(x => x.Name).ThenBy(x => x.Id).FirstOrDefault()),
        new("First(ordered, predicate)",
            q => q.OrderBy(x => x.Id).First(x => x.CategoryId == 1),
            async q => await q.OrderBy(x => x.Id).FirstAsync(x => x.CategoryId == 1),
            rows => rows.OrderBy(x => x.Id).First(x => x.CategoryId == 1)),
        new("Single",
            q => q.Single(),
            async q => await q.SingleAsync(),
            rows => rows.Single()),
        new("SingleOrDefault",
            q => q.SingleOrDefault(),
            async q => await q.SingleOrDefaultAsync(),
            rows => rows.SingleOrDefault()),
        new("Single(predicate)",
            q => q.Single(x => x.Name == "gamma"),
            async q => await q.SingleAsync(x => x.Name == "gamma"),
            rows => rows.Single(x => x.Name == "gamma")),
        new("Last(ordered)",
            q => q.OrderBy(x => x.Name).ThenBy(x => x.Id).Last(),
            async q => await q.OrderBy(x => x.Name).ThenBy(x => x.Id).LastAsync(),
            rows => rows.OrderBy(x => x.Name).ThenBy(x => x.Id).Last()),
        new("LastOrDefault(ordered)",
            q => q.OrderBy(x => x.Name).ThenBy(x => x.Id).LastOrDefault(),
            async q => await q.OrderBy(x => x.Name).ThenBy(x => x.Id).LastOrDefaultAsync(),
            rows => rows.OrderBy(x => x.Name).ThenBy(x => x.Id).LastOrDefault()),
        new("ElementAt(1, ordered)",
            q => q.OrderBy(x => x.Id).ElementAt(1),
            async q => await q.OrderBy(x => x.Id).ElementAtAsync(1),
            rows => rows.OrderBy(x => x.Id).ElementAt(1)),
        new("ElementAtOrDefault(10, ordered)",
            q => q.OrderBy(x => x.Id).ElementAtOrDefault(10),
            async q => await q.OrderBy(x => x.Id).ElementAtOrDefaultAsync(10),
            rows => rows.OrderBy(x => x.Id).ElementAtOrDefault(10)),
        new("First(paged)",
            q => q.OrderBy(x => x.Id).Skip(1).Take(2).First(),
            async q => await q.OrderBy(x => x.Id).Skip(1).Take(2).FirstAsync(),
            rows => rows.OrderBy(x => x.Id).Skip(1).Take(2).First()),
        new("Count(paged)",
            q => q.OrderBy(x => x.Id).Skip(1).Take(2).Count(),
            async q => await q.OrderBy(x => x.Id).Skip(1).Take(2).CountAsync(),
            rows => rows.OrderBy(x => x.Id).Skip(1).Take(2).Count()),
        new("Sum(filtered)",
            q => q.Where(x => x.CategoryId == 1).Sum(x => x.IntValue),
            async q => await q.Where(x => x.CategoryId == 1).SumAsync(x => x.IntValue),
            rows => rows.Where(x => x.CategoryId == 1).Sum(x => x.IntValue)),
        new("FirstOrDefault(filtered, no match)",
            q => q.Where(x => x.CategoryId == 99).OrderBy(x => x.Id).FirstOrDefault(),
            async q => await q.Where(x => x.CategoryId == 99).OrderBy(x => x.Id).FirstOrDefaultAsync(),
            rows => rows.Where(x => x.CategoryId == 99).OrderBy(x => x.Id).FirstOrDefault()),
        new("Min(int, filtered empty)",
            q => q.Where(x => x.CategoryId == 99).Min(x => x.IntValue),
            async q => await q.Where(x => x.CategoryId == 99).MinAsync(x => x.IntValue),
            rows => rows.Where(x => x.CategoryId == 99).Min(x => x.IntValue)),
        new("Max(nullable int, filtered empty)",
            q => q.Where(x => x.CategoryId == 99).Max(x => x.NullableInt),
            async q => await q.Where(x => x.CategoryId == 99).MaxAsync(x => x.NullableInt),
            rows => rows.Where(x => x.CategoryId == 99).Max(x => x.NullableInt)),
    };

    public static IEnumerable<object[]> MatrixCases()
    {
        foreach (var dataset in Datasets.Keys)
            foreach (var opCase in Cases)
                yield return new object[] { dataset, opCase.Name };
    }

    private static OperatorCase CaseByName(string name) => Cases.Single(c => c.Name == name);

    private sealed record Outcome(object? Result, Type? ExceptionType)
    {
        public static async Task<Outcome> CaptureAsync(Func<Task<object?>> run)
        {
            try
            {
                return new Outcome(NormalizeResult(await run()), null);
            }
            catch (Exception ex)
            {
                return new Outcome(null, ex.GetType());
            }
        }
    }

    private static void AssertOutcomeParity(string path, Outcome oracle, Outcome actual)
    {
        if (oracle.ExceptionType != null)
        {
            Assert.True(actual.ExceptionType != null,
                $"{path}: LINQ-to-Objects threw {oracle.ExceptionType.Name} but the query returned '{actual.Result}'.");
            Assert.True(oracle.ExceptionType == actual.ExceptionType,
                $"{path}: LINQ-to-Objects threw {oracle.ExceptionType.Name} but the query threw {actual.ExceptionType!.Name}.");
            return;
        }

        Assert.True(actual.ExceptionType == null,
            $"{path}: LINQ-to-Objects returned '{oracle.Result}' but the query threw {actual.ExceptionType?.Name}.");

        if (oracle.Result is double expectedDouble && actual.Result is double actualDouble)
        {
            Assert.True(Math.Abs(expectedDouble - actualDouble) < 1e-9,
                $"{path}: expected {expectedDouble}, got {actualDouble}.");
            return;
        }

        Assert.True(Equals(oracle.Result, actual.Result),
            $"{path}: expected '{oracle.Result ?? "null"}' ({oracle.Result?.GetType().Name}), got '{actual.Result ?? "null"}' ({actual.Result?.GetType().Name}).");
    }

    [Theory]
    [MemberData(nameof(MatrixCases))]
    public async Task Terminal_operator_matches_linq_to_objects_on_sync_and_async_paths(string dataset, string caseName)
    {
        var rows = Datasets[dataset];
        var opCase = CaseByName(caseName);

        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;
        foreach (var row in rows)
            Insert(cn, row);

        var oracle = await Outcome.CaptureAsync(() => Task.FromResult(opCase.Oracle(rows)));

        var syncOutcome = await Outcome.CaptureAsync(() => Task.FromResult(opCase.Sync(ctx.Query<PathParityRow>())));
        AssertOutcomeParity($"sync/{dataset}/{caseName}", oracle, syncOutcome);

        var asyncOutcome = await Outcome.CaptureAsync(() => opCase.Async(ctx.Query<PathParityRow>()));
        AssertOutcomeParity($"async/{dataset}/{caseName}", oracle, asyncOutcome);
    }

    // ── source-generated materializer path ──────────────────────────────────

    [Table("PathParityRow")]
    [GenerateMaterializer]
    internal sealed class PathParityRowGen
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Name { get; set; } = string.Empty;
        public int IntValue { get; set; }
        public double DoubleValue { get; set; }
        public int? NullableInt { get; set; }
        public double? NullableDouble { get; set; }
    }

    public static IEnumerable<object[]> SourceGenDatasets()
        => Datasets.Keys.Select(k => new object[] { k });

    /// <summary>
    /// Row-returning terminal operators must produce the same rows through the
    /// source-generated materializer as through the runtime reflection
    /// materializer, including exception behavior on empty and multi-row inputs.
    /// </summary>
    [Theory]
    [MemberData(nameof(SourceGenDatasets))]
    public async Task Row_returning_terminals_match_between_runtime_and_source_generated_materializers(string dataset)
    {
        var rows = Datasets[dataset];
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;
        foreach (var row in rows)
            Insert(cn, row);

        Assert.True(CompiledMaterializerStore.TryGet(typeof(PathParityRowGen), "PathParityRow", out _),
            "The source generator did not register a materializer for PathParityRowGen; the source-generated path is not being exercised.");

        async Task<Outcome> RunPlain(Func<IQueryable<PathParityRow>, Task<object?>> run)
            => await Outcome.CaptureAsync(() => run(ctx.Query<PathParityRow>()));
        async Task<Outcome> RunGen(Func<IQueryable<PathParityRowGen>, Task<object?>> run)
            => await Outcome.CaptureAsync(() => run(ctx.Query<PathParityRowGen>()));

        var pairs = new (string Name, Func<IQueryable<PathParityRow>, Task<object?>> Plain, Func<IQueryable<PathParityRowGen>, Task<object?>> Gen)[]
        {
            ("First(ordered)",
                async q => await q.OrderBy(x => x.Name).ThenBy(x => x.Id).FirstAsync(),
                async q => NormalizeGen(await q.OrderBy(x => x.Name).ThenBy(x => x.Id).FirstAsync())),
            ("Single",
                async q => await q.SingleAsync(),
                async q => NormalizeGen(await q.SingleAsync())),
            ("LastOrDefault(ordered)",
                async q => await q.OrderBy(x => x.Id).LastOrDefaultAsync(),
                async q => NormalizeGen(await q.OrderBy(x => x.Id).LastOrDefaultAsync())),
            ("ElementAt(1, ordered)",
                async q => await q.OrderBy(x => x.Id).ElementAtAsync(1),
                async q => NormalizeGen(await q.OrderBy(x => x.Id).ElementAtAsync(1))),
            ("Count",
                async q => await q.CountAsync(),
                async q => await q.CountAsync()),
            ("Sum(int)",
                async q => await q.SumAsync(x => x.IntValue),
                async q => await q.SumAsync(x => x.IntValue)),
        };

        foreach (var (name, plain, gen) in pairs)
        {
            var plainOutcome = await RunPlain(plain);
            var genOutcome = await RunGen(gen);
            AssertOutcomeParity($"sourcegen/{dataset}/{name}", plainOutcome, genOutcome);
        }

        static object? NormalizeGen(object? result)
            => result is PathParityRowGen r ? r.Id : result;
    }
}
