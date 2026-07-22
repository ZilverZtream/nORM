using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Property-based parity harness: generates deterministic pseudo-random query
/// shapes (predicates over comparisons, arithmetic, string operations, null
/// checks, and boolean combinators; ordered paging; aggregates; grouping) and
/// executes each against nORM-on-SQLite and LINQ-to-Objects over the same rows.
/// LINQ-to-Objects is the oracle: any result divergence fails with the seed and
/// expression that reproduce it. A clean NormUnsupportedFeatureException is a
/// valid outcome (the shape is declined); a wrong result never is. Seeds are
/// fixed so runs are reproducible in CI.
/// </summary>
[Trait("Category", "Fast")]
public class LinqParityFuzzTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("FuzzRow_Test")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public int? NullableInt { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Nick { get; set; }
        public decimal Amount { get; set; }
        public double Price { get; set; }
        public bool Flag { get; set; }
        public DateTime Created { get; set; }
    }

    // ── Dataset: fixed rows covering edge values (case variants, empties,
    //    negatives, nulls, duplicate keys for grouping/ordering ties). ─────────
    private static readonly Row[] Rows = BuildRows();

    private static Row[] BuildRows()
    {
        var names = new[] { "alpha", "ALPHA", "Alpha", "beta", "", "gamma", "Gamma", "delta", "α-unicode", "beta" };
        // Nulls interleaved with case variants and the empty string: null vs ""
        // is exactly the distinction SQL null semantics get wrong.
        var nicks = new[] { null, "alpha", "", "ALPHA", "b", null, "beta", "Gamma" };
        var rows = new List<Row>();
        for (var i = 0; i < 40; i++)
        {
            rows.Add(new Row
            {
                Id = i + 1,
                IntVal = (i % 7) - 3,                    // -3..3 with duplicates
                NullableInt = i % 4 == 0 ? null : (i % 5) - 2,
                Name = names[i % names.Length],
                Nick = nicks[i % nicks.Length],
                Amount = ((i * 37) % 11 - 5) + (i % 3) * 0.25m,
                Price = ((i * 13) % 9 - 4) + (i % 2) * 0.5,
                Flag = i % 3 == 0,
                Created = new DateTime(2026, 1, 1).AddDays(i % 14).AddSeconds(i * 977 % 86_400),
            });
        }
        return rows.ToArray();
    }

    // ── Predicate generator ──────────────────────────────────────────────────
    private static Expression<Func<Row, bool>> GeneratePredicate(Random rng)
    {
        var p = Expression.Parameter(typeof(Row), "r");
        var body = GenerateBool(rng, p, depth: 0);
        return Expression.Lambda<Func<Row, bool>>(body, p);
    }

    private static Expression GenerateBool(Random rng, ParameterExpression p, int depth)
    {
        if (depth < 3 && rng.Next(100) < 45)
        {
            var left = GenerateBool(rng, p, depth + 1);
            var right = GenerateBool(rng, p, depth + 1);
            return rng.Next(3) switch
            {
                0 => Expression.AndAlso(left, right),
                1 => Expression.OrElse(left, right),
                _ => Expression.Not(left),
            };
        }

        return rng.Next(8) switch
        {
            0 => IntComparison(rng, p),
            1 => NullableIntComparison(rng, p),
            2 => StringLeaf(rng, p),
            3 => rng.Next(2) == 0
                    ? Expression.Property(p, nameof(Row.Flag))
                    : Expression.Not(Expression.Property(p, nameof(Row.Flag))),
            4 => DecimalComparison(rng, p),
            5 => DateComparison(rng, p),
            6 => NullableStringLeaf(rng, p),
            _ => ArithmeticComparison(rng, p),
        };
    }

    private static readonly ExpressionType[] CompareOps =
    {
        ExpressionType.Equal, ExpressionType.NotEqual,
        ExpressionType.LessThan, ExpressionType.LessThanOrEqual,
        ExpressionType.GreaterThan, ExpressionType.GreaterThanOrEqual,
    };

    private static Expression IntComparison(Random rng, ParameterExpression p)
        => Expression.MakeBinary(
            CompareOps[rng.Next(CompareOps.Length)],
            Expression.Property(p, nameof(Row.IntVal)),
            Expression.Constant(rng.Next(-4, 5)));

    private static Expression NullableIntComparison(Random rng, ParameterExpression p)
    {
        var member = Expression.Property(p, nameof(Row.NullableInt));
        if (rng.Next(4) == 0)
        {
            var isNull = Expression.Equal(member, Expression.Constant(null, typeof(int?)));
            return rng.Next(2) == 0 ? isNull : Expression.Not(isNull);
        }
        return Expression.MakeBinary(
            CompareOps[rng.Next(CompareOps.Length)],
            member,
            Expression.Constant((int?)rng.Next(-3, 4), typeof(int?)));
    }

    private static readonly string[] StringPool = { "alpha", "ALPHA", "bet", "a", "", "Gamma", "zeta" };

    private static Expression StringLeaf(Random rng, ParameterExpression p)
    {
        var member = Expression.Property(p, nameof(Row.Name));
        var s = Expression.Constant(StringPool[rng.Next(StringPool.Length)]);
        return rng.Next(5) switch
        {
            0 => Expression.Call(member, nameof(string.Contains), Type.EmptyTypes, s),
            1 => Expression.Call(member, nameof(string.StartsWith), Type.EmptyTypes, s),
            2 => Expression.Call(member, nameof(string.EndsWith), Type.EmptyTypes, s),
            3 => Expression.MakeBinary(
                    rng.Next(2) == 0 ? ExpressionType.Equal : ExpressionType.NotEqual,
                    member, s),
            _ => Expression.MakeBinary(
                    CompareOps[rng.Next(CompareOps.Length)],
                    Expression.Property(member, nameof(string.Length)),
                    Expression.Constant(rng.Next(0, 8))),
        };
    }

    /// <summary>
    /// Nullable string predicates under C# semantics — the oracle runs the
    /// identical tree, so every generated form must be NRE-free: equality and
    /// its negation (null must satisfy <c>!=</c>), null tests, null-guarded
    /// method calls, coalesced Length, and cross-column comparison.
    /// </summary>
    private static Expression NullableStringLeaf(Random rng, ParameterExpression p)
    {
        var member = Expression.Property(p, "Nick");
        var s = Expression.Constant(StringPool[rng.Next(StringPool.Length)]);
        switch (rng.Next(6))
        {
            case 0: // null test, plain and negated
            {
                var isNull = Expression.Equal(member, Expression.Constant(null, typeof(string)));
                return rng.Next(2) == 0 ? isNull : Expression.Not(isNull);
            }
            case 1: // equality against a constant: C# says null != "x" is TRUE
                return Expression.MakeBinary(
                    rng.Next(2) == 0 ? ExpressionType.Equal : ExpressionType.NotEqual, member, s);
            case 2: // null-guarded method call
            {
                var guard = Expression.NotEqual(member, Expression.Constant(null, typeof(string)));
                Expression call = rng.Next(3) switch
                {
                    0 => Expression.Call(member, nameof(string.Contains), Type.EmptyTypes, s),
                    1 => Expression.Call(member, nameof(string.StartsWith), Type.EmptyTypes, s),
                    _ => Expression.Call(member, nameof(string.EndsWith), Type.EmptyTypes, s),
                };
                return Expression.AndAlso(guard, call);
            }
            case 3: // coalesced Length comparison
                return Expression.MakeBinary(
                    CompareOps[rng.Next(CompareOps.Length)],
                    Expression.Property(Expression.Coalesce(member, Expression.Constant(string.Empty)), nameof(string.Length)),
                    Expression.Constant(rng.Next(0, 6)));
            case 4: // cross-column: null != non-null Name is TRUE in C#
                return Expression.MakeBinary(
                    rng.Next(2) == 0 ? ExpressionType.Equal : ExpressionType.NotEqual,
                    member, Expression.Property(p, nameof(Row.Name)));
            default: // negated equality (De Morgan pushdown + null semantics)
                return Expression.Not(Expression.MakeBinary(
                    rng.Next(2) == 0 ? ExpressionType.Equal : ExpressionType.NotEqual, member, s));
        }
    }

    private static Expression DecimalComparison(Random rng, ParameterExpression p)
        => Expression.MakeBinary(
            CompareOps[rng.Next(CompareOps.Length)],
            Expression.Property(p, nameof(Row.Amount)),
            Expression.Constant((decimal)rng.Next(-5, 6) + (rng.Next(4) * 0.25m)));

    private static Expression DateComparison(Random rng, ParameterExpression p)
        => Expression.MakeBinary(
            CompareOps[rng.Next(CompareOps.Length)],
            Expression.Property(p, nameof(Row.Created)),
            Expression.Constant(new DateTime(2026, 1, 1).AddDays(rng.Next(0, 14)).AddSeconds(rng.Next(0, 86_400))));

    private static Expression ArithmeticComparison(Random rng, ParameterExpression p)
    {
        var member = Expression.Property(p, nameof(Row.IntVal));
        var k = rng.Next(1, 4);
        Expression arith = rng.Next(4) switch
        {
            0 => Expression.Add(member, Expression.Constant(k)),
            1 => Expression.Subtract(member, Expression.Constant(k)),
            2 => Expression.Multiply(member, Expression.Constant(k)),
            _ => Expression.Modulo(member, Expression.Constant(k + 1)),
        };
        return Expression.MakeBinary(
            CompareOps[rng.Next(CompareOps.Length)],
            arith,
            Expression.Constant(rng.Next(-4, 5)));
    }

    // ── Order keys ───────────────────────────────────────────────────────────
    // Name is deliberately absent: string ORDER BY follows the provider
    // collation while LINQ-to-Objects compares by the current culture — a
    // documented, intentionally unresolved divergence.
    private static readonly (string Prop, Type Type)[] OrderKeys =
    {
        (nameof(Row.IntVal), typeof(int)),
        (nameof(Row.Amount), typeof(decimal)),
        (nameof(Row.Created), typeof(DateTime)),
    };

    private static IQueryable<Row> ApplyOrderedPaging(IQueryable<Row> q, Random rng, out bool ordered)
    {
        ordered = rng.Next(2) == 0;
        if (!ordered) return q;

        var (prop, type) = OrderKeys[rng.Next(OrderKeys.Length)];
        var p = Expression.Parameter(typeof(Row), "r");
        var key = Expression.Lambda(Expression.Property(p, prop), p);
        var desc = rng.Next(2) == 0;

        var ordering = (IOrderedQueryable<Row>)q.Provider.CreateQuery<Row>(Expression.Call(
            typeof(Queryable), desc ? nameof(Queryable.OrderByDescending) : nameof(Queryable.OrderBy),
            new[] { typeof(Row), type }, q.Expression, Expression.Quote(key)));

        // Deterministic tiebreak so both systems agree on duplicate keys.
        var tie = ordering.ThenBy(r => r.Id);

        IQueryable<Row> paged = tie;
        if (rng.Next(2) == 0) paged = paged.Skip(rng.Next(0, 10));
        if (rng.Next(2) == 0) paged = paged.Take(rng.Next(1, 15));
        return paged;
    }

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_LINQ_FUZZ_SWEEP to "start:count" (optionally "start:count:dop") to run that
    /// seed range through the full shape battery. Unset, this fact is a no-op so the fixed
    /// seeds stay the baseline.
    ///
    /// Each seed builds its own in-memory database + <see cref="DbContext"/> and shares no
    /// mutable state with any other seed (the oracle lists are read-only), so the sweep is
    /// embarrassingly parallel. It fans out across all cores via
    /// <see cref="System.Threading.Tasks.Parallel.ForEachAsync{TSource}(System.Collections.Generic.IEnumerable{TSource}, System.Threading.Tasks.ParallelOptions, System.Func{TSource, System.Threading.CancellationToken, System.Threading.Tasks.ValueTask})"/>
    /// instead of running one seed at a time — on a 24-core box that is ~20x the throughput
    /// of the old sequential loop. A failing seed still surfaces: the assertion exception
    /// propagates out of the parallel loop. Override the degree of parallelism with the
    /// optional third field (default = <see cref="Environment.ProcessorCount"/>).
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_LINQ_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        var dop = parts.Length > 2
            ? int.Parse(parts[2], System.Globalization.CultureInfo.InvariantCulture)
            : Environment.ProcessorCount;

        var options = new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, dop) };
        await System.Threading.Tasks.Parallel.ForEachAsync(
            Enumerable.Range(start, count), options,
            async (s, _) => await Generated_query_shapes_match_linq_to_objects(s));
    }

    // ── The fuzz run ─────────────────────────────────────────────────────────
    [Theory]
    [InlineData(20260713)]
    [InlineData(42)]
    [InlineData(987654321)]
    [InlineData(1)]
    [InlineData(777_000_111)]
    [InlineData(31337)]
    [InlineData(500_009)]
    [InlineData(500_150)]
    public async System.Threading.Tasks.Task Generated_query_shapes_match_linq_to_objects(int seed)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FuzzRow_Test (
                    Id INTEGER PRIMARY KEY,
                    IntVal INTEGER NOT NULL,
                    NullableInt INTEGER NULL,
                    Name TEXT NOT NULL,
                    Nick TEXT NULL,
                    Amount TEXT NOT NULL,
                    Price REAL NOT NULL,
                    Flag INTEGER NOT NULL,
                    Created TEXT NOT NULL);
                CREATE TABLE FuzzChild_Test (
                    Id INTEGER PRIMARY KEY,
                    ParentId INTEGER NOT NULL,
                    ChildVal INTEGER NOT NULL,
                    Tag TEXT NOT NULL);
                CREATE TABLE FuzzGrand_Test (
                    Id INTEGER PRIMARY KEY,
                    ChildId INTEGER NOT NULL,
                    GVal INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), CreateFuzzOptions());
        await SeedAsync(ctx);
        await SeedChildrenAsync(ctx);
        await SeedGrandsAsync(ctx);
        RunFuzz(ctx, seed, cases: 400);
        RunJoinFuzz(ctx, seed, cases: 150);
        RunSelectManyFuzz(ctx, seed, cases: 120);
        RunNavFlattenFuzz(ctx, seed, cases: 120);
        RunSetOpFuzz(ctx, seed, cases: 120);
        RunKeyedOpFuzz(ctx, seed, cases: 100);
        RunWindowFuzz(ctx, seed, cases: 100);
        RunStringComparisonClosureFuzz(ctx, seed, cases: 80);
        RunGroupedFirstAndCorrelatedAggregateFuzz(ctx, seed, cases: 120);
        RunProjectionClosureFuzz(ctx, seed, cases: 100);
        RunIncludeFuzz(ctx, seed, cases: 60);
        RunThenIncludeFuzz(ctx, seed, cases: 40);
        await RunCompiledQueryFuzzAsync(ctx, seed, cases: 80);
    }

    /// <summary>
    /// Three-level eager-load graph fuzz: filters and windows over the parent with
    /// Include(Kids).ThenInclude(Grands), verifying every level exactly — parents,
    /// each parent's children, and each child's grandchildren. A second-hop
    /// correlation keyed off the wrong level loads another child's grandchildren.
    /// </summary>
    internal static void RunThenIncludeFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        for (var i = 0; i < cases; i++)
        {
            var k = rng.Next(-4, 4);
            var n = rng.Next(1, 8);
            var windowed = rng.Next(2) == 0;

            List<NavRow> actual;
            List<Row> expectedParents;
            try
            {
                if (windowed)
                {
                    actual = ((System.Linq.IQueryable<NavRow>)((INormQueryable<NavRow>)ctx.Query<NavRow>()
                            .OrderByDescending(r => r.IntVal).ThenBy(r => r.Id).Take(n))
                        .Include(r => r.Kids).ThenInclude(c => c.Grands).AsNoTracking()).ToList();
                    expectedParents = Rows.OrderByDescending(r => r.IntVal).ThenBy(r => r.Id).Take(n).ToList();
                }
                else
                {
                    actual = ((System.Linq.IQueryable<NavRow>)((INormQueryable<NavRow>)ctx.Query<NavRow>()
                            .Where(r => r.IntVal >= k))
                        .Include(r => r.Kids).ThenInclude(c => c.Grands).AsNoTracking()).ToList()
                        .OrderBy(r => r.Id).ToList();
                    expectedParents = Rows.Where(r => r.IntVal >= k).OrderBy(r => r.Id).ToList();
                }
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"then-include shape threw (seed={seed} case={i} windowed={windowed} k={k} n={n})", ex);
            }

            Assert.True(expectedParents.Select(r => r.Id).SequenceEqual(actual.Select(r => r.Id)),
                $"then-include parent mismatch seed={seed} case={i} windowed={windowed} k={k} n={n}\n" +
                $"expected [{string.Join(",", expectedParents.Select(r => r.Id))}] got [{string.Join(",", actual.Select(r => r.Id))}]");

            foreach (var parent in actual)
            {
                var expectedKids = Children.Where(c => c.ParentId == parent.Id).Select(c => c.Id).OrderBy(x => x).ToList();
                var actualKids = parent.Kids.OrderBy(c => c.Id).ToList();
                Assert.True(expectedKids.SequenceEqual(actualKids.Select(c => c.Id)),
                    $"then-include kids mismatch seed={seed} case={i} parent={parent.Id}\n" +
                    $"expected [{string.Join(",", expectedKids)}] got [{string.Join(",", actualKids.Select(c => c.Id))}]");

                foreach (var kid in actualKids)
                {
                    var expectedGrands = Grands.Where(g => g.ChildId == kid.Id).OrderBy(g => g.Id).ToList();
                    var actualGrands = kid.Grands.OrderBy(g => g.Id).ToList();
                    Assert.True(expectedGrands.Count == actualGrands.Count
                            && expectedGrands.Zip(actualGrands).All(p => p.First.Id == p.Second.Id && p.First.GVal == p.Second.GVal),
                        $"then-include grands mismatch seed={seed} case={i} parent={parent.Id} kid={kid.Id}\n" +
                        $"expected [{string.Join(",", expectedGrands.Select(g => g.Id))}] got [{string.Join(",", actualGrands.Select(g => g.Id))}]");
                }
            }
        }
    }

    // Compiled queries pre-translate ONCE into a permanently cached plan and bind
    // their free parameter per invocation — the delegates are static so every
    // fuzz case exercises the same cached plan with a different argument. A plan
    // that baked the first invocation's value (or bound the parameter to the
    // wrong slot) diverges immediately.
    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<List<Row>>> _cqThreshold =
        Norm.CompileQuery((DbContext c, int k) => c.Query<Row>().Where(r => r.IntVal >= k));

    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<List<Row>>> _cqBand =
        Norm.CompileQuery((DbContext c, int k) => c.Query<Row>().Where(r => r.IntVal >= k && r.IntVal <= k + 2));

    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<List<Row>>> _cqCorrelated =
        Norm.CompileQuery((DbContext c, int m) => c.Query<Row>()
            .Where(r => c.Query<Child>().Count(ch => ch.ParentId == r.Id && ch.ChildVal >= m - 2) >= 1 || r.IntVal > m));

    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<List<Row>>> _cqWindow =
        Norm.CompileQuery((DbContext c, int n) => c.Query<Row>().OrderBy(r => r.IntVal).ThenBy(r => r.Id).Skip(1).Take(n));

    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<List<Row>>> _cqNullable =
        Norm.CompileQuery((DbContext c, int k) => c.Query<Row>().Where(r => r.NullableInt != null && r.NullableInt >= k));

    internal static async System.Threading.Tasks.Task RunCompiledQueryFuzzAsync(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        for (var i = 0; i < cases; i++)
        {
            var arg = rng.Next(-4, 6);
            var shape = rng.Next(5);
            List<int> expected;
            List<int> actual;
            try
            {
                (expected, actual) = shape switch
                {
                    0 => (Rows.Where(r => r.IntVal >= arg).Select(r => r.Id).OrderBy(x => x).ToList(),
                          (await _cqThreshold(ctx, arg)).Select(r => r.Id).OrderBy(x => x).ToList()),
                    1 => (Rows.Where(r => r.IntVal >= arg && r.IntVal <= arg + 2).Select(r => r.Id).OrderBy(x => x).ToList(),
                          (await _cqBand(ctx, arg)).Select(r => r.Id).OrderBy(x => x).ToList()),
                    2 => (Rows.Where(r => Children.Count(ch => ch.ParentId == r.Id && ch.ChildVal >= arg - 2) >= 1 || r.IntVal > arg)
                              .Select(r => r.Id).OrderBy(x => x).ToList(),
                          (await _cqCorrelated(ctx, arg)).Select(r => r.Id).OrderBy(x => x).ToList()),
                    3 => (Rows.OrderBy(r => r.IntVal).ThenBy(r => r.Id).Skip(1).Take(Math.Max(0, arg)).Select(r => r.Id).ToList(),
                          (await _cqWindow(ctx, arg)).Select(r => r.Id).ToList()),
                    _ => (Rows.Where(r => r.NullableInt != null && r.NullableInt >= arg).Select(r => r.Id).OrderBy(x => x).ToList(),
                          (await _cqNullable(ctx, arg)).Select(r => r.Id).OrderBy(x => x).ToList()),
                };
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"compiled-query shape threw (seed={seed} case={i} shape={shape} arg={arg})", ex);
            }

            Assert.True(expected.SequenceEqual(actual),
                $"compiled-query mismatch seed={seed} case={i} shape={shape} arg={arg}\n" +
                $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    /// <summary>
    /// Eager-load graph fuzz: seeded filters and windows over the navigation-mapped
    /// parent with Include(Kids), verified against the in-memory model — the loaded
    /// parent set AND every parent's child collection must match exactly (a wrong
    /// dependent-query correlation loads another parent's children; a window that
    /// leaks into the dependent query drops children silently). AsNoTracking keeps
    /// instances per-case so tracked-graph fixup cannot carry state across cases.
    /// </summary>
    internal static void RunIncludeFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        for (var i = 0; i < cases; i++)
        {
            var k = rng.Next(-4, 4);
            var s = rng.Next(0, 6);
            var n = rng.Next(1, 8);
            var shape = rng.Next(4);
            // Split-query mode runs the dependent fetch through a different
            // execution strategy; both must produce the identical graph.
            var split = rng.Next(2) == 0;

            List<NavRow> Load(INormQueryable<NavRow> source)
            {
                var included = source.Include(r => r.Kids);
                var query = split
                    ? included.AsSplitQuery().AsNoTracking()
                    : included.AsNoTracking();
                return ((System.Linq.IQueryable<NavRow>)query).ToList();
            }

            List<NavRow> actual;
            List<Row> expectedParents;
            try
            {
                switch (shape)
                {
                    case 0:
                        actual = Load((INormQueryable<NavRow>)ctx.Query<NavRow>().Where(r => r.IntVal >= k))
                            .OrderBy(r => r.Id).ToList();
                        expectedParents = Rows.Where(r => r.IntVal >= k).OrderBy(r => r.Id).ToList();
                        break;
                    case 1:
                        actual = Load((INormQueryable<NavRow>)ctx.Query<NavRow>().Where(r => r.Flag || r.IntVal < k))
                            .OrderBy(r => r.Id).ToList();
                        expectedParents = Rows.Where(r => r.Flag || r.IntVal < k).OrderBy(r => r.Id).ToList();
                        break;
                    case 2:
                        actual = Load((INormQueryable<NavRow>)ctx.Query<NavRow>()
                            .OrderBy(r => r.IntVal).ThenBy(r => r.Id).Skip(s).Take(n));
                        expectedParents = Rows.OrderBy(r => r.IntVal).ThenBy(r => r.Id).Skip(s).Take(n).ToList();
                        break;
                    default:
                        actual = Load((INormQueryable<NavRow>)ctx.Query<NavRow>()
                            .Where(r => r.IntVal >= k).OrderByDescending(r => r.Created).ThenBy(r => r.Id).Take(n));
                        expectedParents = Rows.Where(r => r.IntVal >= k).OrderByDescending(r => r.Created).ThenBy(r => r.Id).Take(n).ToList();
                        break;
                }
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"include shape threw (seed={seed} case={i} shape={shape} split={split} k={k} s={s} n={n})", ex);
            }

            Assert.True(expectedParents.Select(r => r.Id).SequenceEqual(actual.Select(r => r.Id)),
                $"include parent mismatch seed={seed} case={i} shape={shape} k={k} s={s} n={n}\n" +
                $"expected [{string.Join(",", expectedParents.Select(r => r.Id))}] got [{string.Join(",", actual.Select(r => r.Id))}]");

            foreach (var parent in actual)
            {
                var expectedKids = Children.Where(c => c.ParentId == parent.Id).OrderBy(c => c.Id).ToList();
                var actualKids = parent.Kids.OrderBy(c => c.Id).ToList();
                Assert.True(expectedKids.Count == actualKids.Count
                        && expectedKids.Zip(actualKids).All(p => p.First.Id == p.Second.Id
                            && p.First.ChildVal == p.Second.ChildVal && p.First.Tag == p.Second.Tag),
                    $"include kids mismatch seed={seed} case={i} shape={shape} parent={parent.Id}\n" +
                    $"expected [{string.Join(",", expectedKids.Select(c => c.Id))}] got [{string.Join(",", actualKids.Select(c => c.Id))}]");
            }
        }
    }

    /// <summary>
    /// Fuzzes the greatest-per-group projection chain (ordered First/Last/ElementAt
    /// with ThenBy tiebreaks, group-local Where filters, terminal predicates, source
    /// Where re-application) and correlated scalar Queryable aggregates in predicates
    /// (Count/Sum/Min/Max/Average over explicit subqueries). Every constant is a
    /// CLOSURE re-randomized per case so fingerprint-cached plans must re-bind — a
    /// baked value replays the first case's filter and silently returns wrong rows.
    /// Cases whose terminal would throw in LINQ (empty filtered group, out-of-range
    /// index) are skipped: SQL yields NULL there, a documented divergence. Aggregate
    /// oracles guard with Any() to mirror SQL's NULL propagation on empty inputs
    /// (Count alone is 0-valued, not NULL).
    /// </summary>
    internal static void RunGroupedFirstAndCorrelatedAggregateFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        for (var i = 0; i < cases; i++)
        {
            if (rng.Next(2) == 0)
                RunGroupedFirstCase(ctx, rng, seed, i);
            else
                RunCorrelatedAggregateCase(ctx, rng, seed, i);
        }
    }

    // Projection closures register their compiled slots at plan-Build time — after
    // every clause-translated slot — while values extract in document order. These
    // shapes combine closure-bearing projections with trailing operators that mint
    // their own slots (computed order keys, set-op arms, quantifier predicates,
    // paging) so any registration/extraction divergence surfaces as a parity break.
    internal static void RunProjectionClosureFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        for (var i = 0; i < cases; i++)
        {
            var add = rng.Next(-50, 200);
            var mod = rng.Next(2, 12);
            var cut = rng.Next(-3, 4);
            var needle = rng.Next(-30, 240);
            var take = rng.Next(1, 8);
            var shape = rng.Next(6);

            List<int> expected;
            List<int> actual;
            try
            {
                switch (shape)
                {
                    case 0:
                        // Closure ORDER BY key over a projected computed member — the
                        // key expansion renders the projection fragment at clause time.
                        expected = Rows.Select(r => new { r.Id, V = r.IntVal + add })
                            .OrderBy(x => x.V % mod).ThenBy(x => x.Id).Select(x => x.V).ToList();
                        actual = ctx.Query<Row>().Select(r => new { r.Id, V = r.IntVal + add })
                            .OrderBy(x => x.V % mod).ThenBy(x => x.Id)
                            .AsEnumerable().Select(x => x.V).ToList();
                        break;
                    case 1:
                        expected = Rows.Where(r => r.IntVal >= cut).Select(r => new { r.Id, V = r.IntVal * 2 + add })
                            .OrderByDescending(x => x.V % mod).ThenBy(x => x.Id).Select(x => x.V).ToList();
                        actual = ctx.Query<Row>().Where(r => r.IntVal >= cut).Select(r => new { r.Id, V = r.IntVal * 2 + add })
                            .OrderByDescending(x => x.V % mod).ThenBy(x => x.Id)
                            .AsEnumerable().Select(x => x.V).ToList();
                        break;
                    case 2:
                        // Trailing quantifier with a needle closure after a projection closure.
                        var expectedHit = Rows.Select(r => r.IntVal + add).Any(x => x == needle);
                        var actualHit = ctx.Query<Row>().Select(r => r.IntVal + add).Any(x => x == needle);
                        Assert.True(expectedHit == actualHit,
                            $"projection-closure quantifier mismatch seed={seed} case={i} add={add} needle={needle}: expected {expectedHit} got {actualHit}");
                        continue;
                    case 3:
                        expected = Rows.Select(r => r.IntVal + add)
                            .Concat(Rows.Where(r => r.IntVal >= cut).Select(r => r.IntVal + add))
                            .OrderBy(x => x).ToList();
                        actual = ctx.Query<Row>().Select(r => r.IntVal + add)
                            .Concat(ctx.Query<Row>().Where(r => r.IntVal >= cut).Select(r => r.IntVal + add))
                            .AsEnumerable().OrderBy(x => x).ToList();
                        break;
                    case 4:
                        var expectedAny = Rows.Select(r => r.IntVal % mod).Distinct().Any(x => x >= cut);
                        var actualAny = ctx.Query<Row>().Select(r => r.IntVal % mod).Distinct().Any(x => x >= cut);
                        Assert.True(expectedAny == actualAny,
                            $"projection-closure distinct-any mismatch seed={seed} case={i} mod={mod} cut={cut}: expected {expectedAny} got {actualAny}");
                        continue;
                    default:
                        expected = Rows.OrderBy(r => r.Id).Select(r => (r.IntVal + add) % mod).Take(take).ToList();
                        actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (r.IntVal + add) % mod).Take(take)
                            .AsEnumerable().ToList();
                        break;
                }
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"projection-closure shape threw (seed={seed} case={i} shape={shape} add={add} mod={mod} cut={cut} needle={needle} take={take})", ex);
            }

            Assert.True(expected.SequenceEqual(actual),
                $"projection-closure mismatch seed={seed} case={i} shape={shape} add={add} mod={mod} cut={cut} needle={needle} take={take}\n" +
                $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    private static void RunGroupedFirstCase(DbContext ctx, Random rng, int seed, int i)
    {
        var baseCut = rng.Next(-3, 2);
        var localCut = rng.Next(-3, 3);
        var termCut = rng.Next(0, 20);
        var idx = rng.Next(0, 2);
        var shape = rng.Next(5);

        var modelGroups = Rows.Where(r => r.IntVal >= baseCut).GroupBy(r => r.IntVal).ToList();
        if (modelGroups.Count == 0) return;
        switch (shape)
        {
            case 2 when modelGroups.Any(g => !g.Any(x => x.IntVal >= localCut)): return;
            case 3 when modelGroups.Any(g => g.Count() <= idx): return;
            case 4 when modelGroups.Any(g => !g.Any(x => x.Id > termCut)): return;
        }

        List<(int Key, int V)> expected;
        List<(int Key, int V)> actual;
        try
        {
            expected = shape switch
            {
                0 => modelGroups.Select(g => (g.Key, V: g.OrderByDescending(x => x.Price).ThenBy(x => x.Id).First().IntVal)).OrderBy(t => t.Key).ToList(),
                1 => modelGroups.Select(g => (g.Key, V: g.OrderBy(x => x.Amount).ThenBy(x => x.Id).Last().Id)).OrderBy(t => t.Key).ToList(),
                2 => modelGroups.Select(g => (g.Key, V: g.Where(x => x.IntVal >= localCut).OrderBy(x => x.Created).ThenBy(x => x.Id).First().Id)).OrderBy(t => t.Key).ToList(),
                3 => modelGroups.Select(g => (g.Key, V: g.OrderBy(x => x.Price).ThenBy(x => x.Id).ElementAt(idx).Id)).OrderBy(t => t.Key).ToList(),
                _ => modelGroups.Select(g => (g.Key, V: g.OrderByDescending(x => x.IntVal).ThenBy(x => x.Id).First(x => x.Id > termCut).Id)).OrderBy(t => t.Key).ToList(),
            };
            actual = shape switch
            {
                0 => ctx.Query<Row>().Where(r => r.IntVal >= baseCut).GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, V = g.OrderByDescending(x => x.Price).ThenBy(x => x.Id).First().IntVal })
                        .AsEnumerable().Select(x => (x.Key, x.V)).OrderBy(t => t.Key).ToList(),
                1 => ctx.Query<Row>().Where(r => r.IntVal >= baseCut).GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, V = g.OrderBy(x => x.Amount).ThenBy(x => x.Id).Last().Id })
                        .AsEnumerable().Select(x => (x.Key, x.V)).OrderBy(t => t.Key).ToList(),
                2 => ctx.Query<Row>().Where(r => r.IntVal >= baseCut).GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, V = g.Where(x => x.IntVal >= localCut).OrderBy(x => x.Created).ThenBy(x => x.Id).First().Id })
                        .AsEnumerable().Select(x => (x.Key, x.V)).OrderBy(t => t.Key).ToList(),
                3 => ctx.Query<Row>().Where(r => r.IntVal >= baseCut).GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, V = g.OrderBy(x => x.Price).ThenBy(x => x.Id).ElementAt(idx).Id })
                        .AsEnumerable().Select(x => (x.Key, x.V)).OrderBy(t => t.Key).ToList(),
                _ => ctx.Query<Row>().Where(r => r.IntVal >= baseCut).GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, V = g.OrderByDescending(x => x.IntVal).ThenBy(x => x.Id).First(x => x.Id > termCut).Id })
                        .AsEnumerable().Select(x => (x.Key, x.V)).OrderBy(t => t.Key).ToList(),
            };
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            throw new InvalidOperationException(
                $"grouped-first shape threw (seed={seed} case={i} shape={shape} baseCut={baseCut} localCut={localCut} termCut={termCut} idx={idx})", ex);
        }

        Assert.True(expected.SequenceEqual(actual),
            $"grouped-first mismatch seed={seed} case={i} shape={shape} baseCut={baseCut} localCut={localCut} termCut={termCut} idx={idx}\n" +
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    private static void RunCorrelatedAggregateCase(DbContext ctx, Random rng, int seed, int i)
    {
        var k = rng.Next(-4, 4);
        var m = rng.Next(0, 5);
        var sumCut = rng.Next(-10, 15);
        var avgCut = rng.Next(-3, 3) + 0.25;
        var shape = rng.Next(14);

        List<int> expected;
        List<int> actual;
        try
        {
            switch (shape)
            {
                case 0:
                    expected = Rows.Where(r => Children.Count(c => c.ParentId == r.Id && c.ChildVal >= k) >= m)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Count(c => c.ParentId == r.Id && c.ChildVal >= k) >= m)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 1:
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id)
                            && Children.Where(c => c.ParentId == r.Id).Sum(c => c.ChildVal + k) > sumCut)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id).Sum(c => c.ChildVal + k) > sumCut)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 2:
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id)
                            && Children.Where(c => c.ParentId == r.Id).Min(c => c.ChildVal) >= k)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id).Min(c => c.ChildVal) >= k)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 3:
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id && c.ChildVal >= k)
                            && Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k).Max(c => c.ChildVal) <= sumCut)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id && c.ChildVal >= k).Max(c => c.ChildVal) <= sumCut)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 4:
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id)
                            && Children.Where(c => c.ParentId == r.Id).Average(c => c.ChildVal) > avgCut)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id).Average(c => c.ChildVal) > avgCut)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 5:
                    // Two consumed subquery roots in one predicate: both ctx captures
                    // need their own alignment slots, in document order.
                    expected = Rows.Where(r =>
                            Children.Count(c => c.ParentId == r.Id && c.ChildVal >= k)
                            > Children.Count(c => c.ChildVal >= m))
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Count(c => c.ParentId == r.Id && c.ChildVal >= k)
                            > ctx.Query<Child>().Count(c => c.ChildVal >= m))
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 6:
                    // Same-type correlated subquery: the inner FROM alias must bind the
                    // INNER root, not the outer scope's same-mapped entry.
                    expected = Rows.Where(r => Rows.Count(r2 => r2.IntVal == r.IntVal && r2.Id != r.Id) >= m)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Row>().Count(r2 => r2.IntVal == r.IntVal && r2.Id != r.Id) >= m)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 7:
                    // Nested correlation ending on the OUTER entity's type again.
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id && c.ChildVal >= k
                            && Rows.Any(r2 => r2.Id == c.ParentId && r2.IntVal >= m - 2)))
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Any(c => c.ParentId == r.Id && c.ChildVal >= k
                            && ctx.Query<Row>().Any(r2 => r2.Id == c.ParentId && r2.IntVal >= m - 2)))
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 8:
                    // Correlated First over an ordered, scalar-projected subquery in a predicate.
                    // The oracle guards empty groups with Any (First-over-empty throws); nORM's
                    // empty subquery yields SQL NULL which the comparison excludes — same result.
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id && c.ChildVal >= k)
                            && Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                                .OrderBy(c => c.Id).Select(c => c.ChildVal).First() > sumCut)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => c.ChildVal).First() > sumCut)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 9:
                    // Correlated Last = First of the reversed ordering.
                    expected = Rows.Where(r => Children.Any(c => c.ParentId == r.Id && c.ChildVal >= k)
                            && Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                                .OrderBy(c => c.Id).Select(c => c.ChildVal).Last() > sumCut)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => c.ChildVal).Last() > sumCut)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                case 10:
                {
                    // Correlated ElementAt(idx): LIMIT 1 OFFSET idx. Oracle guards Count > idx
                    // (ElementAt-out-of-range throws); nORM's out-of-range OFFSET yields NULL.
                    var idx = m % 3;
                    expected = Rows.Where(r => Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k).Count() > idx
                            && Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                                .OrderBy(c => c.Id).Select(c => c.ChildVal).ElementAt(idx) > sumCut)
                        .Select(r => r.Id).OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Where(r => ctx.Query<Child>().Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => c.ChildVal).ElementAt(idx) > sumCut)
                        .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
                    break;
                }
                case 11:
                    // PROJECTION-side correlated First over a converter-free column, into a
                    // nullable member so an empty group surfaces SQL NULL (matched by the
                    // oracle's FirstOrDefault). Exercises the materializer's shadow-column path.
                    expected = Rows
                        .Select(r => r.Id * 1000 + (Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => (int?)c.ChildVal).FirstOrDefault() ?? -999))
                        .OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Select(r => new { r.Id, V = ctx.Query<Child>().Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => (int?)c.ChildVal).FirstOrDefault() })
                        .AsEnumerable().Select(x => x.Id * 1000 + (x.V ?? -999)).OrderBy(x => x).ToList();
                    break;
                case 12:
                    // PROJECTION-side correlated Last (reversed ordering) into a nullable member.
                    expected = Rows
                        .Select(r => r.Id * 1000 + (Children.Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => (int?)c.ChildVal).LastOrDefault() ?? -999))
                        .OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Select(r => new { r.Id, V = ctx.Query<Child>().Where(c => c.ParentId == r.Id && c.ChildVal >= k)
                            .OrderBy(c => c.Id).Select(c => (int?)c.ChildVal).LastOrDefault() })
                        .AsEnumerable().Select(x => x.Id * 1000 + (x.V ?? -999)).OrderBy(x => x).ToList();
                    break;
                default:
                    // PROJECTION-side correlated count: the ctx capture inside the
                    // projection reserves its own alignment slot and the inner
                    // closure re-binds across cached plans.
                    expected = Rows
                        .Select(r => r.Id * 1000 + Children.Count(c => c.ParentId == r.Id && c.ChildVal >= k))
                        .OrderBy(x => x).ToList();
                    actual = ctx.Query<Row>()
                        .Select(r => new { r.Id, N = ctx.Query<Child>().Count(c => c.ParentId == r.Id && c.ChildVal >= k) })
                        .AsEnumerable().Select(x => x.Id * 1000 + x.N).OrderBy(x => x).ToList();
                    break;
            }
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            throw new InvalidOperationException(
                $"correlated-aggregate shape threw (seed={seed} case={i} shape={shape} k={k} m={m} sumCut={sumCut} avgCut={avgCut})", ex);
        }

        Assert.True(expected.SequenceEqual(actual),
            $"correlated-aggregate mismatch seed={seed} case={i} shape={shape} k={k} m={m} sumCut={sumCut} avgCut={avgCut}\n" +
            $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    /// <summary>
    /// Runs string StartsWith/Contains/EndsWith/Equals shapes whose needle AND
    /// StringComparison are CLOSURE captures, alternating both across cases. The
    /// comparison selects between case-folded and binary SQL shapes, so these
    /// plans must translate fresh per execution — a fingerprint-cached plan (or
    /// pooled command) would replay the first case's needle and semantics.
    /// </summary>
    internal static void RunStringComparisonClosureFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var comparisons = new[] { StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase };
        var oracleRows = Rows.ToList();

        for (var i = 0; i < cases; i++)
        {
            var needle = ((char)('a' + rng.Next(6))).ToString();
            if (rng.Next(3) == 0) needle = needle.ToUpperInvariant();
            var cmp = comparisons[rng.Next(comparisons.Length)];
            var op = rng.Next(4);

            Expression<Func<Row, bool>> predicate = op switch
            {
                0 => r => r.Name.StartsWith(needle, cmp),
                1 => r => r.Name.Contains(needle, cmp),
                2 => r => r.Name.EndsWith(needle, cmp),
                _ => r => r.Nick != null && r.Nick.Equals(needle, cmp),
            };

            var expected = oracleRows.AsQueryable().Where(predicate)
                .Select(r => r.Id).OrderBy(x => x).ToList();
            List<int> actual;
            try
            {
                actual = ctx.Query<Row>().Where(predicate)
                    .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"comparison-closure shape threw (seed={seed} case={i} op={op} needle={needle} cmp={cmp})", ex);
            }

            Assert.True(expected.SequenceEqual(actual),
                $"comparison-closure mismatch seed={seed} case={i} op={op} needle={needle} cmp={cmp}\n" +
                $"expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    /// <summary>
    /// Model configuration for the navigation-mapped fuzz entities. NavRow /
    /// NavChild map onto the same fuzz tables as Row / Child but carry a
    /// collection navigation so the navigation-flatten translation arm gets
    /// fuzzed alongside the correlated and cross arms.
    /// </summary>
    internal static DbContextOptions CreateFuzzOptions() => new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<NavRow>().HasKey(r => r.Id);
            mb.Entity<NavChild>().HasKey(c => c.Id);
            mb.Entity<NavGrand>().HasKey(g => g.Id);
            mb.Entity<NavRow>().HasMany(r => r.Kids).WithOne()
                               .HasForeignKey(c => c.ParentId, r => r.Id);
            mb.Entity<NavChild>().HasMany(c => c.Grands).WithOne()
                                 .HasForeignKey(g => g.ChildId, c => c.Id);
        }
    };

    /// <summary>Inserts the shared dataset through the context under test.</summary>
    internal static async System.Threading.Tasks.Task SeedAsync(DbContext ctx)
    {
        foreach (var row in Rows) ctx.Add(row);
        await ctx.SaveChangesAsync();
    }

    // ── Join fuzzing ─────────────────────────────────────────────────────────

    [System.ComponentModel.DataAnnotations.Schema.Table("FuzzChild_Test")]
    private class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int ChildVal { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private static readonly Child[] Children = BuildChildren();

    private static Child[] BuildChildren()
    {
        // Parents 1..40; children cover none/one/many per parent, keys past the
        // parent range (unmatched), and duplicate tags for grouped shapes.
        var tags = new[] { "x", "y", "x", "z", "" };
        var children = new List<Child>();
        for (var i = 0; i < 70; i++)
        {
            children.Add(new Child
            {
                Id = i + 1,
                ParentId = (i * 7) % 50 + 1,          // some point past 40 → unmatched
                ChildVal = (i % 9) - 4,
                Tag = tags[i % tags.Length],
            });
        }
        return children.ToArray();
    }

    /// <summary>Inserts the join dataset through the context under test.</summary>
    internal static async System.Threading.Tasks.Task SeedChildrenAsync(DbContext ctx)
    {
        foreach (var child in Children) ctx.Add(child);
        await ctx.SaveChangesAsync();
    }

    private sealed record JoinRow(int PId, int CId, int V);

    // ── Navigation flatten fuzzing ───────────────────────────────────────────

    [System.ComponentModel.DataAnnotations.Schema.Table("FuzzRow_Test")]
    private class NavRow
    {
        public int Id { get; set; }
        public int IntVal { get; set; }
        public int? NullableInt { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Nick { get; set; }
        public decimal Amount { get; set; }
        public double Price { get; set; }
        public bool Flag { get; set; }
        public DateTime Created { get; set; }
        public List<NavChild> Kids { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("FuzzChild_Test")]
    private class NavChild
    {
        public int Id { get; set; }
        public int ParentId { get; set; }
        public int ChildVal { get; set; }
        public string Tag { get; set; } = string.Empty;
        public List<NavGrand> Grands { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("FuzzGrand_Test")]
    private class NavGrand
    {
        public int Id { get; set; }
        public int ChildId { get; set; }
        public int GVal { get; set; }
    }

    private static readonly NavGrand[] Grands = BuildGrands();

    private static NavGrand[] BuildGrands()
    {
        // Children 1..70; grandchildren cover none/one/many per child with keys
        // past the child range (unmatched).
        var grands = new List<NavGrand>();
        for (var i = 0; i < 90; i++)
        {
            grands.Add(new NavGrand
            {
                Id = i + 1,
                ChildId = (i * 11) % 80 + 1,
                GVal = (i % 13) - 6,
            });
        }
        return grands.ToArray();
    }

    /// <summary>Inserts the grandchild dataset through the context under test.</summary>
    internal static async System.Threading.Tasks.Task SeedGrandsAsync(DbContext ctx)
    {
        foreach (var grand in Grands) ctx.Add(grand);
        await ctx.SaveChangesAsync();
    }

    /// <summary>In-memory oracle copies with the navigation populated.</summary>
    private static NavRow[] BuildNavRows()
        => Rows.Select(r => new NavRow
        {
            Id = r.Id,
            IntVal = r.IntVal,
            NullableInt = r.NullableInt,
            Name = r.Name,
            Nick = r.Nick,
            Amount = r.Amount,
            Price = r.Price,
            Flag = r.Flag,
            Created = r.Created,
            Kids = Children.Where(c => c.ParentId == r.Id)
                           .Select(c => new NavChild { Id = c.Id, ParentId = c.ParentId, ChildVal = c.ChildVal, Tag = c.Tag })
                           .ToList(),
        }).ToArray();

    // The predicate generator addresses members by name, and NavRow mirrors
    // Row's scalar members, so the same generator fuzzes both entity shapes.
    private static Expression<Func<NavRow, bool>> GenerateNavPredicate(Random rng)
    {
        var p = Expression.Parameter(typeof(NavRow), "r");
        var body = GenerateBool(rng, p, depth: 0);
        return Expression.Lambda<Func<NavRow, bool>>(body, p);
    }

    /// <summary>
    /// Runs generated navigation-flatten shapes (plain and filtered
    /// SelectMany over a collection navigation, computed result selectors,
    /// DefaultIfEmpty left joins in projected and entity-result forms, with a
    /// coin-flip windowed outer) against the context and the LINQ-to-Objects
    /// oracle over nav-populated copies of the same rows.
    /// </summary>
    internal static void RunNavFlattenFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;
        var navRows = BuildNavRows();

        for (var i = 0; i < cases; i++)
        {
            var predicate = GenerateNavPredicate(rng);
            var kind = rng.Next(5);
            var caseRng = new Random(rng.Next());

            IQueryable<NavRow> parents = ctx.Query<NavRow>().Where(predicate);
            IQueryable<NavRow> oracleParents = navRows.AsQueryable().Where(predicate);

            var windowed = caseRng.Next(3) == 0;
            if (windowed)
            {
                var skip = caseRng.Next(0, 10);
                var take = caseRng.Next(1, 20);
                parents = parents.OrderBy(p => p.Id).Skip(skip).Take(take);
                oracleParents = oracleParents.OrderBy(p => p.Id).Skip(skip).Take(take);
            }

            try
            {
                switch (kind)
                {
                    case 0: // plain navigation flatten, entity result
                    {
                        var db = parents.SelectMany(p => p.Kids)
                            .ToList().Select(c => (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        var oracle = oracleParents.SelectMany(p => p.Kids)
                            .ToList().Select(c => (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"nav flatten mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 1: // filtered navigation flatten with a closure bound in the filter
                    {
                        var k = caseRng.Next(-3, 3);
                        var db = parents.SelectMany(p => p.Kids.Where(c => c.ChildVal > k))
                            .ToList().Select(c => (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        var oracle = oracleParents.SelectMany(p => p.Kids.Where(c => c.ChildVal > k))
                            .ToList().Select(c => (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"filtered nav flatten mismatch seed={seed} case={i} windowed={windowed} k={k}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 2: // result selector with a computed member
                    {
                        var db = parents.SelectMany(p => p.Kids, (p, c) => new { p.Id, CId = c.Id, V = p.IntVal + c.ChildVal })
                            .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.SelectMany(p => p.Kids, (p, c) => new { p.Id, CId = c.Id, V = p.IntVal + c.ChildVal })
                            .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"nav computed projection mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(6))}]\noracle: [{string.Join(" | ", oracle.Take(6))}]");
                        break;
                    }
                    case 3: // navigation DefaultIfEmpty left join, null-safe projection (optionally filtered)
                    {
                        var k = caseRng.Next(-3, 3);
                        if (caseRng.Next(2) == 0)
                        {
                            var db = parents.SelectMany(p => p.Kids.DefaultIfEmpty(),
                                    (p, c) => new { p.Id, CId = c == null ? (int?)null : (int?)c.Id, V = c == null ? -99 : c.ChildVal })
                                .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                            var oracle = oracleParents.SelectMany(p => p.Kids.DefaultIfEmpty(),
                                    (p, c) => new { p.Id, CId = c == null ? (int?)null : (int?)c.Id, V = c == null ? -99 : c.ChildVal })
                                .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                            Assert.True(db.SequenceEqual(oracle),
                                $"nav left join mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(6))}]\noracle: [{string.Join(" | ", oracle.Take(6))}]");
                        }
                        else
                        {
                            var db = parents.SelectMany(p => p.Kids.Where(c => c.ChildVal > k).DefaultIfEmpty(),
                                    (p, c) => new { p.Id, CId = c == null ? (int?)null : (int?)c.Id, V = c == null ? -99 : c.ChildVal })
                                .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                            var oracle = oracleParents.SelectMany(p => p.Kids.Where(c => c.ChildVal > k).DefaultIfEmpty(),
                                    (p, c) => new { p.Id, CId = c == null ? (int?)null : (int?)c.Id, V = c == null ? -99 : c.ChildVal })
                                .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                            Assert.True(db.SequenceEqual(oracle),
                                $"filtered nav left join mismatch seed={seed} case={i} windowed={windowed} k={k}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(6))}]\noracle: [{string.Join(" | ", oracle.Take(6))}]");
                        }
                        break;
                    }
                    default: // navigation DefaultIfEmpty left join, entity result (null elements)
                    {
                        var db = parents.SelectMany(p => p.Kids.DefaultIfEmpty())
                            .ToList().Select(c => c == null ? (0, 0, 0) : (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        var oracle = oracleParents.SelectMany(p => p.Kids.DefaultIfEmpty())
                            .ToList().Select(c => c == null ? (0, 0, 0) : (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"nav left join entity mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"nav flatten shape threw (seed={seed} case={i} kind={kind} windowed={windowed})\npredicate: {predicate}", ex);
            }
        }

        Assert.True(unsupported < cases / 5,
            $"{unsupported}/{cases} navigation flatten shapes were declined as unsupported (seed {seed}).");
    }

    /// <summary>
    /// Runs generated join shapes (inner join, GroupJoin aggregate) against the
    /// context and the LINQ-to-Objects oracle. Both query roots are swapped for
    /// their in-memory counterparts so the identical tree runs on the oracle.
    /// </summary>
    internal static void RunJoinFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;

        for (var i = 0; i < cases; i++)
        {
            var predicate = GeneratePredicate(rng);
            var joinKind = rng.Next(8);
            var caseRng = new Random(rng.Next());

            IQueryable<Row> parents = ctx.Query<Row>().Where(predicate);
            IQueryable<Child> children = ctx.Query<Child>();
            var oracleParents = Rows.AsQueryable().Where(predicate);
            var oracleChildren = Children.AsQueryable();

            try
            {
                switch (joinKind)
                {
                    case 0: // inner join on Id = ParentId with computed projection
                    {
                        var k = caseRng.Next(1, 4);
                        var db = parents.Join(children, p => p.Id, c => c.ParentId,
                                (p, c) => new JoinRow(p.Id, c.Id, p.IntVal + c.ChildVal * 1))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.Join(oracleChildren, p => p.Id, c => c.ParentId,
                                (p, c) => new JoinRow(p.Id, c.Id, p.IntVal + c.ChildVal * 1))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"join mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 1: // composite anonymous join key
                    {
                        var db = parents.Join(children,
                                p => new { A = p.Id % 5, B = p.Flag },
                                c => new { A = c.ParentId % 5, B = c.ChildVal > 0 },
                                (p, c) => new JoinRow(p.Id, c.Id, c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.Join(oracleChildren,
                                p => new { A = p.Id % 5, B = p.Flag },
                                c => new { A = c.ParentId % 5, B = c.ChildVal > 0 },
                                (p, c) => new JoinRow(p.Id, c.Id, c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"composite join mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 2: // GroupJoin with per-parent aggregate
                    {
                        var db = parents.GroupJoin(children, p => p.Id, c => c.ParentId,
                                (p, cs) => new { p.Id, N = cs.Count(), S = cs.Sum(c => (int?)c.ChildVal) ?? 0 })
                            .ToList().OrderBy(x => x.Id).ToList();
                        var oracle = oracleParents.GroupJoin(oracleChildren, p => p.Id, c => c.ParentId,
                                (p, cs) => new { p.Id, N = cs.Count(), S = cs.Sum(c => (int?)c.ChildVal) ?? 0 })
                            .ToList().OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"groupjoin mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    case 4: // closure captured in the inner-join keys — a cached plan
                            // must bind the CURRENT modulus, not replay the first one
                    {
                        var m = caseRng.Next(2, 7);
                        var db = parents.Join(children, p => p.Id % m, c => c.ParentId % m,
                                (p, c) => new JoinRow(p.Id, c.Id, c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.Join(oracleChildren, p => p.Id % m, c => c.ParentId % m,
                                (p, c) => new JoinRow(p.Id, c.Id, c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"closure join key mismatch seed={seed} case={i} m={m}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 5: // closure captured in GroupJoin keys
                    {
                        var m = caseRng.Next(2, 7);
                        var db = parents.GroupJoin(children, p => p.Id % m, c => c.ParentId % m,
                                (p, cs) => new { p.Id, N = cs.Count() })
                            .ToList().OrderBy(x => x.Id).ToList();
                        var oracle = oracleParents.GroupJoin(oracleChildren, p => p.Id % m, c => c.ParentId % m,
                                (p, cs) => new { p.Id, N = cs.Count() })
                            .ToList().OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"closure groupjoin key mismatch seed={seed} case={i} m={m}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    case 6: // DTO-projected outer with DUPLICATE join keys + closure in the
                            // result selector — one result per outer ROW (PK segmentation),
                            // and cached plans must rebind the current closure value
                    {
                        var m = caseRng.Next(2, 5);
                        var bonus = caseRng.Next(-10, 10);
                        var db = parents.Select(p => new { K = p.Id % m, p.IntVal })
                            .GroupJoin(children, o => o.K, c => c.ParentId % m,
                                (o, cs) => new { o.IntVal, N = cs.Count() + bonus })
                            .ToList().OrderBy(x => x.IntVal).ThenBy(x => x.N).ToList();
                        var oracle = oracleParents.Select(p => new { K = p.Id % m, p.IntVal })
                            .GroupJoin(oracleChildren, o => o.K, c => c.ParentId % m,
                                (o, cs) => new { o.IntVal, N = cs.Count() + bonus })
                            .ToList().OrderBy(x => x.IntVal).ThenBy(x => x.N).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"projected-outer groupjoin mismatch seed={seed} case={i} m={m} bonus={bonus}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    case 7: // scalar-projected outer with duplicate keys
                    {
                        var m = caseRng.Next(2, 5);
                        var db = parents.Select(p => p.IntVal % m)
                            .GroupJoin(children, v => v, c => c.ParentId % m,
                                (v, cs) => new { V = v, N = cs.Count() })
                            .ToList().OrderBy(x => x.V).ThenBy(x => x.N).ToList();
                        var oracle = oracleParents.Select(p => p.IntVal % m)
                            .GroupJoin(oracleChildren, v => v, c => c.ParentId % m,
                                (v, cs) => new { V = v, N = cs.Count() })
                            .ToList().OrderBy(x => x.V).ThenBy(x => x.N).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"scalar-outer groupjoin mismatch seed={seed} case={i} m={m}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    default: // GroupJoin with filtered count, nullable Max/Min (null on empty), Any
                    {
                        var k = caseRng.Next(-3, 3);
                        var db = parents.GroupJoin(children, p => p.Id, c => c.ParentId,
                                (p, cs) => new
                                {
                                    p.Id,
                                    FC = cs.Where(c => c.ChildVal > k).Count(),
                                    Mx = cs.Max(c => (int?)c.ChildVal),
                                    Mn = cs.Min(c => (int?)c.ChildVal),
                                    HasNeg = cs.Any(c => c.ChildVal < 0),
                                })
                            .ToList().OrderBy(x => x.Id).ToList();
                        var oracle = oracleParents.GroupJoin(oracleChildren, p => p.Id, c => c.ParentId,
                                (p, cs) => new
                                {
                                    p.Id,
                                    FC = cs.Where(c => c.ChildVal > k).Count(),
                                    Mx = cs.Max(c => (int?)c.ChildVal),
                                    Mn = cs.Min(c => (int?)c.ChildVal),
                                    HasNeg = cs.Any(c => c.ChildVal < 0),
                                })
                            .ToList().OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"groupjoin aggregates mismatch seed={seed} case={i} k={k}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(6))}]\noracle: [{string.Join(" | ", oracle.Take(6))}]");
                        break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"join shape threw (seed={seed} case={i} kind={joinKind})\npredicate: {predicate}", ex);
            }
        }

        Assert.True(unsupported < cases / 5,
            $"{unsupported}/{cases} join shapes were declined as unsupported (seed {seed}).");
    }

    /// <summary>
    /// Runs generated keyed-operator shapes (DistinctBy, UnionBy with a local
    /// second sequence, ExceptBy/IntersectBy with local key sequences, and
    /// MinBy/MaxBy) against the context and the LINQ-to-Objects oracle. Sources
    /// are ordered by Id so first-per-key representative selection is
    /// deterministic on both systems; MinBy/MaxBy compare the winning KEY (ties
    /// make the winning element unspecified in SQL).
    /// </summary>
    internal static void RunKeyedOpFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;

        for (var i = 0; i < cases; i++)
        {
            var predicate = GeneratePredicate(rng);
            var kind = rng.Next(5);
            var caseRng = new Random(rng.Next());

            IQueryable<Row> a = ctx.Query<Row>().Where(predicate).OrderBy(r => r.Id);
            IQueryable<Row> oa = Rows.AsQueryable().Where(predicate).OrderBy(r => r.Id);

            try
            {
                switch (kind)
                {
                    case 0: // DistinctBy int key — first element per key in source order
                    {
                        var db = a.DistinctBy(r => r.IntVal).ToList().Select(r => (r.Id, r.IntVal)).OrderBy(x => x).ToList();
                        var oracle = oa.DistinctBy(r => r.IntVal).ToList().Select(r => (r.Id, r.IntVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"DistinctBy int mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: [{string.Join(" ", db)}]\noracle: [{string.Join(" ", oracle)}]");
                        break;
                    }
                    case 1: // DistinctBy nullable string key (null key group + ordinal case variants)
                    {
                        var db = a.DistinctBy(r => r.Nick).ToList().Select(r => (r.Id, r.Nick)).OrderBy(x => x.Id).ToList();
                        var oracle = oa.DistinctBy(r => r.Nick).ToList().Select(r => (r.Id, r.Nick)).OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"DistinctBy string mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: [{string.Join(" ", db)}]\noracle: [{string.Join(" ", oracle)}]");
                        break;
                    }
                    case 2: // UnionBy with a local second sequence
                    {
                        var locals = Rows.Skip(caseRng.Next(0, 25)).Take(caseRng.Next(1, 12)).ToList();
                        var db = a.UnionBy(locals, r => r.IntVal).ToList().Select(r => (r.Id, r.IntVal)).OrderBy(x => x).ToList();
                        var oracle = oa.UnionBy(locals, r => r.IntVal).ToList().Select(r => (r.Id, r.IntVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"UnionBy mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: [{string.Join(" ", db)}]\noracle: [{string.Join(" ", oracle)}]");
                        break;
                    }
                    case 3: // ExceptBy / IntersectBy with local key sequences (int or case-variant string keys)
                    {
                        var except = caseRng.Next(2) == 0;
                        if (caseRng.Next(2) == 0)
                        {
                            var keys = new[] { caseRng.Next(-3, 4), caseRng.Next(-3, 4), caseRng.Next(-3, 4) };
                            var db = (except ? a.ExceptBy(keys, r => r.IntVal) : a.IntersectBy(keys, r => r.IntVal))
                                .ToList().Select(r => (r.Id, r.IntVal)).OrderBy(x => x).ToList();
                            var oracle = (except ? oa.ExceptBy(keys, r => r.IntVal) : oa.IntersectBy(keys, r => r.IntVal))
                                .ToList().Select(r => (r.Id, r.IntVal)).OrderBy(x => x).ToList();
                            Assert.True(db.SequenceEqual(oracle),
                                $"{(except ? "ExceptBy" : "IntersectBy")} int mismatch seed={seed} case={i} keys=[{string.Join(",", keys)}]\npredicate: {predicate}\ndb: [{string.Join(" ", db)}]\noracle: [{string.Join(" ", oracle)}]");
                        }
                        else
                        {
                            var pool = new[] { "alpha", "ALPHA", "beta", "b", "Gamma", "" };
                            var keys = new[] { pool[caseRng.Next(pool.Length)], pool[caseRng.Next(pool.Length)] };
                            var db = (except ? a.ExceptBy(keys, r => r.Nick) : a.IntersectBy(keys, r => r.Nick))
                                .ToList().Select(r => (r.Id, r.Nick)).OrderBy(x => x.Id).ToList();
                            var oracle = (except ? oa.ExceptBy(keys, r => r.Nick) : oa.IntersectBy(keys, r => r.Nick))
                                .ToList().Select(r => (r.Id, r.Nick)).OrderBy(x => x.Id).ToList();
                            Assert.True(db.SequenceEqual(oracle),
                                $"{(except ? "ExceptBy" : "IntersectBy")} string mismatch seed={seed} case={i} keys=[{string.Join(",", keys)}]\npredicate: {predicate}\ndb: [{string.Join(" ", db)}]\noracle: [{string.Join(" ", oracle)}]");
                        }
                        break;
                    }
                    default: // MinBy / MaxBy — compare the winning key
                    {
                        var max = caseRng.Next(2) == 0;
                        var dbRow = max ? a.MaxBy(r => r.Amount) : a.MinBy(r => r.Amount);
                        var orRow = max ? oa.MaxBy(r => r.Amount) : oa.MinBy(r => r.Amount);
                        Assert.True((dbRow == null) == (orRow == null),
                            $"{(max ? "MaxBy" : "MinBy")} null mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: {dbRow?.Id.ToString() ?? "null"} oracle: {orRow?.Id.ToString() ?? "null"}");
                        if (dbRow != null && orRow != null)
                        {
                            Assert.True(dbRow.Amount == orRow.Amount,
                                $"{(max ? "MaxBy" : "MinBy")} key mismatch seed={seed} case={i}\npredicate: {predicate}\ndb: {dbRow.Amount} oracle: {orRow.Amount}");
                        }
                        break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"keyed-op shape threw (seed={seed} case={i} kind={kind})\npredicate: {predicate}", ex);
            }
        }

        Assert.True(unsupported < cases / 4,
            $"{unsupported}/{cases} keyed-op shapes were declined as unsupported (seed {seed}).");
    }

    /// <summary>
    /// Runs generated window-function shapes (WithRowNumber over a fully
    /// tiebroken order, WithRank and WithDenseRank over tie-heavy keys) against
    /// a client-side computed oracle: row number is the ordered index, rank is
    /// 1 + count(smaller keys), dense rank is 1 + count(distinct smaller keys) —
    /// all deterministic per row even when the window order has ties.
    /// </summary>
    internal static void RunWindowFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;

        for (var i = 0; i < cases; i++)
        {
            var predicate = GeneratePredicate(rng);
            var kind = rng.Next(3);
            var caseRng = new Random(rng.Next());
            var byAmount = caseRng.Next(2) == 0;

            var filtered = Rows.Where(predicate.Compile()).ToList();

            try
            {
                switch (kind)
                {
                    case 0: // ROW_NUMBER over a fully tiebroken order (optionally Take-limited)
                    {
                        var take = caseRng.Next(2) == 0 ? caseRng.Next(1, 20) : int.MaxValue;
                        var q = ctx.Query<Row>().Where(predicate)
                            .OrderBy(r => r.IntVal).ThenBy(r => r.Id)
                            .WithRowNumber((r, n) => new { r.Id, N = n });
                        var db = (take == int.MaxValue ? q : q.Take(take))
                            .ToList().OrderBy(x => x.Id).ToList();
                        var oracle = filtered.OrderBy(r => r.IntVal).ThenBy(r => r.Id)
                            .Select((r, idx) => new { r.Id, N = idx + 1 })
                            .Take(take)
                            .ToList().OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"RowNumber mismatch seed={seed} case={i} take={take}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    case 1: // RANK over a tie-heavy key
                    {
                        var db = (byAmount
                                ? ctx.Query<Row>().Where(predicate).OrderBy(r => r.Amount).WithRank((r, n) => new { r.Id, N = n })
                                : ctx.Query<Row>().Where(predicate).OrderBy(r => r.IntVal).WithRank((r, n) => new { r.Id, N = n }))
                            .ToList().OrderBy(x => x.Id).ToList();
                        var oracle = filtered
                            .Select(r => new
                            {
                                r.Id,
                                N = 1 + (byAmount
                                    ? filtered.Count(o => o.Amount < r.Amount)
                                    : filtered.Count(o => o.IntVal < r.IntVal)),
                            })
                            .OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"Rank mismatch seed={seed} case={i} byAmount={byAmount}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    default: // DENSE_RANK over a tie-heavy key
                    {
                        var db = (byAmount
                                ? ctx.Query<Row>().Where(predicate).OrderBy(r => r.Amount).WithDenseRank((r, n) => new { r.Id, N = n })
                                : ctx.Query<Row>().Where(predicate).OrderBy(r => r.IntVal).WithDenseRank((r, n) => new { r.Id, N = n }))
                            .ToList().OrderBy(x => x.Id).ToList();
                        var oracle = filtered
                            .Select(r => new
                            {
                                r.Id,
                                N = 1 + (byAmount
                                    ? filtered.Where(o => o.Amount < r.Amount).Select(o => o.Amount).Distinct().Count()
                                    : filtered.Where(o => o.IntVal < r.IntVal).Select(o => o.IntVal).Distinct().Count()),
                            })
                            .OrderBy(x => x.Id).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"DenseRank mismatch seed={seed} case={i} byAmount={byAmount}\npredicate: {predicate}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"window shape threw (seed={seed} case={i} kind={kind} byAmount={byAmount})\npredicate: {predicate}", ex);
            }
        }

        Assert.True(unsupported < cases / 4,
            $"{unsupported}/{cases} window shapes were declined as unsupported (seed {seed}).");
    }

    private static IQueryable<T> ApplySetOp<T>(IQueryable<T> a, IQueryable<T> b, int op) => op switch
    {
        0 => a.Union(b),
        1 => a.Concat(b),
        2 => a.Intersect(b),
        _ => a.Except(b),
    };

    private static string SetOpName(int op) => op switch
    {
        0 => "Union", 1 => "Concat", 2 => "Intersect", _ => "Except",
    };

    /// <summary>
    /// Runs generated set-operation shapes (Union, Concat, Intersect, Except)
    /// over independently predicated arms of the same table, with int, nullable
    /// string, and anonymous-pair element shapes. C# set semantics are the
    /// oracle: NULL equals NULL, string equality is ordinal, Concat preserves
    /// duplicates. A coin flip windows the first arm with OrderBy/Take.
    /// </summary>
    internal static void RunSetOpFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;

        for (var i = 0; i < cases; i++)
        {
            var pred1 = GeneratePredicate(rng);
            var pred2 = GeneratePredicate(rng);
            var kind = rng.Next(4);
            var op = rng.Next(4);
            var caseRng = new Random(rng.Next());

            IQueryable<Row> a = ctx.Query<Row>().Where(pred1);
            IQueryable<Row> b = ctx.Query<Row>().Where(pred2);
            IQueryable<Row> oa = Rows.AsQueryable().Where(pred1);
            IQueryable<Row> ob = Rows.AsQueryable().Where(pred2);

            var windowed = caseRng.Next(3) == 0;
            if (windowed)
            {
                var take = caseRng.Next(1, 25);
                a = a.OrderBy(r => r.Id).Take(take);
                oa = oa.OrderBy(r => r.Id).Take(take);
            }

            try
            {
                switch (kind)
                {
                    case 0: // nullable string element
                    {
                        var db = ApplySetOp(a.Select(r => r.Nick), b.Select(r => r.Nick), op)
                            .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
                        var oracle = ApplySetOp(oa.Select(r => r.Nick), ob.Select(r => r.Nick), op)
                            .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"{SetOpName(op)} string mismatch seed={seed} case={i} windowed={windowed}\npred1: {pred1}\npred2: {pred2}\ndb: [{string.Join(",", db)}]\noracle: [{string.Join(",", oracle)}]");
                        break;
                    }
                    case 1: // int element
                    {
                        var db = ApplySetOp(a.Select(r => r.IntVal), b.Select(r => r.IntVal), op)
                            .ToList().OrderBy(x => x).ToList();
                        var oracle = ApplySetOp(oa.Select(r => r.IntVal), ob.Select(r => r.IntVal), op)
                            .ToList().OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"{SetOpName(op)} int mismatch seed={seed} case={i} windowed={windowed}\npred1: {pred1}\npred2: {pred2}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 2: // anonymous pair with a nullable string member
                    {
                        var db = ApplySetOp(
                                a.Select(r => new { r.Nick, r.IntVal }),
                                b.Select(r => new { r.Nick, r.IntVal }), op)
                            .ToList().OrderBy(x => x.Nick, StringComparer.Ordinal).ThenBy(x => x.IntVal).ToList();
                        var oracle = ApplySetOp(
                                oa.Select(r => new { r.Nick, r.IntVal }),
                                ob.Select(r => new { r.Nick, r.IntVal }), op)
                            .ToList().OrderBy(x => x.Nick, StringComparer.Ordinal).ThenBy(x => x.IntVal).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"{SetOpName(op)} anon mismatch seed={seed} case={i} windowed={windowed}\npred1: {pred1}\npred2: {pred2}\ndb: [{string.Join(" | ", db.Take(8))}]\noracle: [{string.Join(" | ", oracle.Take(8))}]");
                        break;
                    }
                    default: // decimal element (scale-insensitive dedup, exact text storage on SQLite)
                    {
                        var db = ApplySetOp(a.Select(r => r.Amount), b.Select(r => r.Amount), op)
                            .ToList().OrderBy(x => x).ToList();
                        var oracle = ApplySetOp(oa.Select(r => r.Amount), ob.Select(r => r.Amount), op)
                            .ToList().OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"{SetOpName(op)} decimal mismatch seed={seed} case={i} windowed={windowed}\npred1: {pred1}\npred2: {pred2}\ndb: [{string.Join(",", db.Take(10))}]\noracle: [{string.Join(",", oracle.Take(10))}]");
                        break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"set-op shape threw (seed={seed} case={i} kind={kind} op={SetOpName(op)} windowed={windowed})\npred1: {pred1}\npred2: {pred2}", ex);
            }
        }

        Assert.True(unsupported < cases / 5,
            $"{unsupported}/{cases} set-op shapes were declined as unsupported (seed {seed}).");
    }

    /// <summary>
    /// Runs generated SelectMany flatten shapes (correlated inner join, filtered
    /// correlation, cross join, correlated DefaultIfEmpty left join in both
    /// projected and entity-result forms) against the context and the
    /// LINQ-to-Objects oracle. A coin flip windows the outer source with
    /// OrderBy/Skip/Take first so the derived-table outer branch gets fuzzed too.
    /// </summary>
    internal static void RunSelectManyFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;

        for (var i = 0; i < cases; i++)
        {
            var predicate = GeneratePredicate(rng);
            var kind = rng.Next(5);
            var caseRng = new Random(rng.Next());

            IQueryable<Row> parents = ctx.Query<Row>().Where(predicate);
            IQueryable<Row> oracleParents = Rows.AsQueryable().Where(predicate);

            var windowed = caseRng.Next(3) == 0;
            if (windowed)
            {
                var skip = caseRng.Next(0, 10);
                var take = caseRng.Next(1, 20);
                parents = parents.OrderBy(p => p.Id).Skip(skip).Take(take);
                oracleParents = oracleParents.OrderBy(p => p.Id).Skip(skip).Take(take);
            }

            try
            {
                switch (kind)
                {
                    case 0: // correlated flatten, entity result
                    {
                        var db = parents.SelectMany(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id))
                            .ToList().Select(c => (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        var oracle = oracleParents.SelectMany(p => Children.AsQueryable().Where(c => c.ParentId == p.Id))
                            .ToList().Select(c => (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"correlated flatten mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 1: // filtered correlation with result selector
                    {
                        var k = caseRng.Next(-3, 3);
                        var db = parents.SelectMany(
                                p => ctx.Query<Child>().Where(c => c.ParentId == p.Id && c.ChildVal > k),
                                (p, c) => new JoinRow(p.Id, c.Id, p.IntVal + c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.SelectMany(
                                p => Children.AsQueryable().Where(c => c.ParentId == p.Id && c.ChildVal > k),
                                (p, c) => new JoinRow(p.Id, c.Id, p.IntVal + c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"filtered correlation mismatch seed={seed} case={i} windowed={windowed} k={k}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 2: // cross join (outer capped to bound the row explosion)
                    {
                        var db = parents.Where(p => p.Id <= 6)
                            .SelectMany(p => ctx.Query<Child>(), (p, c) => new JoinRow(p.Id, c.Id, p.IntVal - c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.Where(p => p.Id <= 6)
                            .SelectMany(p => Children.AsQueryable(), (p, c) => new JoinRow(p.Id, c.Id, p.IntVal - c.ChildVal))
                            .ToList().OrderBy(x => x.PId).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"cross join mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    case 3: // correlated DefaultIfEmpty left join, null-safe projection
                    {
                        var db = parents.SelectMany(
                                p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).DefaultIfEmpty(),
                                (p, c) => new { p.Id, CId = c == null ? (int?)null : (int?)c.Id, V = c == null ? 0 : c.ChildVal })
                            .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                        var oracle = oracleParents.SelectMany(
                                p => Children.AsQueryable().Where(c => c.ParentId == p.Id).DefaultIfEmpty(),
                                (p, c) => new { p.Id, CId = c == null ? (int?)null : (int?)c.Id, V = c == null ? 0 : c.ChildVal })
                            .ToList().OrderBy(x => x.Id).ThenBy(x => x.CId).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"left join flatten mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                    default: // correlated DefaultIfEmpty left join, entity result (null elements)
                    {
                        var db = parents.SelectMany(
                                p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).DefaultIfEmpty())
                            .ToList().Select(c => c == null ? (0, 0, 0) : (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        var oracle = oracleParents.SelectMany(
                                p => Children.AsQueryable().Where(c => c.ParentId == p.Id).DefaultIfEmpty())
                            .ToList().Select(c => c == null ? (0, 0, 0) : (c.Id, c.ParentId, c.ChildVal)).OrderBy(x => x).ToList();
                        Assert.True(db.SequenceEqual(oracle),
                            $"left join entity flatten mismatch seed={seed} case={i} windowed={windowed}\npredicate: {predicate}\ndb: {db.Count} rows, oracle: {oracle.Count} rows");
                        break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"selectmany shape threw (seed={seed} case={i} kind={kind} windowed={windowed})\npredicate: {predicate}", ex);
            }
        }

        Assert.True(unsupported < cases / 5,
            $"{unsupported}/{cases} SelectMany shapes were declined as unsupported (seed {seed}).");
    }

    /// <summary>
    /// Runs <paramref name="cases"/> generated query shapes against the context
    /// and the LINQ-to-Objects oracle. Shared by the SQLite fast test and the
    /// live-provider variant so the same shapes exercise every dialect.
    /// </summary>
    /// <summary>
    /// True when <paramref name="predicate"/> evaluates identically under .NET's IL expression
    /// compiler (<c>Compile(false)</c>) and its tree interpreter (<c>Compile(true)</c>) across the
    /// whole dataset. The LINQ-to-Objects oracle (<see cref="EnumerableQuery{T}"/>) filters via
    /// <c>Expression.Compile()</c> — the IL compiler — which has defects on some deeply-nested
    /// boolean/arithmetic/nullable trees where it disagrees with both its own interpreter and a
    /// C#-compiled delegate (first observed at seed 3002168, case 343). When the two .NET
    /// compilation modes disagree the oracle is self-inconsistent, so no answer can fairly hold
    /// nORM to account — such cases are skipped rather than recorded as spurious failures.
    /// </summary>
    private static bool OracleSelfConsistent(Expression<Func<Row, bool>> predicate)
    {
        var il = predicate.Compile(preferInterpretation: false);
        var interpreted = predicate.Compile(preferInterpretation: true);
        foreach (var r in Rows)
            if (il(r) != interpreted(r))
                return false;
        return true;
    }

    internal static void RunFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;
        var skipped = 0;

        for (var i = 0; i < cases; i++)
        {
            var predicate = GeneratePredicate(rng);
            var secondPredicate = rng.Next(3) == 0 ? GeneratePredicate(rng) : null;
            var caseRng = new Random(rng.Next());

            // Only assert against an oracle .NET evaluates consistently (see OracleSelfConsistent).
            if (!OracleSelfConsistent(predicate) || (secondPredicate != null && !OracleSelfConsistent(secondPredicate)))
            {
                skipped++;
                continue;
            }

            IQueryable<Row> dbQuery = ctx.Query<Row>().Where(predicate);
            if (secondPredicate != null)
                dbQuery = dbQuery.Where(secondPredicate);

            var pagedDb = ApplyOrderedPaging(dbQuery, caseRng, out var ordered);

            try
            {
                RunTerminal(pagedDb, predicate, secondPredicate, caseRng, ordered, seed, i);
            }
            catch (NormUnsupportedFeatureException)
            {
                unsupported++;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                throw new InvalidOperationException(
                    $"shape execution threw (seed={seed} case={i} ordered={ordered})\npredicate: {predicate}\nsecond: {secondPredicate}\npipeline: {pagedDb.Expression}",
                    ex);
            }
        }

        // The generator intentionally emits only supported shapes; if a large
        // share starts throwing, the generator or translator regressed.
        Assert.True(unsupported < cases / 10,
            $"{unsupported}/{cases} generated shapes were declined as unsupported (seed {seed}).");

        // Transparency: report any cases dropped because .NET could not evaluate the oracle
        // consistently (a .NET Expression.Compile defect, not reduced nORM coverage by choice).
        if (skipped > 0)
            System.Console.WriteLine($"[LinqParityFuzz] seed={seed}: skipped {skipped}/{cases} case(s) with a .NET-inconsistent oracle (IL compiler vs interpreter disagree).");
    }

    private static void RunTerminal(
        IQueryable<Row> db,
        Expression<Func<Row, bool>> predicate,
        Expression<Func<Row, bool>>? secondPredicate,
        Random caseRng,
        bool ordered,
        int seed,
        int caseIndex)
    {
        // Evaluate the DB query's exact expression tree (predicates AND paging)
        // against LINQ-to-Objects by swapping the nORM root for the in-memory one.
        var oracleRoot = Rows.AsQueryable();
        var swapped = new RootSwapper(oracleRoot.Expression).Visit(db.Expression)!;
        var oracle = oracleRoot.Provider.CreateQuery<Row>(swapped);

        var terminal = caseRng.Next(7);
        string Describe() =>
            $"seed={seed} case={caseIndex} ordered={ordered} terminal={terminal}\npredicate: {predicate}\nsecond: {secondPredicate}\npipeline: {db.Expression}";

        try
        {
        switch (terminal)
        {
            case 0: // materialize
                var dbRows = db.Select(r => r.Id).ToList();
                var oracleRows = oracle.Select(r => r.Id).ToList();
                if (ordered)
                    Assert.True(dbRows.SequenceEqual(oracleRows), $"ordered row mismatch\n{Describe()}\ndb: [{string.Join(",", dbRows)}]\noracle: [{string.Join(",", oracleRows)}]");
                else
                    Assert.True(dbRows.OrderBy(x => x).SequenceEqual(oracleRows.OrderBy(x => x)), $"row set mismatch\n{Describe()}\ndb: [{string.Join(",", dbRows)}]\noracle: [{string.Join(",", oracleRows)}]");
                break;
            case 1:
                Assert.True(db.Count() == oracle.Count(), $"Count mismatch\n{Describe()}\ndb: {db.Count()} oracle: {oracle.Count()}");
                break;
            case 2:
                var dbSum = db.Sum(r => r.IntVal);
                var orSum = oracle.Sum(r => r.IntVal);
                Assert.True(dbSum == orSum, $"Sum mismatch\n{Describe()}\ndb: {dbSum} oracle: {orSum}");
                break;
            case 3:
                var dbAny = db.Any();
                var orAny = oracle.Any();
                Assert.True(dbAny == orAny, $"Any mismatch\n{Describe()}\ndb: {dbAny} oracle: {orAny}");
                if (dbAny)
                {
                    var dbMax = db.Max(r => r.Amount);
                    var orMax = oracle.Max(r => r.Amount);
                    Assert.True(dbMax == orMax, $"Max mismatch\n{Describe()}\ndb: {dbMax} oracle: {orMax}");
                }
                break;
            case 4: // group — int key, or nullable string key (NULL group + ordinal case variants)
                if (caseRng.Next(2) == 0)
                {
                    var dbGroups = db.GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.Id) })
                        .ToList().OrderBy(x => x.Key).ToList();
                    var orGroups = oracle.GroupBy(r => r.IntVal)
                        .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.Id) })
                        .ToList().OrderBy(x => x.Key).ToList();
                    Assert.True(dbGroups.SequenceEqual(orGroups), $"GroupBy mismatch\n{Describe()}\ndb: [{string.Join(" | ", dbGroups)}]\noracle: [{string.Join(" | ", orGroups)}]");
                }
                else
                {
                    var dbNickGroups = db.GroupBy(r => r.Nick)
                        .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.Id) })
                        .ToList().OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
                    var orNickGroups = oracle.GroupBy(r => r.Nick)
                        .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.Id) })
                        .ToList().OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
                    Assert.True(dbNickGroups.SequenceEqual(orNickGroups), $"nullable-string GroupBy mismatch\n{Describe()}\ndb: [{string.Join(" | ", dbNickGroups)}]\noracle: [{string.Join(" | ", orNickGroups)}]");
                }
                break;
            case 5: // generated constructor projection (SCV surface)
                var projection = GenerateProjection(caseRng);
                var dbProj = db.Select(projection).ToList();
                var orProj = oracle.AsQueryable().Select(projection).ToList();
                if (!ordered)
                {
                    dbProj = dbProj.OrderBy(x => x.Id).ToList();
                    orProj = orProj.OrderBy(x => x.Id).ToList();
                }
                Assert.True(dbProj.SequenceEqual(orProj),
                    $"projection mismatch\n{Describe()}\nprojection: {projection}\ndb: [{string.Join(" | ", dbProj)}]\noracle: [{string.Join(" | ", orProj)}]");
                break;
            default: // distinct — int scalar, nullable string scalar, or anonymous pair
                switch (caseRng.Next(3))
                {
                    case 0:
                    {
                        var dbDistinct = db.Select(r => r.IntVal).Distinct().ToList().OrderBy(x => x).ToList();
                        var orDistinct = oracle.Select(r => r.IntVal).Distinct().ToList().OrderBy(x => x).ToList();
                        Assert.True(dbDistinct.SequenceEqual(orDistinct),
                            $"Distinct mismatch\n{Describe()}\ndb: [{string.Join(",", dbDistinct)}]\noracle: [{string.Join(",", orDistinct)}]");
                        break;
                    }
                    case 1:
                    {
                        var dbNickDistinct = db.Select(r => r.Nick).Distinct().ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
                        var orNickDistinct = oracle.Select(r => r.Nick).Distinct().ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
                        Assert.True(dbNickDistinct.SequenceEqual(orNickDistinct),
                            $"nullable-string Distinct mismatch\n{Describe()}\ndb: [{string.Join(",", dbNickDistinct)}]\noracle: [{string.Join(",", orNickDistinct)}]");
                        break;
                    }
                    default: // anonymous member pair (NULL + case variants in a composite)
                    {
                        var dbAnon = db.Select(r => new { r.Nick, r.IntVal }).Distinct().ToList()
                            .OrderBy(x => x.Nick, StringComparer.Ordinal).ThenBy(x => x.IntVal).ToList();
                        var orAnon = oracle.Select(r => new { r.Nick, r.IntVal }).Distinct().ToList()
                            .OrderBy(x => x.Nick, StringComparer.Ordinal).ThenBy(x => x.IntVal).ToList();
                        Assert.True(dbAnon.SequenceEqual(orAnon),
                            $"anonymous Distinct mismatch\n{Describe()}\ndb: [{string.Join(" | ", dbAnon)}]\noracle: [{string.Join(" | ", orAnon)}]");
                        break;
                    }
                }
                break;
        }
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NormUnsupportedFeatureException)
        {
            throw new InvalidOperationException("terminal threw: " + Describe(), ex);
        }
    }

    private sealed record ProjRow(int Id, int X, bool B, string? S);

    /// <summary>
    /// Generates a constructor projection over computed expressions so the
    /// SELECT-clause translator gets fuzzed too (arithmetic, boolean, and
    /// nullable-string projections have their own emission paths distinct
    /// from predicates).
    /// </summary>
    private static Expression<Func<Row, ProjRow>> GenerateProjection(Random rng)
    {
        var p = Expression.Parameter(typeof(Row), "r");
        var ctor = typeof(ProjRow).GetConstructors().Single();

        var member = Expression.Property(p, nameof(Row.IntVal));
        var k = rng.Next(1, 4);
        Expression x = rng.Next(4) switch
        {
            0 => Expression.Add(member, Expression.Constant(k)),
            1 => Expression.Subtract(member, Expression.Constant(k)),
            2 => Expression.Multiply(member, Expression.Constant(k)),
            _ => Expression.Property(p, nameof(Row.Id)),
        };
        Expression b = rng.Next(3) switch
        {
            0 => Expression.GreaterThan(member, Expression.Constant(rng.Next(-3, 4))),
            1 => Expression.Property(p, nameof(Row.Flag)),
            _ => Expression.Equal(
                    Expression.Property(Expression.Property(p, nameof(Row.Name)), nameof(string.Length)),
                    Expression.Constant(rng.Next(0, 8))),
        };
        var nick = Expression.Property(p, "Nick");
        var concat = typeof(string).GetMethod(nameof(string.Concat), new[] { typeof(string), typeof(string) })!;
        Expression s = rng.Next(4) switch
        {
            // Raw null round-trip through the reader.
            0 => nick,
            1 => Expression.Coalesce(nick, Expression.Constant(string.Empty)),
            // Conditional on the null test — SCV must not lose the NULL branch.
            2 => Expression.Condition(
                    Expression.Equal(nick, Expression.Constant(null, typeof(string))),
                    Expression.Property(p, nameof(Row.Name)),
                    nick),
            // C# concat treats null as empty ("x" stays "x"); SQL || / CONCAT would null it.
            _ => Expression.Add(nick, Expression.Constant("x"), concat),
        };

        var body = Expression.New(ctor, Expression.Property(p, nameof(Row.Id)), x, b, s);
        return Expression.Lambda<Func<Row, ProjRow>>(body, p);
    }

    /// <summary>
    /// Replaces the nORM query root inside an expression tree with the
    /// LINQ-to-Objects root so the exact same pipeline (including the paging
    /// decisions already baked into the tree) runs against the oracle.
    /// </summary>
    private sealed class RootSwapper : ExpressionVisitor
    {
        private readonly Expression _newRoot;
        public RootSwapper(Expression newRoot) => _newRoot = newRoot;

        public override Expression? Visit(Expression? node)
        {
            if (node is ConstantExpression { Value: IQueryable<Row> } )
                return _newRoot;
            return base.Visit(node);
        }
    }
}
