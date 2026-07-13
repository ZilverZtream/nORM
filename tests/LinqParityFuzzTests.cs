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
        public int Id { get; set; }
        public int IntVal { get; set; }
        public int? NullableInt { get; set; }
        public string Name { get; set; } = string.Empty;
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
        var rows = new List<Row>();
        for (var i = 0; i < 40; i++)
        {
            rows.Add(new Row
            {
                Id = i + 1,
                IntVal = (i % 7) - 3,                    // -3..3 with duplicates
                NullableInt = i % 4 == 0 ? null : (i % 5) - 2,
                Name = names[i % names.Length],
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

        return rng.Next(7) switch
        {
            0 => IntComparison(rng, p),
            1 => NullableIntComparison(rng, p),
            2 => StringLeaf(rng, p),
            3 => rng.Next(2) == 0
                    ? Expression.Property(p, nameof(Row.Flag))
                    : Expression.Not(Expression.Property(p, nameof(Row.Flag))),
            4 => DecimalComparison(rng, p),
            5 => DateComparison(rng, p),
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

    // ── The fuzz run ─────────────────────────────────────────────────────────
    [Theory]
    [InlineData(20260713)]
    [InlineData(42)]
    [InlineData(987654321)]
    [InlineData(1)]
    [InlineData(777_000_111)]
    [InlineData(31337)]
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
                    Amount TEXT NOT NULL,
                    Price REAL NOT NULL,
                    Flag INTEGER NOT NULL,
                    Created TEXT NOT NULL);
                CREATE TABLE FuzzChild_Test (
                    Id INTEGER PRIMARY KEY,
                    ParentId INTEGER NOT NULL,
                    ChildVal INTEGER NOT NULL,
                    Tag TEXT NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), CreateFuzzOptions());
        await SeedAsync(ctx);
        await SeedChildrenAsync(ctx);
        RunFuzz(ctx, seed, cases: 400);
        RunJoinFuzz(ctx, seed, cases: 150);
        RunSelectManyFuzz(ctx, seed, cases: 120);
        RunNavFlattenFuzz(ctx, seed, cases: 120);
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
            mb.Entity<NavRow>().HasMany(r => r.Kids).WithOne()
                               .HasForeignKey(c => c.ParentId, r => r.Id);
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
        public int Id { get; set; }
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
    }

    /// <summary>In-memory oracle copies with the navigation populated.</summary>
    private static NavRow[] BuildNavRows()
        => Rows.Select(r => new NavRow
        {
            Id = r.Id,
            IntVal = r.IntVal,
            NullableInt = r.NullableInt,
            Name = r.Name,
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
            var joinKind = rng.Next(3);
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
                    default: // GroupJoin with per-parent aggregate
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
    internal static void RunFuzz(DbContext ctx, int seed, int cases)
    {
        var rng = new Random(seed);
        var unsupported = 0;

        for (var i = 0; i < cases; i++)
        {
            var predicate = GeneratePredicate(rng);
            var secondPredicate = rng.Next(3) == 0 ? GeneratePredicate(rng) : null;
            var caseRng = new Random(rng.Next());

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
            case 4: // group
                var dbGroups = db.GroupBy(r => r.IntVal)
                    .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.Id) })
                    .ToList().OrderBy(x => x.Key).ToList();
                var orGroups = oracle.GroupBy(r => r.IntVal)
                    .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.Id) })
                    .ToList().OrderBy(x => x.Key).ToList();
                Assert.True(dbGroups.SequenceEqual(orGroups), $"GroupBy mismatch\n{Describe()}\ndb: [{string.Join(" | ", dbGroups)}]\noracle: [{string.Join(" | ", orGroups)}]");
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
            default: // distinct scalar
                var dbDistinct = db.Select(r => r.IntVal).Distinct().ToList().OrderBy(x => x).ToList();
                var orDistinct = oracle.Select(r => r.IntVal).Distinct().ToList().OrderBy(x => x).ToList();
                Assert.True(dbDistinct.SequenceEqual(orDistinct),
                    $"Distinct mismatch\n{Describe()}\ndb: [{string.Join(",", dbDistinct)}]\noracle: [{string.Join(",", orDistinct)}]");
                break;
        }
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NormUnsupportedFeatureException)
        {
            throw new InvalidOperationException("terminal threw: " + Describe(), ex);
        }
    }

    private sealed record ProjRow(int Id, int X, bool B);

    /// <summary>
    /// Generates a constructor projection over computed expressions so the
    /// SELECT-clause translator gets fuzzed too (arithmetic and boolean
    /// projections have their own emission paths distinct from predicates).
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

        var body = Expression.New(ctor, Expression.Property(p, nameof(Row.Id)), x, b);
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
