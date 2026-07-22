using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Compiles a <see cref="QueryIr"/> to BOTH the nORM query and a LINQ-to-Objects oracle, executes them
    /// over the same data, and classifies the result into a <see cref="FuzzCaseResult"/> for the measurement
    /// core. Because the two executions share one IR, the differential is a metamorphic check: they must agree
    /// on the row set (and, when the query pins a total order, the exact sequence). Disagreement is a
    /// <see cref="FuzzOutcome.WrongResult"/>; an unsupported-shape throw is a rejection; a re-run disagreement
    /// is <see cref="FuzzOutcome.NonDeterministic"/>.
    /// </summary>
    public static class QueryIrDifferential
    {
        public const string Family = "query-ir";
        public const int GeneratorVersion = 1;

        public static FuzzCaseResult Execute(QueryIr ir, long seed)
        {
            if (ir.GroupBy != null)
                return ExecuteScalar(ir, seed, RunLinqGrouped, RunNormGrouped);
            if (ir.Projection != null)
                return ExecuteProjected(ir, seed);

            var serialized = ir.ToJson();
            List<IrRow> expected;
            try
            {
                expected = RunLinq(ir).ToList();
            }
            catch (Exception ex)
            {
                // The oracle itself faulting means the IR is ill-formed — a harness bug, surfaced not hidden.
                return Fail(FuzzOutcome.UnexpectedException, $"oracle threw {ex.GetType().Name}", ir, seed, serialized);
            }

            List<IrRow> actual, actual2;
            using (var ctx = CreateSeededContext(ir.Rows))
            {
                try
                {
                    actual = RunNorm(ctx, ir).ToList();
                    actual2 = RunNorm(ctx, ir).ToList();   // second run to catch non-determinism
                }
                catch (NormUnsupportedFeatureException nufe)
                {
                    // The IR only builds supported shapes, so a rejection is a capability gap to investigate,
                    // not a healthy documented rejection. Recorded with a reason code the contract can allow later.
                    return new FuzzCaseResult
                    {
                        Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                        Outcome = FuzzOutcome.UnexpectedlyRejected,
                        ReasonCode = "query-ir/" + FirstUnsupportedToken(nufe.Message),
                        SerializedCase = serialized, Detail = nufe.Message,
                        Features = ExtractFeatures(ir),
                    };
                }
                catch (Exception ex)
                {
                    return Fail(FuzzOutcome.UnexpectedException, $"nORM threw {ex.GetType().Name}: {ex.Message}", ir, seed, serialized);
                }
            }

            var totallyOrdered = IsTotallyOrdered(ir);
            if (!RowsEqual(actual, actual2, totallyOrdered))
                return Fail(FuzzOutcome.NonDeterministic, "two nORM executions of the same case disagreed", ir, seed, serialized);

            if (!RowsEqual(expected, actual, totallyOrdered))
                return Fail(FuzzOutcome.WrongResult,
                    $"oracle={Render(expected)} nORM={Render(actual)} (ordered={totallyOrdered})", ir, seed, serialized);

            return new FuzzCaseResult
            {
                Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                Outcome = FuzzOutcome.Executed, SerializedCase = serialized, Features = ExtractFeatures(ir),
            };
        }

        /// <summary>
        /// Minimizes a failing IR to a compact regression: first delta-debugs the step list, then the rows,
        /// each reduction adopted only while it reproduces the SAME failure signature. The result is the
        /// smallest query + data that still diverges — "these rows through these steps disagree", not the whole
        /// generated case. <paramref name="execute"/> defaults to the real differential; a synthetic executor
        /// can be supplied to test the reducer.
        /// </summary>
        public static QueryIr ShrinkPreservingFailure(QueryIr failing, Func<QueryIr, FuzzCaseResult>? execute = null)
        {
            execute ??= ir => Execute(ir, seed: 0);
            var target = execute(failing);
            if (!target.Outcome.IsFailure())
                throw new ArgumentException("The case to shrink must reproduce a failure.", nameof(failing));
            var signature = target.FailureSignature();

            bool StillFails(QueryIr candidate)
            {
                var r = execute(candidate);
                return r.Outcome.IsFailure() && r.FailureSignature() == signature;
            }

            var steps = Shrinker.MinimizeSequence(failing.Steps,
                s => StillFails(failing with { Steps = s.ToList() }));
            var afterSteps = failing with { Steps = steps.ToList() };

            var rows = Shrinker.MinimizeSequence(afterSteps.Rows,
                r => StillFails(afterSteps with { Rows = r.ToList() }));
            return afterSteps with { Rows = rows.ToList() };
        }

        // ─── projected path (scalar Select, multiset comparison) ────────────

        private static FuzzCaseResult ExecuteProjected(QueryIr ir, long seed)
            => ExecuteScalar(ir, seed, RunLinqProjected, RunNormProjected);

        // Shared int-multiset differential for the scalar-producing shapes (scalar projection, grouped Count).
        private static FuzzCaseResult ExecuteScalar(QueryIr ir, long seed,
            Func<QueryIr, IEnumerable<int>> runLinq, Func<DbContext, QueryIr, IEnumerable<int>> runNorm)
        {
            var serialized = ir.ToJson();
            List<int> expected;
            try
            {
                expected = runLinq(ir).ToList();
            }
            catch (Exception ex)
            {
                return Fail(FuzzOutcome.UnexpectedException, $"oracle threw {ex.GetType().Name}", ir, seed, serialized);
            }

            List<int> actual, actual2;
            using (var ctx = CreateSeededContext(ir.Rows))
            {
                try
                {
                    actual = runNorm(ctx, ir).ToList();
                    actual2 = runNorm(ctx, ir).ToList();
                }
                catch (NormUnsupportedFeatureException nufe)
                {
                    return new FuzzCaseResult
                    {
                        Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                        Outcome = FuzzOutcome.UnexpectedlyRejected,
                        ReasonCode = "query-ir/" + FirstUnsupportedToken(nufe.Message),
                        SerializedCase = serialized, Detail = nufe.Message, Features = ExtractFeatures(ir),
                    };
                }
                catch (Exception ex)
                {
                    return Fail(FuzzOutcome.UnexpectedException, $"nORM threw {ex.GetType().Name}: {ex.Message}", ir, seed, serialized);
                }
            }

            if (!IntsEqual(actual, actual2))
                return Fail(FuzzOutcome.NonDeterministic, "two nORM executions of the same case disagreed", ir, seed, serialized);
            if (!IntsEqual(expected, actual))
                return Fail(FuzzOutcome.WrongResult,
                    $"oracle=[{string.Join(",", expected.OrderBy(x => x))}] nORM=[{string.Join(",", actual.OrderBy(x => x))}]", ir, seed, serialized);

            return new FuzzCaseResult
            {
                Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                Outcome = FuzzOutcome.Executed, SerializedCase = serialized, Features = ExtractFeatures(ir),
            };
        }

        // Key-aware: encode (key, aggregate) so a wrong-key grouping with a matching aggregate still diverges. All
        // key and aggregate values are tiny here (rows < 9, columns < 9, so any Count/Sum/Min/Max < 1000), making
        // key*1000+value collision-free. The encode runs in memory over the materialized keyed projection, so nORM
        // still translates the keyed group SELECT (including the SUM/MIN/MAX aggregate).
        private static IEnumerable<int> RunLinqGrouped(QueryIr ir)
        {
            IEnumerable<IrRow> rows = ir.Rows;
            foreach (var w in ir.Steps.Where(IsPredicateStep))
                rows = rows.Where(BuildPredicate(w).Compile());
            var grouped = rows.GroupBy(KeySelector(ir.GroupBy!.Key).Compile());
            var gb = ir.GroupBy!;
            return gb.Aggregate switch
            {
                IrAggregate.Count => grouped.Select(g => Encode(g.Key, g.Count())),
                IrAggregate.Sum => gb.AggregateColumn switch
                {
                    IrColumn.Id => grouped.Select(g => Encode(g.Key, g.Sum(r => r.Id))),
                    IrColumn.A => grouped.Select(g => Encode(g.Key, g.Sum(r => r.A))),
                    _ => grouped.Select(g => Encode(g.Key, g.Sum(r => r.B))),
                },
                IrAggregate.Min => gb.AggregateColumn switch
                {
                    IrColumn.Id => grouped.Select(g => Encode(g.Key, g.Min(r => r.Id))),
                    IrColumn.A => grouped.Select(g => Encode(g.Key, g.Min(r => r.A))),
                    _ => grouped.Select(g => Encode(g.Key, g.Min(r => r.B))),
                },
                _ => gb.AggregateColumn switch
                {
                    IrColumn.Id => grouped.Select(g => Encode(g.Key, g.Max(r => r.Id))),
                    IrColumn.A => grouped.Select(g => Encode(g.Key, g.Max(r => r.A))),
                    _ => grouped.Select(g => Encode(g.Key, g.Max(r => r.B))),
                },
            };
        }

        private static IEnumerable<int> RunNormGrouped(DbContext ctx, QueryIr ir)
        {
            IQueryable<IrRow> q = ctx.Query<IrRow>();
            foreach (var w in ir.Steps.Where(IsPredicateStep))
                q = q.Where(BuildPredicate(w));
            var grouped = q.GroupBy(KeySelector(ir.GroupBy!.Key));
            var gb = ir.GroupBy!;
            var pairs = gb.Aggregate switch
            {
                IrAggregate.Count => grouped.Select(g => new { g.Key, V = g.Count() }).ToList(),
                IrAggregate.Sum => gb.AggregateColumn switch
                {
                    IrColumn.Id => grouped.Select(g => new { g.Key, V = g.Sum(r => r.Id) }).ToList(),
                    IrColumn.A => grouped.Select(g => new { g.Key, V = g.Sum(r => r.A) }).ToList(),
                    _ => grouped.Select(g => new { g.Key, V = g.Sum(r => r.B) }).ToList(),
                },
                IrAggregate.Min => gb.AggregateColumn switch
                {
                    IrColumn.Id => grouped.Select(g => new { g.Key, V = g.Min(r => r.Id) }).ToList(),
                    IrColumn.A => grouped.Select(g => new { g.Key, V = g.Min(r => r.A) }).ToList(),
                    _ => grouped.Select(g => new { g.Key, V = g.Min(r => r.B) }).ToList(),
                },
                _ => gb.AggregateColumn switch
                {
                    IrColumn.Id => grouped.Select(g => new { g.Key, V = g.Max(r => r.Id) }).ToList(),
                    IrColumn.A => grouped.Select(g => new { g.Key, V = g.Max(r => r.A) }).ToList(),
                    _ => grouped.Select(g => new { g.Key, V = g.Max(r => r.B) }).ToList(),
                },
            };
            return pairs.Select(x => Encode(x.Key, x.V));
        }

        private static int Encode(int key, int value) => key * 1000 + value;

        private static IEnumerable<int> RunLinqProjected(QueryIr ir)
        {
            IEnumerable<IrRow> left = ir.Rows;
            foreach (var w in ir.Steps.Where(IsPredicateStep))
                left = left.Where(BuildPredicate(w).Compile());
            IEnumerable<IrRow> rows = left;
            if (ir.SetOp is { } setOp)
            {
                IEnumerable<IrRow> right = ir.Rows;
                foreach (var w in setOp.RightWheres.Where(IsPredicateStep))
                    right = right.Where(BuildPredicate(w).Compile());
                rows = setOp.Kind switch
                {
                    IrSetOpKind.Union => left.Union(right, IrRowComparer.Instance),
                    IrSetOpKind.Concat => left.Concat(right),
                    IrSetOpKind.Intersect => left.Intersect(right, IrRowComparer.Instance),
                    IrSetOpKind.Except => left.Except(right, IrRowComparer.Instance),
                    _ => left,
                };
            }
            var projected = rows.Select(BuildProjection(ir.Projection!).Compile());
            if (ir.Steps.Any(s => s.Kind == IrStepKind.Distinct))
                projected = projected.Distinct();
            return projected;
        }

        private static IEnumerable<int> RunNormProjected(DbContext ctx, QueryIr ir)
        {
            IQueryable<IrRow> Filter(IReadOnlyList<IrStep> steps)
            {
                IQueryable<IrRow> s = ctx.Query<IrRow>();
                foreach (var w in steps.Where(IsPredicateStep))
                    s = s.Where(BuildPredicate(w));
                return s;
            }

            var left = Filter(ir.Steps);
            IQueryable<IrRow> rows = left;
            if (ir.SetOp is { } setOp)
            {
                var right = Filter(setOp.RightWheres);
                rows = setOp.Kind switch
                {
                    IrSetOpKind.Union => left.Union(right),
                    IrSetOpKind.Concat => left.Concat(right),
                    IrSetOpKind.Intersect => left.Intersect(right),
                    IrSetOpKind.Except => left.Except(right),
                    _ => left,
                };
            }
            IQueryable<int> projected = rows.Select(BuildProjection(ir.Projection!));
            if (ir.Steps.Any(s => s.Kind == IrStepKind.Distinct))
                projected = projected.Distinct();
            return projected.ToList();
        }

        private static Expression<Func<IrRow, int>> BuildProjection(IrProjection proj)
        {
            var p = Expression.Parameter(typeof(IrRow), "r");
            Expression body = Expression.Property(p, proj.Column.ToString());
            if (proj.Add != 0)
                body = Expression.Add(body, Expression.Constant(proj.Add, typeof(int)));
            return Expression.Lambda<Func<IrRow, int>>(body, p);
        }

        private static bool IntsEqual(IReadOnlyList<int> a, IReadOnlyList<int> b)
            => a.Count == b.Count && a.OrderBy(x => x).SequenceEqual(b.OrderBy(x => x));

        // ─── compilation ────────────────────────────────────────────────────

        private static IEnumerable<IrRow> RunLinq(QueryIr ir)
        {
            IEnumerable<IrRow> left = ir.Rows;
            foreach (var w in ir.Steps.Where(IsPredicateStep))
                left = left.Where(BuildPredicate(w).Compile());

            IEnumerable<IrRow> q = left;
            if (ir.SetOp is { } setOp)
            {
                IEnumerable<IrRow> right = ir.Rows;
                foreach (var w in setOp.RightWheres.Where(IsPredicateStep))
                    right = right.Where(BuildPredicate(w).Compile());
                q = setOp.Kind switch
                {
                    IrSetOpKind.Union => left.Union(right, IrRowComparer.Instance),
                    IrSetOpKind.Concat => left.Concat(right),
                    IrSetOpKind.Intersect => left.Intersect(right, IrRowComparer.Instance),
                    IrSetOpKind.Except => left.Except(right, IrRowComparer.Instance),
                    _ => left,
                };
            }

            if (ir.Steps.Any(s => s.Kind == IrStepKind.Distinct))
                q = q.Distinct(IrRowComparer.Instance);
            q = ApplyOrdering(q, ir);
            foreach (var s in ir.Steps.Where(s => s.Kind == IrStepKind.Skip)) q = q.Skip(s.Count);
            foreach (var s in ir.Steps.Where(s => s.Kind == IrStepKind.Take)) q = q.Take(s.Count);
            return q;
        }

        private static IEnumerable<IrRow> RunNorm(DbContext ctx, QueryIr ir)
        {
            IQueryable<IrRow> Filter(IReadOnlyList<IrStep> steps)
            {
                IQueryable<IrRow> s = ctx.Query<IrRow>();
                foreach (var w in steps.Where(IsPredicateStep))
                    s = s.Where(BuildPredicate(w));
                return s;
            }

            var left = Filter(ir.Steps);
            IQueryable<IrRow> q = left;
            if (ir.SetOp is { } setOp)
            {
                var right = Filter(setOp.RightWheres);
                q = setOp.Kind switch
                {
                    IrSetOpKind.Union => left.Union(right),
                    IrSetOpKind.Concat => left.Concat(right),
                    IrSetOpKind.Intersect => left.Intersect(right),
                    IrSetOpKind.Except => left.Except(right),
                    _ => left,
                };
            }

            if (ir.Steps.Any(s => s.Kind == IrStepKind.Distinct))
                q = q.Distinct();
            q = ApplyOrderingQueryable(q, ir);
            foreach (var s in ir.Steps.Where(s => s.Kind == IrStepKind.Skip)) q = q.Skip(s.Count);
            foreach (var s in ir.Steps.Where(s => s.Kind == IrStepKind.Take)) q = q.Take(s.Count);
            return q.ToList();
        }

        private static IEnumerable<IrRow> ApplyOrdering(IEnumerable<IrRow> q, QueryIr ir)
        {
            var keys = ir.Steps.Where(s => s.Kind == IrStepKind.OrderBy).ToList();
            if (keys.Count == 0 && !NeedsTotalOrder(ir))
                return q;
            IOrderedEnumerable<IrRow>? oq = null;
            foreach (var k in keys)
            {
                var sel = KeySelector(k.Column).Compile();
                oq = oq == null
                    ? (k.Descending ? q.OrderByDescending(sel) : q.OrderBy(sel))
                    : (k.Descending ? oq.ThenByDescending(sel) : oq.ThenBy(sel));
            }
            // Id tiebreaker makes any ordered/paged result a total order, so the comparison is well-defined.
            oq = oq == null ? q.OrderBy(r => r.Id) : oq.ThenBy(r => r.Id);
            return oq;
        }

        private static IQueryable<IrRow> ApplyOrderingQueryable(IQueryable<IrRow> q, QueryIr ir)
        {
            var keys = ir.Steps.Where(s => s.Kind == IrStepKind.OrderBy).ToList();
            if (keys.Count == 0 && !NeedsTotalOrder(ir))
                return q;
            IOrderedQueryable<IrRow>? oq = null;
            foreach (var k in keys)
            {
                var sel = KeySelector(k.Column);
                oq = oq == null
                    ? (k.Descending ? q.OrderByDescending(sel) : q.OrderBy(sel))
                    : (k.Descending ? oq.ThenByDescending(sel) : oq.ThenBy(sel));
            }
            oq = oq == null ? q.OrderBy(r => r.Id) : oq.ThenBy(r => r.Id);
            return oq;
        }

        private static bool NeedsTotalOrder(QueryIr ir) =>
            ir.Steps.Any(s => s.Kind is IrStepKind.Skip or IrStepKind.Take);

        private static bool IsTotallyOrdered(QueryIr ir) =>
            ir.Steps.Any(s => s.Kind is IrStepKind.OrderBy or IrStepKind.Skip or IrStepKind.Take);

        private static bool IsPredicateStep(IrStep s) => s.Kind is IrStepKind.Where or IrStepKind.WhereName;

        private static Expression<Func<IrRow, bool>> BuildPredicate(IrStep step)
        {
            var p = Expression.Parameter(typeof(IrRow), "r");
            if (step.Kind == IrStepKind.WhereName)
            {
                // C# string ==/!= and nORM's string comparison are both ordinal (case-sensitive), so the same
                // expression is the oracle and the nORM query — a case-insensitive divergence would be a real bug.
                var nameMember = Expression.Property(p, nameof(IrRow.Name));
                var text = Expression.Constant(step.Text, typeof(string));
                Expression cmp = step.StringOp == IrStringOp.Eq
                    ? Expression.Equal(nameMember, text)
                    : Expression.NotEqual(nameMember, text);
                return Expression.Lambda<Func<IrRow, bool>>(cmp, p);
            }
            var member = Expression.Property(p, step.Column.ToString());
            var constant = Expression.Constant(step.Value, typeof(int));
            Expression body = step.Op switch
            {
                IrCompare.Eq => Expression.Equal(member, constant),
                IrCompare.Ne => Expression.NotEqual(member, constant),
                IrCompare.Lt => Expression.LessThan(member, constant),
                IrCompare.Le => Expression.LessThanOrEqual(member, constant),
                IrCompare.Gt => Expression.GreaterThan(member, constant),
                IrCompare.Ge => Expression.GreaterThanOrEqual(member, constant),
                _ => throw new ArgumentOutOfRangeException(nameof(step)),
            };
            return Expression.Lambda<Func<IrRow, bool>>(body, p);
        }

        private static Expression<Func<IrRow, int>> KeySelector(IrColumn column)
        {
            var p = Expression.Parameter(typeof(IrRow), "r");
            return Expression.Lambda<Func<IrRow, int>>(Expression.Property(p, column.ToString()), p);
        }

        // ─── data / comparison ──────────────────────────────────────────────

        private static DbContext CreateSeededContext(IReadOnlyList<IrRow> rows)
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE IrRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }
            foreach (var r in rows)
            {
                using var c = cn.CreateCommand();
                c.CommandText = "INSERT INTO IrRow (Id, A, B, Name) VALUES ($id,$a,$b,$n)";
                c.Parameters.AddWithValue("$id", r.Id);
                c.Parameters.AddWithValue("$a", r.A);
                c.Parameters.AddWithValue("$b", r.B);
                c.Parameters.AddWithValue("$n", r.Name);
                c.ExecuteNonQuery();
            }
            return new DbContext(cn, new SqliteProvider(), new nORM.Configuration.DbContextOptions(), ownsConnection: true);
        }

        private static bool RowsEqual(IReadOnlyList<IrRow> a, IReadOnlyList<IrRow> b, bool ordered)
        {
            if (a.Count != b.Count) return false;
            IEnumerable<IrRow> ax = a, bx = b;
            if (!ordered)
            {
                ax = a.OrderBy(r => r.Id).ThenBy(r => r.A).ThenBy(r => r.B).ThenBy(r => r.Name, StringComparer.Ordinal);
                bx = b.OrderBy(r => r.Id).ThenBy(r => r.A).ThenBy(r => r.B).ThenBy(r => r.Name, StringComparer.Ordinal);
            }
            return ax.Zip(bx, (x, y) => IrRowComparer.Instance.Equals(x, y)).All(eq => eq);
        }

        private static string Render(IReadOnlyList<IrRow> rows) =>
            "[" + string.Join(",", rows.Select(r => $"({r.Id},{r.A},{r.B},{r.Name})")) + "]";

        private static string FirstUnsupportedToken(string message) =>
            new string(message.TakeWhile(ch => char.IsLetterOrDigit(ch) || ch == ' ').ToArray()).Trim().Replace(' ', '-').ToLowerInvariant();

        private static FuzzCaseResult Fail(FuzzOutcome outcome, string detail, QueryIr ir, long seed, string serialized) => new()
        {
            Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
            Outcome = outcome, SerializedCase = serialized, Detail = detail, Features = ExtractFeatures(ir),
        };

        // ─── coverage features ──────────────────────────────────────────────

        public static IReadOnlyList<string> ExtractFeatures(QueryIr ir)
        {
            var f = new SortedSet<string>(StringComparer.Ordinal);
            var kinds = ir.Steps.Select(s => s.Kind).ToHashSet();
            if (kinds.Contains(IrStepKind.Where)) f.Add("where");
            if (kinds.Contains(IrStepKind.WhereName)) f.Add("where-name");
            if (kinds.Contains(IrStepKind.OrderBy)) f.Add("orderby");
            if (kinds.Contains(IrStepKind.Distinct)) f.Add("distinct");
            if (kinds.Contains(IrStepKind.Skip) || kinds.Contains(IrStepKind.Take)) f.Add("paging");
            if (ir.Steps.Count(IsPredicateStep) >= 2) f.Add("where-multi");
            if (kinds.Contains(IrStepKind.OrderBy) && (kinds.Contains(IrStepKind.Skip) || kinds.Contains(IrStepKind.Take))) f.Add("orderby+paging");
            if (kinds.Contains(IrStepKind.Where) && kinds.Contains(IrStepKind.OrderBy)) f.Add("where+orderby");
            if (ir.SetOp is { } setOp)
            {
                f.Add("setop");
                f.Add("setop-" + setOp.Kind.ToString().ToLowerInvariant());
                if (kinds.Contains(IrStepKind.Distinct)) f.Add("setop+distinct");
                if (kinds.Contains(IrStepKind.OrderBy)) f.Add("setop+orderby");
                if (kinds.Contains(IrStepKind.Skip) || kinds.Contains(IrStepKind.Take)) f.Add("setop+paging");
            }
            if (ir.Projection is { } proj)
            {
                f.Add("projection");
                if (proj.Add != 0) f.Add("projection-computed");
                if (ir.SetOp != null) f.Add("setop+projection");
                if (kinds.Contains(IrStepKind.Distinct)) f.Add("projection+distinct");
            }
            if (ir.GroupBy != null)
            {
                f.Add("groupby");
                f.Add("groupby-" + ir.GroupBy.Aggregate.ToString().ToLowerInvariant());
                if (kinds.Contains(IrStepKind.Where)) f.Add("groupby+where");
            }
            if (ir.Rows.Count == 0) f.Add("empty-table");
            if (ir.Rows.Count == 1) f.Add("single-row");
            return f.ToArray();
        }

        private sealed class IrRowComparer : IEqualityComparer<IrRow>
        {
            public static readonly IrRowComparer Instance = new();
            public bool Equals(IrRow? x, IrRow? y) =>
                x != null && y != null && x.Id == y.Id && x.A == y.A && x.B == y.B && string.Equals(x.Name, y.Name, StringComparison.Ordinal);
            public int GetHashCode(IrRow r) => HashCode.Combine(r.Id, r.A, r.B, r.Name);
        }
    }
}
