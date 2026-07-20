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

        // ─── compilation ────────────────────────────────────────────────────

        private static IEnumerable<IrRow> RunLinq(QueryIr ir)
        {
            IEnumerable<IrRow> q = ir.Rows;
            foreach (var w in ir.Steps.Where(s => s.Kind == IrStepKind.Where))
                q = q.Where(BuildPredicate(w).Compile());
            if (ir.Steps.Any(s => s.Kind == IrStepKind.Distinct))
                q = q.Distinct(IrRowComparer.Instance);
            q = ApplyOrdering(q, ir);
            foreach (var s in ir.Steps.Where(s => s.Kind == IrStepKind.Skip)) q = q.Skip(s.Count);
            foreach (var s in ir.Steps.Where(s => s.Kind == IrStepKind.Take)) q = q.Take(s.Count);
            return q;
        }

        private static IEnumerable<IrRow> RunNorm(DbContext ctx, QueryIr ir)
        {
            IQueryable<IrRow> q = ctx.Query<IrRow>();
            foreach (var w in ir.Steps.Where(s => s.Kind == IrStepKind.Where))
                q = q.Where(BuildPredicate(w));
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

        private static Expression<Func<IrRow, bool>> BuildPredicate(IrStep step)
        {
            var p = Expression.Parameter(typeof(IrRow), "r");
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
            if (kinds.Contains(IrStepKind.OrderBy)) f.Add("orderby");
            if (kinds.Contains(IrStepKind.Distinct)) f.Add("distinct");
            if (kinds.Contains(IrStepKind.Skip) || kinds.Contains(IrStepKind.Take)) f.Add("paging");
            if (ir.Steps.Count(s => s.Kind == IrStepKind.Where) >= 2) f.Add("where-multi");
            if (kinds.Contains(IrStepKind.OrderBy) && (kinds.Contains(IrStepKind.Skip) || kinds.Contains(IrStepKind.Take))) f.Add("orderby+paging");
            if (kinds.Contains(IrStepKind.Where) && kinds.Contains(IrStepKind.OrderBy)) f.Add("where+orderby");
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
