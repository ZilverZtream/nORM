using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Exercises the typed query-IR differential end to end: a battery of query shapes compiled to both nORM
    /// and a LINQ-to-Objects oracle must agree (recorded as Executed in a manifest with a populated feature
    /// frontier), the IR must round-trip through JSON as the durable artifact, and feature extraction must tag
    /// the plan shapes that drive coverage-guided retention. This is the query-family slice of the serializable
    /// IR the whole harness is built on.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class QueryIrDifferentialTests
    {
        // A has ties (3 at Id 2,5,7) so ordering exercises the Id tiebreaker; some rows filter out.
        // N mixes NULLs (Id 1,3,6) with a repeated value (3 at Id 2,5,7) so NULL predicates and NULL dedup have teeth.
        private static readonly IrRow[] Data =
        {
            new() { Id = 1, A = 8, B = 2, Name = "a", N = null },
            new() { Id = 2, A = 3, B = 9, Name = "b", N = 3 },
            new() { Id = 3, A = 7, B = 7, Name = "c", N = null },
            new() { Id = 4, A = 1, B = 1, Name = "d", N = 7 },
            new() { Id = 5, A = 3, B = 4, Name = "e", N = 3 },
            new() { Id = 6, A = 6, B = 6, Name = "f", N = null },
            new() { Id = 7, A = 3, B = 8, Name = "g", N = 3 },
        };

        private static QueryIr Q(params IrStep[] steps) => new() { Rows = Data, Steps = steps };

        public static IEnumerable<object[]> Shapes()
        {
            var cases = new List<QueryIr>
            {
                Q(),                                                                          // whole table (multiset)
                Q(IrStep.Where(IrColumn.A, IrCompare.Gt, 5)),
                Q(IrStep.Where(IrColumn.A, IrCompare.Eq, 3)),                                  // ties
                Q(IrStep.Where(IrColumn.A, IrCompare.Ge, 3), IrStep.Where(IrColumn.B, IrCompare.Lt, 8)),
                Q(IrStep.Where(IrColumn.Id, IrCompare.Ne, 3)),
                Q(IrStep.Where(IrColumn.A, IrCompare.Lt, 0)),                                  // empty result
                Q(IrStep.OrderBy(IrColumn.A)),                                                 // ties -> Id tiebreak
                Q(IrStep.OrderBy(IrColumn.A, descending: true)),
                Q(IrStep.OrderBy(IrColumn.B), IrStep.Where(IrColumn.A, IrCompare.Gt, 2)),      // canonical order: Where before OrderBy
                Q(IrStep.Where(IrColumn.A, IrCompare.Ge, 3), IrStep.OrderBy(IrColumn.B, descending: true)),
                Q(IrStep.OrderBy(IrColumn.A), IrStep.Skip(2), IrStep.Take(3)),                 // paging over a total order
                Q(IrStep.OrderBy(IrColumn.B, descending: true), IrStep.Take(4)),
                Q(IrStep.Distinct()),                                                          // no-op on unique-key rows
                Q(IrStep.Where(IrColumn.A, IrCompare.Le, 6), IrStep.OrderBy(IrColumn.A), IrStep.Skip(1)),
            };
            return cases.Select((c, i) => new object[] { i, c });
        }

        [Theory]
        [MemberData(nameof(Shapes))]
        public void Nrm_matches_the_linq_oracle_for_each_shape(int index, QueryIr ir)
        {
            var result = QueryIrDifferential.Execute(ir, seed: 1000 + index);
            Assert.True(result.Outcome == FuzzOutcome.Executed,
                $"shape #{index} [{ir.Describe()}] classified {result.Outcome}: {result.Detail}");
        }

        [Fact]
        public void Battery_run_is_clean_and_covers_the_expected_feature_frontier()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "ir-battery", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });

            var i = 0;
            foreach (var row in Shapes())
                manifest.Record(QueryIrDifferential.Execute((QueryIr)row[1], seed: 2000 + i++));

            Assert.True(manifest.IsClean(), "the differential battery must have no failing outcomes");
            var stats = manifest.FamilyStats().Single();
            Assert.Equal(QueryIrDifferential.Family, stats.Family);
            Assert.True(stats.Executed >= 10);
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("where", frontier);
            Assert.Contains("orderby", frontier);
            Assert.Contains("paging", frontier);
            Assert.Contains("orderby+paging", frontier);
        }

        [Fact]
        public void Ir_round_trips_through_json_and_reproduces_the_same_outcome()
        {
            var ir = Q(IrStep.Where(IrColumn.A, IrCompare.Ge, 3), IrStep.OrderBy(IrColumn.B, descending: true), IrStep.Take(2));
            var json = ir.ToJson();
            var restored = QueryIr.FromJson(json);

            Assert.Equal(ir.Describe(), restored.Describe());
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(restored, seed: 42).Outcome);
        }

        [Fact]
        public void Feature_extraction_tags_the_plan_shape()
        {
            var features = QueryIrDifferential.ExtractFeatures(
                Q(IrStep.Where(IrColumn.A, IrCompare.Gt, 2), IrStep.OrderBy(IrColumn.A), IrStep.Skip(1), IrStep.Take(2)));
            Assert.Contains("where", features);
            Assert.Contains("orderby", features);
            Assert.Contains("paging", features);
            Assert.Contains("where+orderby", features);
            Assert.Contains("orderby+paging", features);
        }

        [Fact]
        public void Grouped_aggregates_match_the_linq_oracle()
        {
            // A has ties (three rows at A=3), so grouping by A yields a real multi-row group whose Count/Sum/Min/Max
            // differ from one another and from the key — a wrong-key or wrong-aggregate grouping would diverge.
            foreach (var agg in new[] { IrAggregate.Count, IrAggregate.Sum, IrAggregate.Min, IrAggregate.Max })
            foreach (var col in new[] { IrColumn.Id, IrColumn.A, IrColumn.B })
            {
                var ir = new QueryIr
                {
                    Rows = Data,
                    Steps = Array.Empty<IrStep>(),
                    GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = agg, AggregateColumn = col },
                };
                var r = QueryIrDifferential.Execute(ir, seed: 3100);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }

            // A filtered grouped Sum also matches, and its features tag the aggregate.
            var filtered = new QueryIr
            {
                Rows = Data,
                Steps = new[] { IrStep.Where(IrColumn.B, IrCompare.Lt, 8) },
                GroupBy = new IrGroupBy { Key = IrColumn.B, Aggregate = IrAggregate.Sum, AggregateColumn = IrColumn.A },
            };
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(filtered, seed: 3200).Outcome);
            Assert.Contains("groupby-sum", QueryIrDifferential.ExtractFeatures(filtered));
        }

        [Fact]
        public void Grouped_aggregates_with_a_having_filter_match_the_linq_oracle()
        {
            // HAVING over the same aggregate the group projects (GroupBy(A).Where(g => g.<agg>() <op> v)) must
            // filter groups by the aggregate value, not by the key or post-materialization. Grouping by A (three
            // rows at A=3, two derived-equal via ties elsewhere) gives groups of size 1..3 and distinct
            // Sum/Min/Max, so a HAVING that keeps only some groups is a real discriminator. Every aggregate x
            // every comparison must match the oracle — a dropped/kept-wrong group would diverge.
            foreach (var agg in new[] { IrAggregate.Count, IrAggregate.Sum, IrAggregate.Min, IrAggregate.Max })
            foreach (var op in new[] { IrCompare.Eq, IrCompare.Ne, IrCompare.Lt, IrCompare.Le, IrCompare.Gt, IrCompare.Ge })
            {
                var ir = new QueryIr
                {
                    Rows = Data,
                    Steps = Array.Empty<IrStep>(),
                    GroupBy = new IrGroupBy
                    {
                        Key = IrColumn.A, Aggregate = agg, AggregateColumn = IrColumn.B,
                        Having = new IrHaving { Op = op, Value = 3 },
                    },
                };
                var r = QueryIrDifferential.Execute(ir, seed: 3300);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }

            // HAVING that keeps every group and HAVING that keeps none both round-trip (boundary results).
            var keepsAll = new QueryIr
            {
                Rows = Data, Steps = Array.Empty<IrStep>(),
                GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = IrAggregate.Count, Having = new IrHaving { Op = IrCompare.Ge, Value = 1 } },
            };
            var keepsNone = new QueryIr
            {
                Rows = Data, Steps = Array.Empty<IrStep>(),
                GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = IrAggregate.Count, Having = new IrHaving { Op = IrCompare.Gt, Value = 100 } },
            };
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(keepsAll, seed: 3301).Outcome);
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(keepsNone, seed: 3302).Outcome);
            Assert.Contains("groupby+having", QueryIrDifferential.ExtractFeatures(keepsAll));

            // HAVING composes with a leading Where and with a set-op source.
            var whereThenHaving = new QueryIr
            {
                Rows = Data, Steps = new[] { IrStep.Where(IrColumn.B, IrCompare.Lt, 9) },
                GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = IrAggregate.Sum, AggregateColumn = IrColumn.B, Having = new IrHaving { Op = IrCompare.Gt, Value = 4 } },
            };
            var setopThenHaving = new QueryIr
            {
                Rows = Data, Steps = Array.Empty<IrStep>(),
                SetOp = new IrSetOp { Kind = IrSetOpKind.Union, RightWheres = new[] { IrStep.Where(IrColumn.A, IrCompare.Ge, 3) } },
                GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = IrAggregate.Count, Having = new IrHaving { Op = IrCompare.Ge, Value = 2 } },
            };
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(whereThenHaving, seed: 3303).Outcome);
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(setopThenHaving, seed: 3304).Outcome);

            // A COLUMN aggregate (Sum/Min/Max) in a HAVING over a set-op source is the shape the fuzzer caught:
            // the SELECT and HAVING aggregate columns must bind to the compound's derived-table wrap alias, not a
            // phantom T{n} (which produced `no such column: T1.B`). Count() has no column operand so it hid the bug.
            foreach (var kind in new[] { IrSetOpKind.Union, IrSetOpKind.Concat, IrSetOpKind.Intersect, IrSetOpKind.Except })
            foreach (var agg in new[] { IrAggregate.Sum, IrAggregate.Min, IrAggregate.Max })
            {
                var ir = new QueryIr
                {
                    Rows = Data, Steps = Array.Empty<IrStep>(),
                    SetOp = new IrSetOp { Kind = kind, RightWheres = new[] { IrStep.Where(IrColumn.A, IrCompare.Ge, 3) } },
                    GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = agg, AggregateColumn = IrColumn.B, Having = new IrHaving { Op = IrCompare.Gt, Value = 2 } },
                };
                var r = QueryIrDifferential.Execute(ir, seed: 3400);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }
        }

        [Fact]
        public void Nullable_predicates_match_the_linq_oracle()
        {
            // The nullable N column: IS NULL / IS NOT NULL must translate correctly (a `N = @p` with @p=NULL would
            // silently drop every row), and lifted numeric comparisons must exclude NULL rows (3-valued logic).
            // Data has NULLs at Id 1,3,6 and value 3 at Id 2,5,7, so each predicate has a non-trivial result.
            var cases = new (string Name, IrStep Step, int ExpectedCount)[]
            {
                ("N IS NULL",       IrStep.WhereNullable(IrCompare.Eq, null), 3),   // Id 1,3,6
                ("N IS NOT NULL",   IrStep.WhereNullable(IrCompare.Ne, null), 4),   // Id 2,4,5,7
                ("N == 3",          IrStep.WhereNullable(IrCompare.Eq, 3),    3),   // Id 2,5,7 (NULLs excluded: null==3 is false)
                ("N != 3",          IrStep.WhereNullable(IrCompare.Ne, 3),    4),   // Id 1,3,4,6 — C# null!=3 is TRUE, so NULLs are INCLUDED (EF-parity; a bare SQL `N<>3` would wrongly give 1)
                ("N > 3",           IrStep.WhereNullable(IrCompare.Gt, 3),    1),   // Id 4
                ("N <= 3",          IrStep.WhereNullable(IrCompare.Le, 3),    3),   // Id 2,5,7
                ("N > 99",          IrStep.WhereNullable(IrCompare.Gt, 99),   0),   // none
            };
            foreach (var (name, step, expected) in cases)
            {
                var ir = new QueryIr { Rows = Data, Steps = new[] { step } };
                var r = QueryIrDifferential.Execute(ir, seed: 3500);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{name}: {ir.Describe()}] classified {r.Outcome}: {r.Detail}");
                // Cross-check the oracle count itself, so a wrong shared expression can't make both sides agree.
                Assert.Equal(expected, Data.Count(BuildOraclePredicate(step)));
            }

            // `N == 3` (3 rows) and `N != 3` (4 rows) sum to 7 = every row, because C# includes NULLs in `!=`
            // (null != 3 is true) but excludes them from `==`. nORM matches this (EF-parity null semantics) — a
            // naive `N <> 3` in SQL would drop the 3 NULL rows and desync from LINQ. That divergence is the trap.
            Assert.Contains("where-nullable", QueryIrDifferential.ExtractFeatures(new QueryIr { Rows = Data, Steps = new[] { IrStep.WhereNullable(IrCompare.Gt, 1) } }));
            Assert.Contains("where-null-check", QueryIrDifferential.ExtractFeatures(new QueryIr { Rows = Data, Steps = new[] { IrStep.WhereNullable(IrCompare.Eq, null) } }));

            // NULL dedup through a set operation: EXCEPT of the table with its NULL-and-3 rows removed leaves the
            // rest; NULLs must dedup as equal (SQLite treats NULLs as not distinct in set ops), matching the oracle.
            var setopNullable = new QueryIr
            {
                Rows = Data, Steps = Array.Empty<IrStep>(),
                SetOp = new IrSetOp { Kind = IrSetOpKind.Except, RightWheres = new[] { IrStep.WhereNullable(IrCompare.Eq, null) } },
            };
            Assert.Equal(FuzzOutcome.Executed, QueryIrDifferential.Execute(setopNullable, seed: 3501).Outcome);
        }

        private static Func<IrRow, bool> BuildOraclePredicate(IrStep step) => step.NullLiteral
            ? (step.Op == IrCompare.Ne ? r => r.N != null : r => r.N == null)
            : step.Op switch
            {
                IrCompare.Eq => r => r.N == step.Value,
                IrCompare.Ne => r => r.N != step.Value,
                IrCompare.Lt => r => r.N < step.Value,
                IrCompare.Le => r => r.N <= step.Value,
                IrCompare.Gt => r => r.N > step.Value,
                _ => r => r.N >= step.Value,
            };

        [Fact]
        public void Grouped_over_a_setop_matches_the_linq_oracle()
        {
            // GroupBy over each set operation must aggregate the combined rows and match the oracle (nORM wraps the
            // compound as a derived table). Both arms unfiltered on one axis exercises the historically fragile shape.
            foreach (var kind in new[] { IrSetOpKind.Union, IrSetOpKind.Concat, IrSetOpKind.Intersect, IrSetOpKind.Except })
            foreach (var agg in new[] { IrAggregate.Count, IrAggregate.Sum, IrAggregate.Max })
            {
                var ir = new QueryIr
                {
                    Rows = Data,
                    Steps = Array.Empty<IrStep>(),
                    SetOp = new IrSetOp { Kind = kind, RightWheres = new[] { IrStep.Where(IrColumn.A, IrCompare.Ge, 3) } },
                    GroupBy = new IrGroupBy { Key = IrColumn.A, Aggregate = agg, AggregateColumn = IrColumn.B },
                };
                var r = QueryIrDifferential.Execute(ir, seed: 5100);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }

            Assert.Contains("groupby+setop", QueryIrDifferential.ExtractFeatures(new QueryIr
            {
                Rows = Data,
                Steps = Array.Empty<IrStep>(),
                SetOp = new IrSetOp { Kind = IrSetOpKind.Concat, RightWheres = Array.Empty<IrStep>() },
                GroupBy = new IrGroupBy { Key = IrColumn.A },
            }));
        }

        [Fact]
        public void String_equality_is_ordinal_and_case_sensitive()
        {
            // Mixed-case names: an ordinal "Name == a" matches only the lowercase rows, never "A". A case-
            // insensitive translation in nORM would diverge from the ordinal LINQ-to-Objects oracle.
            var rows = new[]
            {
                new IrRow { Id = 1, A = 1, B = 1, Name = "a" },
                new IrRow { Id = 2, A = 2, B = 2, Name = "A" },
                new IrRow { Id = 3, A = 3, B = 3, Name = "b" },
                new IrRow { Id = 4, A = 4, B = 4, Name = "a" },
            };
            foreach (var (op, text) in new[]
                { (IrStringOp.Eq, "a"), (IrStringOp.Ne, "a"), (IrStringOp.Eq, "A"), (IrStringOp.Eq, "b") })
            {
                var ir = new QueryIr { Rows = rows, Steps = new[] { IrStep.WhereName(op, text) } };
                var r = QueryIrDifferential.Execute(ir, seed: 4100);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }
            Assert.Contains("where-name", QueryIrDifferential.ExtractFeatures(
                new QueryIr { Rows = rows, Steps = new[] { IrStep.WhereName(IrStringOp.Eq, "a") } }));
        }

        [Fact]
        public void String_contains_is_ordinal_and_case_sensitive()
        {
            // Two-char mixed-case names: Contains("a") matches only names with a lowercase a, never the uppercase.
            // string.Contains(string) is ordinal in C#, so a case-insensitive nORM translation would diverge.
            var rows = new[]
            {
                new IrRow { Id = 1, A = 1, B = 1, Name = "ab" },
                new IrRow { Id = 2, A = 2, B = 2, Name = "Ab" },
                new IrRow { Id = 3, A = 3, B = 3, Name = "aB" },
                new IrRow { Id = 4, A = 4, B = 4, Name = "AB" },
            };
            foreach (var needle in new[] { "a", "A", "b", "B", "ab", "AB", "x" })
            {
                var ir = new QueryIr { Rows = rows, Steps = new[] { IrStep.WhereName(IrStringOp.Contains, needle) } };
                var r = QueryIrDifferential.Execute(ir, seed: 4300);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }
        }

        [Fact]
        public void String_prefix_and_suffix_are_ordinal_and_case_sensitive()
        {
            // StartsWith/EndsWith(string) are culture-sensitive in C#, so the fuzzer uses the explicit-Ordinal
            // overload on both sides; nORM honours the StringComparison, so "ab".StartsWith("A") is false (ordinal).
            var rows = new[]
            {
                new IrRow { Id = 1, A = 1, B = 1, Name = "ab" },
                new IrRow { Id = 2, A = 2, B = 2, Name = "Ab" },
                new IrRow { Id = 3, A = 3, B = 3, Name = "aB" },
                new IrRow { Id = 4, A = 4, B = 4, Name = "AB" },
            };
            foreach (var op in new[] { IrStringOp.StartsWith, IrStringOp.EndsWith })
            foreach (var needle in new[] { "a", "A", "b", "B" })
            {
                var ir = new QueryIr { Rows = rows, Steps = new[] { IrStep.WhereName(op, needle) } };
                var r = QueryIrDifferential.Execute(ir, seed: 4400);
                Assert.True(r.Outcome == FuzzOutcome.Executed, $"[{ir.Describe()}] classified {r.Outcome}: {r.Detail}");
            }
        }
    }
}
