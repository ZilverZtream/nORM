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
        private static readonly IrRow[] Data =
        {
            new() { Id = 1, A = 8, B = 2, Name = "a" },
            new() { Id = 2, A = 3, B = 9, Name = "b" },
            new() { Id = 3, A = 7, B = 7, Name = "c" },
            new() { Id = 4, A = 1, B = 1, Name = "d" },
            new() { Id = 5, A = 3, B = 4, Name = "e" },
            new() { Id = 6, A = 6, B = 6, Name = "f" },
            new() { Id = 7, A = 3, B = 8, Name = "g" },
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
    }
}
