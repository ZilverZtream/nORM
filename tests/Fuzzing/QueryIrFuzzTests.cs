using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// The coverage-guided differential loop: generate query-IR cases, run each against nORM and the
    /// LINQ-to-Objects oracle, retain the cases that grow the semantic-feature frontier into a corpus, and — on
    /// any divergence — auto-minimize it to a compact regression. This is the working core of the fuzzing
    /// vision: exploration measured by coverage, not seed volume, with automatic shrinking.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class QueryIrFuzzTests
    {
        private static FuzzRunEnvironment Env() => new()
        {
            RunId = "ir-fuzz", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
        };

        [Fact]
        public void Coverage_guided_sweep_agrees_with_the_oracle_and_builds_a_corpus()
        {
            var manifest = new FuzzRunManifest(Env());
            var seen = new HashSet<string>(StringComparer.Ordinal);
            var corpus = new List<QueryIr>();

            for (var seed = 0; seed < 400; seed++)
            {
                var ir = QueryIrGenerator.Generate(seed);
                var result = QueryIrDifferential.Execute(ir, seed);
                manifest.Record(result);
                if (result.Features.Any(f => seen.Add(f)))   // coverage-increasing -> keep in corpus
                    corpus.Add(ir);
            }

            var failures = manifest.DedupedFailures();
            Assert.True(failures.Count == 0,
                "coverage-guided differential found a divergence in nORM's query translation:\n" +
                string.Join("\n", failures.Select(f => $"  {f.Outcome}: {f.Detail}\n  case: {f.SerializedCase}")));

            Assert.True(corpus.Count >= 4, $"corpus should capture diverse coverage, got {corpus.Count}");
            Assert.True(manifest.FeatureFrontier().Count >= 6, "the sweep should exercise several plan features");
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("setop", frontier);
            Assert.Contains("setop+orderby", frontier);       // the shape that previously produced invalid SQL
            Assert.Contains("projection", frontier);
            Assert.Contains("setop+projection", frontier);    // the shape whose Distinct previously did not dedup
        }

        [Fact]
        public void Mutation_of_corpus_cases_stays_correct()
        {
            // Structure-aware mutation of valid cases must not break correctness either.
            var manifest = new FuzzRunManifest(Env());
            var ir = QueryIrGenerator.Generate(7);
            for (var i = 0; i < 150; i++)
            {
                ir = QueryIrGenerator.Mutate(ir, i);
                manifest.Record(QueryIrDifferential.Execute(ir, 10_000 + i));
                if (ir.Steps.Count > 8 || ir.Rows.Count > 12) ir = QueryIrGenerator.Generate(i); // keep cases bounded
            }
            Assert.True(manifest.IsClean(),
                "mutation sweep found a divergence:\n" +
                string.Join("\n", manifest.DedupedFailures().Select(f => $"  {f.Outcome}: {f.Detail} | {f.SerializedCase}")));
        }

        [Fact]
        public void Ir_shrinker_reduces_a_failing_case_to_its_essential_step_and_row()
        {
            // Bigger case: 4 steps, 6 rows; only Id=4 carries A==99.
            var big = new QueryIr
            {
                Rows = Enumerable.Range(1, 6)
                    .Select(i => new IrRow { Id = i, A = i == 4 ? 99 : i, B = i, Name = "x" })
                    .ToList(),
                Steps = new[]
                {
                    IrStep.Where(IrColumn.A, IrCompare.Gt, 0),
                    IrStep.OrderBy(IrColumn.A),
                    IrStep.Where(IrColumn.B, IrCompare.Lt, 3),
                    IrStep.Take(2),
                },
            };

            // Synthetic defect: diverges only when a Where-on-B is present AND a row with A==99 exists.
            static FuzzCaseResult Synthetic(QueryIr ir)
            {
                var fails = ir.Steps.Any(s => s.Kind == IrStepKind.Where && s.Column == IrColumn.B)
                            && ir.Rows.Any(r => r.A == 99);
                return new FuzzCaseResult
                {
                    Family = "query-ir", Seed = 1, GeneratorVersion = 1,
                    Outcome = fails ? FuzzOutcome.WrongResult : FuzzOutcome.Executed,
                    Detail = fails ? "synthetic-defect" : null,
                    SerializedCase = ir.ToJson(),
                };
            }

            var min = QueryIrDifferential.ShrinkPreservingFailure(big, Synthetic);

            var step = Assert.Single(min.Steps);
            Assert.Equal(IrStepKind.Where, step.Kind);
            Assert.Equal(IrColumn.B, step.Column);
            var row = Assert.Single(min.Rows);
            Assert.Equal(99, row.A);
            Assert.Equal(FuzzOutcome.WrongResult, Synthetic(min).Outcome);   // still reproduces
        }
    }
}
