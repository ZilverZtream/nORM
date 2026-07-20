using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Exercises the write-path snapshot-diff oracle on a battery of CRUD scenarios: insert-only, update,
    /// delete, batched inserts, an insert+update in one save, an insert+delete net no-op, and mixed multi-save
    /// histories. Each must persist the reference model exactly (verified against the raw-SQL DB state), so a
    /// lost/duplicated/corrupted row would surface as a WrongResult.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class WriteScenarioTests
    {
        private static WriteScenario S(params WriteOp[] ops) => new() { Ops = ops };

        public static IEnumerable<object[]> Scenarios()
        {
            var cases = new List<WriteScenario>
            {
                S(WriteOp.Insert(1, 10), WriteOp.Insert(2, 20), WriteOp.Save()),
                S(WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.Update(1, 15), WriteOp.Save()),
                S(WriteOp.Insert(1, 10), WriteOp.Insert(2, 20), WriteOp.Save(), WriteOp.Delete(1), WriteOp.Save()),
                S(WriteOp.Insert(1, 10), WriteOp.Update(1, 15), WriteOp.Save()),                 // insert+update, one save
                S(WriteOp.Insert(1, 10), WriteOp.Delete(1), WriteOp.Save()),                     // insert+delete net no-op
                S(WriteOp.Insert(1, 10), WriteOp.Insert(2, 20), WriteOp.Insert(3, 30), WriteOp.Save(),
                    WriteOp.Update(2, 25), WriteOp.Delete(1), WriteOp.Save()),                   // batched then mixed
                S(WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.Delete(1), WriteOp.Save(), WriteOp.Insert(1, 99), WriteOp.Save()), // delete then reinsert same id
                S(WriteOp.Insert(5, 50), WriteOp.Insert(6, 60), WriteOp.Update(5, 55), WriteOp.Delete(6), WriteOp.Save()), // all in one batch
            };
            return cases.Select((c, i) => new object[] { i, c });
        }

        [Theory]
        [MemberData(nameof(Scenarios))]
        public void Norm_persists_the_reference_model(int index, WriteScenario scenario)
        {
            var result = WriteScenarioDifferential.Execute(scenario, seed: 5000 + index);
            Assert.True(result.Outcome == FuzzOutcome.Executed,
                $"scenario #{index} [{scenario.Describe()}] classified {result.Outcome}: {result.Detail}");
        }

        [Fact]
        public void Battery_is_clean_and_covers_write_features()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "crud-battery", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });
            var i = 0;
            foreach (var row in Scenarios())
                manifest.Record(WriteScenarioDifferential.Execute((WriteScenario)row[1], seed: 6000 + i++));

            Assert.True(manifest.IsClean(), "the CRUD battery must have no failing outcomes");
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("insert", frontier);
            Assert.Contains("update", frontier);
            Assert.Contains("delete", frontier);
            Assert.Contains("batched-insert", frontier);
            Assert.Contains("insert-then-delete-in-batch", frontier);
        }

        [Fact]
        public void Coverage_guided_write_sweep_persists_the_model()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "crud-sweep", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });
            var seen = new HashSet<string>(System.StringComparer.Ordinal);
            var corpus = new List<WriteScenario>();

            for (var seed = 0; seed < 400; seed++)
            {
                var s = WriteScenarioGenerator.Generate(seed);
                var r = WriteScenarioDifferential.Execute(s, seed);
                manifest.Record(r);
                if (r.Features.Any(f => seen.Add(f)))
                    corpus.Add(s);
            }

            var failures = manifest.DedupedFailures();
            Assert.True(failures.Count == 0,
                "write-path sweep found a divergence between nORM and the reference model:\n" +
                string.Join("\n", failures.Select(f => $"  {f.Outcome}: {f.Detail}\n  case: {f.SerializedCase}")));
            Assert.True(corpus.Count >= 3, $"corpus should capture diverse write coverage, got {corpus.Count}");
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("insert", frontier);
            Assert.Contains("update", frontier);
            Assert.Contains("delete", frontier);
        }

        [Fact]
        public void Scenario_round_trips_through_json()
        {
            var s = S(WriteOp.Insert(1, 10), WriteOp.Update(1, 15), WriteOp.Save(), WriteOp.Delete(1), WriteOp.Save());
            var restored = WriteScenario.FromJson(s.ToJson());
            Assert.Equal(s.Describe(), restored.Describe());
            Assert.Equal(FuzzOutcome.Executed, WriteScenarioDifferential.Execute(restored, 1).Outcome);
        }
    }
}
