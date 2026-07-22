using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Exercises the optimistic-concurrency slice of the write-path snapshot-diff oracle on a battery of
    /// interleavings: the classic lost-update race (a stale writer's save must throw, not silently clobber), a
    /// non-contended two-row case, a same-writer re-save whose token refreshes, a fresh read after a concurrent
    /// commit that succeeds, and a no-op save. Each must classify as <see cref="FuzzOutcome.Executed"/> — the
    /// oracle asserts nORM throws <c>DbConcurrencyException</c> exactly when the model predicts a stale token and
    /// that the persisted state matches — so a silent lost update or a false conflict would surface as WrongResult.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class OccScenarioTests
    {
        private static OccScenario Occ(int writers, int[] ids, params OccOp[] ops) =>
            new() { WriterCount = writers, SeededIds = ids, Ops = ops };

        public static IEnumerable<object[]> Scenarios()
        {
            var one = new[] { 1 };
            var cases = new List<OccScenario>
            {
                // Classic lost update: both load, W0 commits, W1's stale save must be rejected.
                Occ(2, one,
                    OccOp.Load(0, 1), OccOp.Load(1, 1),
                    OccOp.Update(0, 1, 10), OccOp.Save(0),
                    OccOp.Update(1, 1, 20), OccOp.Save(1)),

                // Non-contended: two writers on two different rows both succeed.
                Occ(2, new[] { 1, 2 },
                    OccOp.Load(0, 1), OccOp.Load(1, 2),
                    OccOp.Update(0, 1, 5), OccOp.Save(0),
                    OccOp.Update(1, 2, 6), OccOp.Save(1)),

                // Same writer re-saves the same row: its token refreshes after the first commit, so the second succeeds.
                Occ(1, one,
                    OccOp.Load(0, 1),
                    OccOp.Update(0, 1, 5), OccOp.Save(0),
                    OccOp.Update(0, 1, 6), OccOp.Save(0)),

                // Fresh read after a concurrent commit: W1 loads AFTER W0 committed, so its token is current and it wins.
                Occ(2, one,
                    OccOp.Load(0, 1), OccOp.Update(0, 1, 5), OccOp.Save(0),
                    OccOp.Load(1, 1), OccOp.Update(1, 1, 7), OccOp.Save(1)),

                // A save with nothing pending is a no-op, even after a concurrent writer advanced the row.
                Occ(2, one,
                    OccOp.Load(0, 1), OccOp.Load(1, 1),
                    OccOp.Update(0, 1, 10), OccOp.Save(0),
                    OccOp.Save(1)),

                // Three writers contend: the first wins, the other two are both stale and must be rejected.
                Occ(3, one,
                    OccOp.Load(0, 1), OccOp.Load(1, 1), OccOp.Load(2, 1),
                    OccOp.Update(0, 1, 10), OccOp.Save(0),
                    OccOp.Update(1, 1, 20), OccOp.Save(1),
                    OccOp.Update(2, 1, 30), OccOp.Save(2)),
            };
            return cases.Select((c, i) => new object[] { i, c });
        }

        [Theory]
        [MemberData(nameof(Scenarios))]
        public void Norm_enforces_occ_and_matches_the_model(int index, OccScenario scenario)
        {
            var result = OccScenarioDifferential.Execute(scenario, seed: 7000 + index);
            Assert.True(result.Outcome == FuzzOutcome.Executed,
                $"scenario #{index} [{scenario.Describe()}] classified {result.Outcome}: {result.Detail}");
        }

        [Fact]
        public void Battery_is_clean_and_covers_occ_features()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "occ-battery", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });
            var i = 0;
            foreach (var row in Scenarios())
                manifest.Record(OccScenarioDifferential.Execute((OccScenario)row[1], seed: 8000 + i++));

            Assert.True(manifest.IsClean(), "the OCC battery must have no failing outcomes");
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("update", frontier);
            Assert.Contains("contended-row", frontier);
            Assert.Contains("competing-writes-same-row", frontier);
        }

        [Fact]
        public void Coverage_guided_occ_sweep_enforces_concurrency_and_persists_the_model()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "occ-sweep", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });
            var seen = new HashSet<string>(System.StringComparer.Ordinal);
            var corpus = new List<OccScenario>();

            for (var seed = 0; seed < 500; seed++)
            {
                var s = OccScenarioGenerator.Generate(seed);
                var r = OccScenarioDifferential.Execute(s, seed);
                manifest.Record(r);
                if (r.Features.Any(f => seen.Add(f)))
                    corpus.Add(s);
            }

            var failures = manifest.DedupedFailures();
            Assert.True(failures.Count == 0,
                "OCC sweep found a divergence between nORM and the reference model (silent lost update or false conflict):\n" +
                string.Join("\n", failures.Select(f => $"  {f.Outcome}: {f.Detail}\n  case: {f.SerializedCase}")));

            // The sweep must actually reach the contention interleavings, or it proves nothing.
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("contended-row", frontier);
            Assert.Contains("competing-writes-same-row", frontier);
        }

        [Fact]
        public void Scenario_round_trips_through_json()
        {
            var s = Occ(2, new[] { 1 },
                OccOp.Load(0, 1), OccOp.Load(1, 1),
                OccOp.Update(0, 1, 10), OccOp.Save(0),
                OccOp.Update(1, 1, 20), OccOp.Save(1));
            var restored = OccScenario.FromJson(s.ToJson());
            Assert.Equal(s.Describe(), restored.Describe());
            Assert.Equal(FuzzOutcome.Executed, OccScenarioDifferential.Execute(restored, 1).Outcome);
        }
    }
}
