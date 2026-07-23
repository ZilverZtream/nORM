using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Exercises the many-to-many slice of the write-path snapshot-diff oracle: adding a link, removing a link,
    /// a mixed add+remove, a batch that toggles the same link to a net no-op, a multi-post edit, and a multi-save
    /// history. Each must persist the reference link set exactly (verified against the raw-SQL join table), so a
    /// dropped add/remove (the m2m_write_sync silent-no-op) or a duplicate join row would surface as WrongResult.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class M2mScenarioTests
    {
        private static M2mScenario M(int[] posts, int[] tags, (int p, int t)[] initial, params M2mOp[] ops) => new()
        {
            PostIds = posts,
            TagIds = tags,
            InitialLinks = initial.Select(l => new M2mLink { PostId = l.p, TagId = l.t }).ToList(),
            Ops = ops,
        };

        public static IEnumerable<object[]> Scenarios()
        {
            var cases = new List<M2mScenario>
            {
                // Add a link.
                M(new[] { 1 }, new[] { 1, 2 }, System.Array.Empty<(int, int)>(),
                    M2mOp.LoadPost(1), M2mOp.Link(1, 1), M2mOp.Save()),

                // Remove a link (the silent-no-op area).
                M(new[] { 1 }, new[] { 1, 2 }, new[] { (1, 1) },
                    M2mOp.LoadPost(1), M2mOp.Unlink(1, 1), M2mOp.Save()),

                // Add one, remove another, in one save.
                M(new[] { 1 }, new[] { 1, 2 }, new[] { (1, 1) },
                    M2mOp.LoadPost(1), M2mOp.Link(1, 2), M2mOp.Unlink(1, 1), M2mOp.Save()),

                // Toggle the same link within one batch = net no-op.
                M(new[] { 1 }, new[] { 1, 2 }, System.Array.Empty<(int, int)>(),
                    M2mOp.LoadPost(1), M2mOp.Link(1, 1), M2mOp.Unlink(1, 1), M2mOp.Save()),

                // Two posts edited independently.
                M(new[] { 1, 2 }, new[] { 1, 2, 3 }, new[] { (1, 1) },
                    M2mOp.LoadPost(1), M2mOp.LoadPost(2), M2mOp.Link(1, 2), M2mOp.Link(2, 3), M2mOp.Unlink(1, 1), M2mOp.Save()),

                // Multi-save: a second edit after the first commit.
                M(new[] { 1 }, new[] { 1, 2, 3 }, System.Array.Empty<(int, int)>(),
                    M2mOp.LoadPost(1), M2mOp.Link(1, 1), M2mOp.Save(), M2mOp.Link(1, 2), M2mOp.Unlink(1, 1), M2mOp.Save()),
            };
            return cases.Select((c, i) => new object[] { i, c });
        }

        [Theory]
        [MemberData(nameof(Scenarios))]
        public void Norm_persists_the_reference_link_set(int index, M2mScenario scenario)
        {
            var result = M2mScenarioDifferential.Execute(scenario, seed: 9000 + index);
            Assert.True(result.Outcome == FuzzOutcome.Executed,
                $"scenario #{index} [{scenario.Describe()}] classified {result.Outcome}: {result.Detail}");
        }

        [Fact]
        public void Coverage_guided_m2m_sweep_persists_the_link_set()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "m2m-sweep", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });
            var seen = new HashSet<string>(System.StringComparer.Ordinal);
            var corpus = new List<M2mScenario>();

            for (var seed = 0; seed < 500; seed++)
            {
                var s = M2mScenarioGenerator.Generate(seed);
                var r = M2mScenarioDifferential.Execute(s, seed);
                manifest.Record(r);
                if (r.Features.Any(f => seen.Add(f)))
                    corpus.Add(s);
            }

            var failures = manifest.DedupedFailures();
            Assert.True(failures.Count == 0,
                "M2M sweep found a divergence between nORM and the reference link set (dropped/duplicate join row):\n" +
                string.Join("\n", failures.Select(f => $"  {f.Outcome}: {f.Detail}\n  case: {f.SerializedCase}")));

            var frontier = manifest.FeatureFrontier();
            Assert.Contains("link", frontier);
            Assert.Contains("unlink", frontier);
        }

        [Fact]
        public void Scenario_round_trips_through_json()
        {
            var s = M(new[] { 1 }, new[] { 1, 2 }, new[] { (1, 1) },
                M2mOp.LoadPost(1), M2mOp.Link(1, 2), M2mOp.Unlink(1, 1), M2mOp.Save());
            var restored = M2mScenario.FromJson(s.ToJson());
            Assert.Equal(s.Describe(), restored.Describe());
            Assert.Equal(FuzzOutcome.Executed, M2mScenarioDifferential.Execute(restored, 1).Outcome);
        }
    }
}
