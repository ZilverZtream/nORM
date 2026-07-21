using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Coverage sweep for the caller-owned-transaction write path — the interaction class that produced the
    /// recent data-loss fixes (client-key re-insert, repeated-edit lost update, OCC token) and that the
    /// implicit-per-save families cannot reach. Each seed generates an interleaved insert/update/delete/save
    /// scenario wrapped in committed and rolled-back transactions and runs it through the snapshot-diff oracle;
    /// the authoritative committed database must equal the reference model for every one.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class WriteTransactionSweepTests
    {
        [Fact]
        public void Generated_transactional_scenarios_match_the_reference_model()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "crud-tx-sweep", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });

            for (var seed = 4_000_000; seed < 4_001_500; seed++)
            {
                var scenario = WriteScenarioGenerator.GenerateTransactional(seed);
                manifest.Record(WriteScenarioDifferential.Execute(scenario, seed));
            }

            Assert.True(manifest.IsClean(),
                "caller-transaction sweep diverged:\n" +
                string.Join("\n", manifest.DedupedFailures().Take(10).Select(f => $"  seed={f.Seed} {f.Outcome}: {f.Detail}\n    {f.SerializedCase}")));

            // The sweep must actually exercise transactions, not degenerate into plain CRUD.
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("tx-commit", frontier);
            Assert.Contains("tx-rollback", frontier);
        }
    }
}
