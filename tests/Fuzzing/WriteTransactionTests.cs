using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Exercises the transaction invariant of the write-path oracle: a committed transaction's changes are
    /// durable, and a rolled-back transaction reverts the database to the state at BeginTx. The reference model
    /// snapshots at BeginTx and restores it on rollback; the persisted state (read via raw SQL) must match.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class WriteTransactionTests
    {
        private static WriteScenario S(params WriteOp[] ops) => new() { Ops = ops };

        public static IEnumerable<object[]> Scenarios()
        {
            var cases = new List<WriteScenario>
            {
                // Commit persists (implicit flush at Commit).
                S(WriteOp.BeginTx(), WriteOp.Insert(1, 10), WriteOp.Commit()),
                // Rollback reverts a tx-only insert.
                S(WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.BeginTx(), WriteOp.Insert(2, 20), WriteOp.Save(), WriteOp.Rollback()),
                // Rollback reverts an update back to the committed value.
                S(WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.BeginTx(), WriteOp.Update(1, 99), WriteOp.Save(), WriteOp.Rollback()),
                // Rollback reverts a delete (the row survives).
                S(WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.BeginTx(), WriteOp.Delete(1), WriteOp.Save(), WriteOp.Rollback()),
                // Commit an update inside a tx.
                S(WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.BeginTx(), WriteOp.Update(1, 99), WriteOp.Save(), WriteOp.Commit()),
                // Multiple ops in a committed tx (no mid-tx Save: Delete of a still-pending insert is a net no-op).
                S(WriteOp.BeginTx(), WriteOp.Insert(1, 10), WriteOp.Insert(2, 20), WriteOp.Delete(1), WriteOp.Commit()),
                // A rollback then a fresh committed transaction (the second must persist).
                S(WriteOp.BeginTx(), WriteOp.Insert(1, 10), WriteOp.Save(), WriteOp.Rollback()),
            };
            return cases.Select((c, i) => new object[] { i, c });
        }

        [Theory]
        [MemberData(nameof(Scenarios))]
        public void Commit_persists_and_rollback_reverts(int index, WriteScenario scenario)
        {
            var result = WriteScenarioDifferential.Execute(scenario, seed: 7000 + index);
            Assert.True(result.Outcome == FuzzOutcome.Executed,
                $"scenario #{index} [{scenario.Describe()}] classified {result.Outcome}: {result.Detail}");
        }

        [Fact]
        public void Transaction_battery_is_clean_and_covers_tx_features()
        {
            var manifest = new FuzzRunManifest(new FuzzRunEnvironment
            {
                RunId = "crud-tx", CommitSha = "local", Runtime = ".NET 8", Os = "test", TimestampUtc = "2020-01-01T00:00:00Z",
            });
            var i = 0;
            foreach (var row in Scenarios())
                manifest.Record(WriteScenarioDifferential.Execute((WriteScenario)row[1], seed: 8000 + i++));

            Assert.True(manifest.IsClean(),
                "transaction battery diverged:\n" + string.Join("\n", manifest.DedupedFailures().Select(f => $"  {f.Outcome}: {f.Detail} | {f.SerializedCase}")));
            var frontier = manifest.FeatureFrontier();
            Assert.Contains("tx-commit", frontier);
            Assert.Contains("tx-rollback", frontier);
        }
    }
}
