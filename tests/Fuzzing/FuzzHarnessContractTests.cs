using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Proves the Phase-1 measurement core has teeth: the outcome taxonomy, the run manifest, and the
    /// support-contract gate must classify outcomes correctly, dedup failures, track the coverage frontier,
    /// and FAIL a run on a silent defect, an undocumented rejection, an unsupported-rate breach, or a
    /// shrunken frontier — while passing a healthy run. A green fuzz run must mean "correct AND still capable".
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class FuzzHarnessContractTests
    {
        private const int GenVersion = 1;

        private static FuzzRunEnvironment Env() => new()
        {
            RunId = "test-run",
            CommitSha = "deadbeef",
            Runtime = ".NET 8",
            Os = "test",
            TimestampUtc = "2020-01-01T00:00:00Z",
        };

        private static FuzzCaseResult Case(FuzzOutcome outcome, string family = "linq", string? reason = null,
            string? detail = null, IReadOnlyList<string>? features = null) => new()
        {
            Family = family,
            Seed = 1,
            GeneratorVersion = GenVersion,
            Outcome = outcome,
            ReasonCode = reason,
            Detail = detail,
            SerializedCase = "{}",
            Features = features ?? Array.Empty<string>(),
        };

        [Fact]
        public void Only_executed_and_documented_rejection_are_healthy()
        {
            Assert.False(FuzzOutcome.Executed.IsFailure());
            Assert.False(FuzzOutcome.CorrectlyRejected.IsFailure());
            Assert.True(FuzzOutcome.UnexpectedlyRejected.IsFailure());
            Assert.True(FuzzOutcome.WrongResult.IsFailure());
            Assert.True(FuzzOutcome.WrongResult.IsSilentDefect());
            Assert.True(FuzzOutcome.NonDeterministic.IsSilentDefect());
            Assert.False(FuzzOutcome.UnexpectedException.IsSilentDefect());
        }

        [Fact]
        public void Contract_classifies_documented_vs_undocumented_rejections()
        {
            var contract = new FuzzSupportContract(new Dictionary<string, FuzzFamilyContract>
            {
                ["linq"] = new(new[] { "correlated-subquery-materialize" }, MaxUnsupportedRate: 0.5),
            });

            Assert.Equal(FuzzOutcome.CorrectlyRejected, contract.ClassifyRejection("linq", "correlated-subquery-materialize"));
            Assert.Equal(FuzzOutcome.UnexpectedlyRejected, contract.ClassifyRejection("linq", "some-new-refusal"));
            // A throw-site with no reason code yet yields the unclassified sentinel -> flagged, not hidden.
            Assert.Equal(FuzzOutcome.UnexpectedlyRejected, contract.ClassifyRejection("linq", null));
        }

        [Fact]
        public void Manifest_tallies_rates_and_dedups_failures()
        {
            var m = new FuzzRunManifest(Env());
            for (int i = 0; i < 6; i++) m.Record(Case(FuzzOutcome.Executed));
            for (int i = 0; i < 4; i++) m.Record(Case(FuzzOutcome.CorrectlyRejected, reason: "unsupported-x"));
            // Two distinct seeds hitting the SAME defect must collapse to one durable artifact.
            m.Record(Case(FuzzOutcome.WrongResult, detail: "count mismatch 3 vs 4") with { Seed = 100 });
            m.Record(Case(FuzzOutcome.WrongResult, detail: "count mismatch 3 vs 4") with { Seed = 200 });

            var stats = m.FamilyStats().Single();
            Assert.Equal(12, stats.Attempted);
            Assert.Equal(6, stats.Executed);
            Assert.Equal(4, stats.CorrectlyRejected);
            Assert.Equal(2, stats.Failures);
            Assert.Equal(4d / 12d, stats.UnsupportedRate, 6);
            Assert.Single(m.DedupedFailures());            // both WrongResult seeds -> one artifact
            Assert.False(m.IsClean());
        }

        [Fact]
        public void Manifest_tracks_the_feature_frontier_and_serializes()
        {
            var m = new FuzzRunManifest(Env());
            m.Record(Case(FuzzOutcome.Executed, features: new[] { "set-op+distinct", "converter-key" }));
            m.Record(Case(FuzzOutcome.Executed, features: new[] { "group-join+tenant" }));

            Assert.Equal(new[] { "converter-key", "group-join+tenant", "set-op+distinct" }, m.FeatureFrontier());
            var json = m.ToJson();
            Assert.Contains("\"FeatureFrontier\"", json);
            Assert.Contains("set-op+distinct", json);
            Assert.Contains("deadbeef", json);            // environment reproducibility metadata present
        }

        [Fact]
        public void Clean_run_within_budget_and_frontier_passes()
        {
            var contract = new FuzzSupportContract(
                new Dictionary<string, FuzzFamilyContract> { ["linq"] = new(new[] { "unsupported-x" }, 0.5) },
                requiredFeatures: new[] { "set-op+distinct" });

            var m = new FuzzRunManifest(Env());
            for (int i = 0; i < 8; i++) m.Record(Case(FuzzOutcome.Executed, features: new[] { "set-op+distinct" }));
            for (int i = 0; i < 2; i++) m.Record(Case(FuzzOutcome.CorrectlyRejected, reason: "unsupported-x"));

            Assert.Empty(contract.Evaluate(m));
        }

        [Fact]
        public void Gate_fails_on_silent_defect()
        {
            var contract = new FuzzSupportContract(new Dictionary<string, FuzzFamilyContract> { ["linq"] = new(Array.Empty<string>(), 1.0) });
            var m = new FuzzRunManifest(Env());
            m.Record(Case(FuzzOutcome.Executed));
            m.Record(Case(FuzzOutcome.WrongResult, detail: "row loss"));

            var v = contract.Evaluate(m);
            Assert.Contains(v, x => x.Kind == FuzzViolationKind.SilentDefect);
        }

        [Fact]
        public void Gate_fails_on_undocumented_rejection_even_with_no_wrong_results()
        {
            var contract = new FuzzSupportContract(new Dictionary<string, FuzzFamilyContract> { ["linq"] = new(new[] { "known-code" }, 1.0) });
            var m = new FuzzRunManifest(Env());
            m.Record(Case(FuzzOutcome.Executed));
            // Reason code not in the contract -> the fuzzer recorded it as UnexpectedlyRejected.
            m.Record(Case(FuzzOutcome.UnexpectedlyRejected, reason: "newly-refused-shape"));

            var v = contract.Evaluate(m);
            Assert.Contains(v, x => x.Kind == FuzzViolationKind.UndocumentedRejection);
        }

        [Fact]
        public void Gate_fails_when_unsupported_rate_exceeds_ceiling()
        {
            var contract = new FuzzSupportContract(new Dictionary<string, FuzzFamilyContract> { ["linq"] = new(new[] { "unsupported-x" }, MaxUnsupportedRate: 0.10) });
            var m = new FuzzRunManifest(Env());
            for (int i = 0; i < 5; i++) m.Record(Case(FuzzOutcome.Executed));
            for (int i = 0; i < 5; i++) m.Record(Case(FuzzOutcome.CorrectlyRejected, reason: "unsupported-x")); // 50% >> 10%

            var v = contract.Evaluate(m);
            Assert.Contains(v, x => x.Kind == FuzzViolationKind.UnsupportedRateExceeded);
        }

        [Fact]
        public void Gate_fails_when_coverage_frontier_shrinks()
        {
            var contract = new FuzzSupportContract(
                new Dictionary<string, FuzzFamilyContract> { ["linq"] = new(Array.Empty<string>(), 1.0) },
                requiredFeatures: new[] { "set-op+distinct", "temporal+include" });

            var m = new FuzzRunManifest(Env());
            m.Record(Case(FuzzOutcome.Executed, features: new[] { "set-op+distinct" })); // temporal+include never hit

            var v = contract.Evaluate(m);
            var frontier = Assert.Single(v, x => x.Kind == FuzzViolationKind.CoverageFrontierShrank);
            Assert.Contains("temporal+include", frontier.Detail);
        }

        [Fact]
        public void Contract_json_round_trips()
        {
            var contract = new FuzzSupportContract(
                new Dictionary<string, FuzzFamilyContract> { ["linq"] = new(new[] { "a", "b" }, 0.25) },
                requiredFeatures: new[] { "set-op+distinct" });

            var round = FuzzSupportContract.FromJson(contract.ToJson());
            Assert.Equal(FuzzOutcome.CorrectlyRejected, round.ClassifyRejection("linq", "a"));
            Assert.Equal(FuzzOutcome.UnexpectedlyRejected, round.ClassifyRejection("linq", "c"));
            Assert.Contains("set-op+distinct", round.RequiredFeatures);
        }
    }
}
