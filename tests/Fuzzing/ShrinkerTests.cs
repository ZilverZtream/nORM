using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Proves the shrinkers reduce a large failing case to a minimal one while preserving the specific
    /// failure, and that the sequence reducer is wired to the measurement core's failure signature so a
    /// minimized case provably reproduces the ORIGINAL defect rather than some new one exposed by removal.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class ShrinkerTests
    {
        [Fact]
        public void Sequence_reducer_finds_the_minimal_failing_subset()
        {
            var original = Enumerable.Range(1, 12).ToList();
            // Failure requires BOTH 3 and 7 present; everything else is irrelevant noise.
            var minimized = Shrinker.MinimizeSequence(original, s => s.Contains(3) && s.Contains(7));
            Assert.Equal(new[] { 3, 7 }, minimized);
        }

        [Fact]
        public void Greedy_minimizer_reduces_a_constant_to_its_boundary()
        {
            // Candidates smallest-first; failure holds while n > 5, so the minimum still-failing value is 6.
            IEnumerable<int> Reductions(int n) => new[] { 0, n / 2, n - 1 }.Where(c => c >= 0 && c < n).Distinct();
            var minimized = Shrinker.Minimize(100, Reductions, n => n > 5);
            Assert.Equal(6, minimized);
        }

        [Fact]
        public void Sequence_reducer_preserves_the_failure_signature_and_strips_noise()
        {
            // A synthetic CRUD history: the defect is an orphaned B, triggered only when both "attach-B" and
            // "remove-A" appear. save/noop/reparent-B are irrelevant to THIS failure and must be stripped.
            var history = new[] { "noop", "attach-A", "attach-B", "save", "reparent-B", "remove-A", "noop", "save" };

            FuzzCaseResult Execute(IReadOnlyList<string> ops)
            {
                var fails = ops.Contains("attach-B") && ops.Contains("remove-A");
                return new FuzzCaseResult
                {
                    Family = "crud",
                    Seed = 1,
                    GeneratorVersion = 1,
                    Outcome = fails ? FuzzOutcome.WrongResult : FuzzOutcome.Executed,
                    Detail = fails ? "orphaned-B" : null,
                    SerializedCase = string.Join(",", ops),
                };
            }

            var minimized = Shrinker.MinimizeSequencePreservingFailure(history, Execute);

            Assert.Equal(new[] { "attach-B", "remove-A" }, minimized);
            // The minimized case still reproduces the exact original failure.
            var result = Execute(minimized);
            Assert.Equal(FuzzOutcome.WrongResult, result.Outcome);
            Assert.Equal("crud:WrongResult:orphaned-B", result.FailureSignature());
        }

        [Fact]
        public void Preserving_shrink_rejects_a_non_failing_input()
        {
            FuzzCaseResult Execute(IReadOnlyList<int> ops) => new()
            {
                Family = "x", Seed = 1, GeneratorVersion = 1,
                Outcome = FuzzOutcome.Executed, SerializedCase = "{}",
            };
            Assert.Throws<ArgumentException>(() =>
                Shrinker.MinimizeSequencePreservingFailure(new[] { 1, 2, 3 }, Execute));
        }

        [Fact]
        public void Sequence_reducer_handles_duplicate_elements_without_over_removing()
        {
            // Two distinct "5" markers matter; the reducer must not collapse both when removing one chunk.
            var original = new List<int> { 5, 1, 2, 5, 3, 4 };
            // Failure needs at least two 5s present.
            var minimized = Shrinker.MinimizeSequence(original, s => s.Count(x => x == 5) >= 2);
            Assert.Equal(2, minimized.Count(x => x == 5));
            Assert.True(minimized.Count <= original.Count);
        }
    }
}
