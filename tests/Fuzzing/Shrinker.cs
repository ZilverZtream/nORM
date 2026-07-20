using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Structural shrinkers that turn a large failing case ("seed 602775 fails after a long sequence") into a
    /// minimal one ("Add A, attach B, re-parent B, remove A, save"). Reduction is failure-preserving: a
    /// candidate is only adopted when it still reproduces the SAME failure, identified by its stable signature
    /// rather than by any coincidental later crash. Minimized cases become clear, self-contained regressions.
    /// </summary>
    public static class Shrinker
    {
        /// <summary>
        /// Greedy fix-point minimizer for tree-shaped cases (expression trees, schemas, values): repeatedly
        /// adopt the first candidate reduction that still reproduces the failure, until no reduction does.
        /// <paramref name="reductions"/> should yield simpler variants (smallest-first for best results):
        /// replacing a binary node with an operand, dropping a query layer, shrinking a constant toward a
        /// boundary, reducing navigation depth. Returns a locally-minimal case.
        /// </summary>
        public static T Minimize<T>(T failing, Func<T, IEnumerable<T>> reductions, Func<T, bool> stillReproduces,
            int maxSteps = 10_000)
        {
            var current = failing;
            for (var step = 0; step < maxSteps; step++)
            {
                var progressed = false;
                foreach (var candidate in reductions(current))
                {
                    if (stillReproduces(candidate))
                    {
                        current = candidate;
                        progressed = true;
                        break;
                    }
                }
                if (!progressed)
                    return current;
            }
            return current;
        }

        /// <summary>
        /// Delta-debugging (Zeller ddmin) for operation histories and row/column sets: finds a
        /// 1-minimal sub-sequence that still reproduces the failure by removing progressively finer chunks.
        /// Use for state-machine histories ("delete chunks of operations"), datasets ("delta-debug rows"),
        /// and fault schedules ("remove injected faults not required").
        /// </summary>
        public static IReadOnlyList<T> MinimizeSequence<T>(
            IReadOnlyList<T> failing,
            Func<IReadOnlyList<T>, bool> stillReproduces,
            int maxProbes = 100_000)
        {
            var current = failing.ToList();
            var granularity = 2;
            var probes = 0;

            while (current.Count >= 2)
            {
                var chunkSize = Math.Max(1, current.Count / granularity);
                var chunks = Chunk(current, chunkSize).ToList();
                var reduced = false;

                // Try each subset alone (fastest path to a big reduction).
                foreach (var subset in chunks)
                {
                    if (++probes > maxProbes) return current;
                    if (subset.Count > 0 && subset.Count < current.Count && stillReproduces(subset))
                    {
                        current = subset;
                        granularity = 2;
                        reduced = true;
                        break;
                    }
                }
                if (reduced) continue;

                // Try each complement (remove one chunk at a time).
                foreach (var subset in chunks)
                {
                    if (++probes > maxProbes) return current;
                    var complement = current.Where(x => !subset.Contains(x)).ToList();
                    // Fall back to index-based removal so duplicate elements are not over-removed.
                    if (complement.Count == current.Count)
                        complement = RemoveRange(current, subset);
                    if (complement.Count > 0 && complement.Count < current.Count && stillReproduces(complement))
                    {
                        current = complement;
                        granularity = Math.Max(granularity - 1, 2);
                        reduced = true;
                        break;
                    }
                }
                if (reduced) continue;

                if (granularity >= current.Count)
                    break;
                granularity = Math.Min(current.Count, granularity * 2);
            }
            return current;
        }

        /// <summary>
        /// Failure-preserving sequence shrink wired to the measurement core: reduces the history while the
        /// executor keeps returning a result with the SAME <see cref="FuzzCaseResult.FailureSignature"/>, so
        /// the minimized case provably reproduces the original defect (not some new one uncovered by removal).
        /// </summary>
        public static IReadOnlyList<T> MinimizeSequencePreservingFailure<T>(
            IReadOnlyList<T> failing,
            Func<IReadOnlyList<T>, FuzzCaseResult> execute)
        {
            var target = execute(failing);
            if (!target.Outcome.IsFailure())
                throw new ArgumentException("The case to shrink must reproduce a failure.", nameof(failing));
            var targetSignature = target.FailureSignature();
            return MinimizeSequence(failing, candidate =>
            {
                var r = execute(candidate);
                return r.Outcome.IsFailure() && r.FailureSignature() == targetSignature;
            });
        }

        private static IEnumerable<List<T>> Chunk<T>(IReadOnlyList<T> items, int size)
        {
            for (var i = 0; i < items.Count; i += size)
                yield return items.Skip(i).Take(size).ToList();
        }

        private static List<T> RemoveRange<T>(List<T> source, List<T> toRemove)
        {
            // Index-based removal: drop the contiguous span occupied by the chunk, robust to duplicate values.
            var start = source.Count;
            for (var i = 0; i + toRemove.Count <= source.Count; i++)
            {
                if (source.Skip(i).Take(toRemove.Count).SequenceEqual(toRemove))
                {
                    start = i;
                    break;
                }
            }
            if (start == source.Count)
                return source;
            var result = new List<T>(source);
            result.RemoveRange(start, toRemove.Count);
            return result;
        }
    }
}
