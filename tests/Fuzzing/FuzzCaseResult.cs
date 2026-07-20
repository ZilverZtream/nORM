using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// The durable, self-contained record of one fuzz case execution. A seed alone is NOT a reproduction
    /// contract — changing one <c>rng.Next()</c> call shifts every later choice — so a failing case carries
    /// its <see cref="SerializedCase"/> (the replayable artifact) plus the metadata needed to reproduce the
    /// exact environment: generator version, runtime, provider, and server version. Failures are serialized
    /// to disk beside a machine-readable run manifest.
    /// </summary>
    public sealed record FuzzCaseResult
    {
        /// <summary>The generator family this case came from (e.g. "linq-parity", "crud-state-machine").</summary>
        public required string Family { get; init; }

        /// <summary>The seed that produced the case. Metadata only — reproduction is via <see cref="SerializedCase"/>.</summary>
        public required long Seed { get; init; }

        /// <summary>Monotonic generator version; a case is only replayable-by-seed against the same version.</summary>
        public required int GeneratorVersion { get; init; }

        /// <summary>The classified outcome.</summary>
        public required FuzzOutcome Outcome { get; init; }

        /// <summary>
        /// For <see cref="FuzzOutcome.CorrectlyRejected"/> / <see cref="FuzzOutcome.UnexpectedlyRejected"/>,
        /// the stable reason code of the unsupported-feature signal (never the exception message text, which
        /// is not a stable contract). Null for other outcomes.
        /// </summary>
        public string? ReasonCode { get; init; }

        /// <summary>The replayable artifact: a serialized description of schema + data + query/command + options sufficient to reproduce the case independently of the seed.</summary>
        public required string SerializedCase { get; init; }

        /// <summary>The SQL nORM emitted, when the case reached translation. Aids diagnosis and dedup by plan signature.</summary>
        public string? Sql { get; init; }

        /// <summary>A short human summary of the failure (exception type, or oracle-mismatch shape). Not a stable contract.</summary>
        public string? Detail { get; init; }

        /// <summary>Wall-clock milliseconds the case took (translation + execution). Feeds the complexity/limit checks.</summary>
        public double ElapsedMs { get; init; }

        /// <summary>Semantic feature tags this case exercised (translator branches, plan-feature tuples, provider fallbacks). Drives coverage-guided retention and the coverage-frontier gate.</summary>
        public IReadOnlyList<string> Features { get; init; } = Array.Empty<string>();

        /// <summary>A stable signature for de-duplicating failures: outcome + reason/plan shape, independent of the specific seed. Two seeds that hit the same defect collapse to one artifact.</summary>
        public string FailureSignature()
        {
            var key = Outcome switch
            {
                FuzzOutcome.CorrectlyRejected or FuzzOutcome.UnexpectedlyRejected => ReasonCode ?? "(no-code)",
                _ => Detail ?? "(no-detail)",
            };
            return $"{Family}:{Outcome}:{key}";
        }
    }
}
