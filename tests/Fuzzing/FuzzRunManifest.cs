using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Thread-safe accumulator for a fuzz run. Fuzzers record every case; the manifest tallies per-family
    /// outcomes, tracks the semantic-feature frontier (for coverage-frontier gating), de-duplicates failures
    /// by <see cref="FuzzCaseResult.FailureSignature"/>, and emits a machine-readable JSON artifact for CI.
    /// A green run is one where every family stays within its support-contract budget AND the feature
    /// frontier has not shrunk — not merely one where nothing was silently wrong.
    /// </summary>
    public sealed class FuzzRunManifest
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, Dictionary<FuzzOutcome, long>> _byFamily = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<string>> _reasonCodesByFamily = new(StringComparer.Ordinal);
        private readonly HashSet<string> _featureFrontier = new(StringComparer.Ordinal);
        private readonly Dictionary<string, FuzzCaseResult> _dedupedFailures = new(StringComparer.Ordinal);

        /// <summary>Metadata identifying the environment this run reproduces against.</summary>
        public FuzzRunEnvironment Environment { get; }

        public FuzzRunManifest(FuzzRunEnvironment environment) => Environment = environment;

        /// <summary>Records one case. Safe to call concurrently from parallel fuzz workers.</summary>
        public void Record(FuzzCaseResult result)
        {
            lock (_gate)
            {
                if (!_byFamily.TryGetValue(result.Family, out var tally))
                    _byFamily[result.Family] = tally = new Dictionary<FuzzOutcome, long>();
                tally[result.Outcome] = tally.GetValueOrDefault(result.Outcome) + 1;

                if (result.ReasonCode is { Length: > 0 } code)
                {
                    if (!_reasonCodesByFamily.TryGetValue(result.Family, out var codes))
                        _reasonCodesByFamily[result.Family] = codes = new HashSet<string>(StringComparer.Ordinal);
                    codes.Add(code);
                }

                foreach (var feature in result.Features)
                    _featureFrontier.Add(feature);

                // Keep one representative artifact per distinct failure signature so a million seeds that hit
                // one defect produce one durable case, not a million.
                if (result.Outcome.IsFailure())
                    _dedupedFailures.TryAdd(result.FailureSignature(), result);
            }
        }

        /// <summary>Per-family attempted/outcome tallies and derived rates, for gating and reporting.</summary>
        public IReadOnlyList<FuzzFamilyStats> FamilyStats()
        {
            lock (_gate)
            {
                return _byFamily.Select(kv =>
                {
                    var counts = kv.Value;
                    long attempted = counts.Values.Sum();
                    long executed = counts.GetValueOrDefault(FuzzOutcome.Executed);
                    long correctlyRejected = counts.GetValueOrDefault(FuzzOutcome.CorrectlyRejected);
                    long failures = counts.Where(c => c.Key.IsFailure()).Sum(c => c.Value);
                    return new FuzzFamilyStats(
                        Family: kv.Key,
                        Attempted: attempted,
                        Executed: executed,
                        CorrectlyRejected: correctlyRejected,
                        Failures: failures,
                        UnsupportedRate: attempted == 0 ? 0d : (double)correctlyRejected / attempted,
                        Outcomes: counts.ToDictionary(c => c.Key, c => c.Value),
                        ReasonCodes: _reasonCodesByFamily.TryGetValue(kv.Key, out var rc)
                            ? rc.OrderBy(x => x, StringComparer.Ordinal).ToArray()
                            : Array.Empty<string>());
                }).OrderBy(s => s.Family, StringComparer.Ordinal).ToArray();
            }
        }

        /// <summary>The union of every semantic feature any case exercised — the coverage frontier.</summary>
        public IReadOnlyCollection<string> FeatureFrontier()
        {
            lock (_gate) return _featureFrontier.OrderBy(x => x, StringComparer.Ordinal).ToArray();
        }

        /// <summary>One representative case per distinct failure — the durable artifacts to serialize.</summary>
        public IReadOnlyCollection<FuzzCaseResult> DedupedFailures()
        {
            lock (_gate) return _dedupedFailures.Values.ToArray();
        }

        /// <summary>True when no recorded case is a failure outcome.</summary>
        public bool IsClean()
        {
            lock (_gate) return _dedupedFailures.Count == 0;
        }

        /// <summary>Serializes the manifest to indented JSON for CI publication.</summary>
        public string ToJson()
        {
            var doc = new ManifestDto
            {
                Environment = Environment,
                Families = FamilyStats().Select(s => new FamilyDto
                {
                    Family = s.Family,
                    Attempted = s.Attempted,
                    Executed = s.Executed,
                    CorrectlyRejected = s.CorrectlyRejected,
                    Failures = s.Failures,
                    UnsupportedRate = Math.Round(s.UnsupportedRate, 6),
                    Outcomes = s.Outcomes.ToDictionary(o => o.Key.ToString(), o => o.Value),
                    ReasonCodes = s.ReasonCodes.ToArray(),
                }).ToArray(),
                FeatureFrontier = FeatureFrontier().ToArray(),
                Failures = DedupedFailures().Select(f => new FailureDto
                {
                    Family = f.Family,
                    Seed = f.Seed,
                    GeneratorVersion = f.GeneratorVersion,
                    Outcome = f.Outcome.ToString(),
                    ReasonCode = f.ReasonCode,
                    Signature = f.FailureSignature(),
                    Detail = f.Detail,
                    Sql = f.Sql,
                    SerializedCase = f.SerializedCase,
                }).ToArray(),
            };
            return JsonSerializer.Serialize(doc, ManifestJsonOptions);
        }

        internal static readonly JsonSerializerOptions ManifestJsonOptions = new()
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            // Feature tags contain '+' (plan-feature tuples) and other symbols; the relaxed encoder keeps the
            // manifest human-readable (set-op+distinct, not set-op+distinct) while staying valid JSON.
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };

        private sealed class ManifestDto
        {
            public FuzzRunEnvironment Environment { get; set; } = null!;
            public FamilyDto[] Families { get; set; } = Array.Empty<FamilyDto>();
            public string[] FeatureFrontier { get; set; } = Array.Empty<string>();
            public FailureDto[] Failures { get; set; } = Array.Empty<FailureDto>();
        }

        private sealed class FamilyDto
        {
            public string Family { get; set; } = "";
            public long Attempted { get; set; }
            public long Executed { get; set; }
            public long CorrectlyRejected { get; set; }
            public long Failures { get; set; }
            public double UnsupportedRate { get; set; }
            public Dictionary<string, long> Outcomes { get; set; } = new();
            public string[] ReasonCodes { get; set; } = Array.Empty<string>();
        }

        private sealed class FailureDto
        {
            public string Family { get; set; } = "";
            public long Seed { get; set; }
            public int GeneratorVersion { get; set; }
            public string Outcome { get; set; } = "";
            public string? ReasonCode { get; set; }
            public string Signature { get; set; } = "";
            public string? Detail { get; set; }
            public string? Sql { get; set; }
            public string SerializedCase { get; set; } = "";
        }
    }

    /// <summary>Environment metadata that, together with a serialized case, makes a failure reproducible.</summary>
    public sealed record FuzzRunEnvironment
    {
        public required string RunId { get; init; }
        public required string CommitSha { get; init; }
        public required string Runtime { get; init; }
        public required string Os { get; init; }
        public string? Provider { get; init; }
        public string? ServerVersion { get; init; }
        public required string TimestampUtc { get; init; }
    }

    /// <summary>Derived per-family statistics for gating and reporting.</summary>
    public sealed record FuzzFamilyStats(
        string Family,
        long Attempted,
        long Executed,
        long CorrectlyRejected,
        long Failures,
        double UnsupportedRate,
        IReadOnlyDictionary<FuzzOutcome, long> Outcomes,
        IReadOnlyList<string> ReasonCodes);
}
