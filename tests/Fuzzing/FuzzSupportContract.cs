using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// The checked-in baseline that turns "unsupported" from a silent coverage sink into a gated contract.
    /// For each family it pins the set of reason codes nORM is ALLOWED to reject with and the maximum share
    /// of cases that may be rejected; a global feature list pins the coverage frontier. A run is a regression
    /// when it rejects with an unregistered reason code, exceeds a family's unsupported ceiling, or loses a
    /// required feature edge — even if nothing was silently wrong.
    /// </summary>
    public sealed class FuzzSupportContract
    {
        private readonly Dictionary<string, FuzzFamilyContract> _families;

        /// <summary>Features that must appear in every run's frontier; losing one means coverage silently shrank.</summary>
        public IReadOnlyCollection<string> RequiredFeatures { get; }

        public FuzzSupportContract(
            IReadOnlyDictionary<string, FuzzFamilyContract> families,
            IReadOnlyCollection<string>? requiredFeatures = null)
        {
            _families = new Dictionary<string, FuzzFamilyContract>(families, StringComparer.Ordinal);
            RequiredFeatures = requiredFeatures?.ToArray() ?? Array.Empty<string>();
        }

        /// <summary>
        /// Classifies a rejection: a documented reason code (registered for the family) is a healthy
        /// <see cref="FuzzOutcome.CorrectlyRejected"/>; anything else — an unknown code, or the
        /// <c>unclassified</c> sentinel a throw-site with no code yet produces — is
        /// <see cref="FuzzOutcome.UnexpectedlyRejected"/> and fails the run until the contract is updated.
        /// </summary>
        public FuzzOutcome ClassifyRejection(string family, string? reasonCode)
        {
            if (reasonCode is { Length: > 0 }
                && _families.TryGetValue(family, out var contract)
                && contract.AllowedReasonCodes.Contains(reasonCode))
                return FuzzOutcome.CorrectlyRejected;
            return FuzzOutcome.UnexpectedlyRejected;
        }

        /// <summary>Evaluates a completed run against the contract, returning every violation (empty = pass).</summary>
        public IReadOnlyList<FuzzContractViolation> Evaluate(FuzzRunManifest manifest)
        {
            var violations = new List<FuzzContractViolation>();

            foreach (var stats in manifest.FamilyStats())
            {
                // Any failing outcome is a violation; report the highest-severity representative per family.
                foreach (var (outcome, count) in stats.Outcomes.OrderByDescending(o => o.Key))
                {
                    if (!outcome.IsFailure() || count == 0)
                        continue;
                    violations.Add(new FuzzContractViolation(
                        stats.Family,
                        outcome.IsSilentDefect() ? FuzzViolationKind.SilentDefect
                            : outcome == FuzzOutcome.UnexpectedlyRejected ? FuzzViolationKind.UndocumentedRejection
                            : FuzzViolationKind.UnexpectedFailure,
                        $"{count} case(s) with outcome {outcome}"));
                }

                if (_families.TryGetValue(stats.Family, out var contract)
                    && stats.UnsupportedRate > contract.MaxUnsupportedRate + 1e-9)
                {
                    violations.Add(new FuzzContractViolation(
                        stats.Family,
                        FuzzViolationKind.UnsupportedRateExceeded,
                        $"unsupported rate {stats.UnsupportedRate:P2} exceeds ceiling {contract.MaxUnsupportedRate:P2}"));
                }
            }

            var frontier = new HashSet<string>(manifest.FeatureFrontier(), StringComparer.Ordinal);
            var missing = RequiredFeatures.Where(f => !frontier.Contains(f)).OrderBy(f => f, StringComparer.Ordinal).ToArray();
            if (missing.Length > 0)
                violations.Add(new FuzzContractViolation(
                    "(coverage)",
                    FuzzViolationKind.CoverageFrontierShrank,
                    "missing required feature edges: " + string.Join(", ", missing)));

            return violations;
        }

        public static FuzzSupportContract FromJson(string json)
        {
            var dto = JsonSerializer.Deserialize<ContractDto>(json, FuzzRunManifest.ManifestJsonOptions)
                      ?? throw new InvalidOperationException("Empty support contract JSON.");
            var families = (dto.Families ?? new()).ToDictionary(
                kv => kv.Key,
                kv => new FuzzFamilyContract(
                    kv.Value.AllowedReasonCodes ?? Array.Empty<string>(),
                    kv.Value.MaxUnsupportedRate),
                StringComparer.Ordinal);
            return new FuzzSupportContract(families, dto.RequiredFeatures);
        }

        public string ToJson()
        {
            var dto = new ContractDto
            {
                Families = _families.ToDictionary(
                    kv => kv.Key,
                    kv => new FamilyContractDto
                    {
                        AllowedReasonCodes = kv.Value.AllowedReasonCodes.OrderBy(x => x, StringComparer.Ordinal).ToArray(),
                        MaxUnsupportedRate = kv.Value.MaxUnsupportedRate,
                    }),
                RequiredFeatures = RequiredFeatures.OrderBy(x => x, StringComparer.Ordinal).ToArray(),
            };
            return JsonSerializer.Serialize(dto, FuzzRunManifest.ManifestJsonOptions);
        }

        private sealed class ContractDto
        {
            public Dictionary<string, FamilyContractDto>? Families { get; set; }
            public string[]? RequiredFeatures { get; set; }
        }

        private sealed class FamilyContractDto
        {
            public string[]? AllowedReasonCodes { get; set; }
            public double MaxUnsupportedRate { get; set; }
        }
    }

    /// <summary>Per-family contract: the reason codes nORM may reject with, and the ceiling on how often it may.</summary>
    public sealed record FuzzFamilyContract(IReadOnlyCollection<string> AllowedReasonCodes, double MaxUnsupportedRate)
    {
        public IReadOnlyCollection<string> AllowedReasonCodes { get; } =
            new HashSet<string>(AllowedReasonCodes, StringComparer.Ordinal);
    }

    public enum FuzzViolationKind
    {
        SilentDefect,
        UnexpectedFailure,
        UndocumentedRejection,
        UnsupportedRateExceeded,
        CoverageFrontierShrank,
    }

    public sealed record FuzzContractViolation(string Family, FuzzViolationKind Kind, string Detail);
}
