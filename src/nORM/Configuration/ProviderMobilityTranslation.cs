using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Describes how nORM can carry a feature across supported providers.
    /// </summary>
    public enum ProviderMobilitySupport
    {
        /// <summary>
        /// nORM can translate the feature to each supported provider with the
        /// same observable behavior.
        /// </summary>
        Portable,

        /// <summary>
        /// nORM can preserve behavior by rewriting, simulating, or using a
        /// bounded fallback rather than a single provider-native primitive.
        /// </summary>
        Emulated,

        /// <summary>
        /// The feature exposes caller-authored provider language or provider
        /// handles. It can exist in compatibility mode but cannot be certified
        /// as provider-mobile generated surface.
        /// </summary>
        ProviderBound,

        /// <summary>
        /// nORM cannot preserve semantics safely and must fail deterministically.
        /// </summary>
        Unsupported
    }

    /// <summary>
    /// Provider-mobility features known to nORM's runtime and certification layers.
    /// </summary>
    public enum ProviderMobilityFeature
    {
        /// <summary>Generated LINQ queries translated by nORM.</summary>
        GeneratedLinqQuery,
        /// <summary>Generated direct write, change tracker, and bulk write paths.</summary>
        GeneratedWrite,
        /// <summary>Generated tenant filters, write predicates, cache keys, and diagnostics.</summary>
        GeneratedTenantBoundary,
        /// <summary>nORM-managed temporal history tables, tags, triggers, and AsOf queries.</summary>
        NormManagedTemporal,
        /// <summary>nORM-owned transaction savepoint wrapper APIs.</summary>
        WrappedSavepoint,
        /// <summary>Explicit warning-level top-level client projection tail.</summary>
        ExplicitClientProjectionTail,
        /// <summary>Silent client evaluation opt-in.</summary>
        SilentClientEvaluation,
        /// <summary>Raw SQL query APIs.</summary>
        RawSql,
        /// <summary>Stored procedure execution APIs and procedure definitions.</summary>
        StoredProcedure,
        /// <summary>User-authored SQL function format strings.</summary>
        CustomSqlFunction,
        /// <summary>Runtime dynamic table query based on live provider schema.</summary>
        DynamicTableQuery,
        /// <summary>Direct provider connection access.</summary>
        DirectConnectionAccess,
        /// <summary>Direct DatabaseProvider access.</summary>
        DirectProviderAccess,
        /// <summary>Direct DbCommand construction or usage.</summary>
        DirectCommandAccess,
        /// <summary>Direct DbTransaction access.</summary>
        DirectTransactionAccess,
        /// <summary>Write overloads that accept caller-owned DbTransaction instances.</summary>
        ExternalTransactionWrite,
        /// <summary>Raw DbTransaction savepoint overloads.</summary>
        RawTransactionSavepoint,
        /// <summary>Command interceptors that can inspect, rewrite, or suppress generated commands.</summary>
        CommandInterceptor,
        /// <summary>Provider-native tenant security or RLS bootstrap.</summary>
        ProviderNativeTenantSecurity,
        /// <summary>Provider-native temporal/versioning bootstrap.</summary>
        ProviderNativeTemporal,
        /// <summary>Raw SQL source-generation command paths.</summary>
        CompileTimeRawSql,
        /// <summary>Provider-specific package or concrete provider bootstrap code.</summary>
        ProviderBootstrap,
        /// <summary>Provider-specific column type strings.</summary>
        ProviderSpecificColumnType,
        /// <summary>Unsupported or unresolved schema CLR type metadata.</summary>
        SchemaUnsupportedClrType,
        /// <summary>Invalid, duplicate, or incomplete schema metadata.</summary>
        SchemaInvalidMetadata,
        /// <summary>Schema identity configuration that cannot translate across providers.</summary>
        SchemaNonPortableIdentity,
        /// <summary>Provider-specific schema default SQL.</summary>
        SchemaProviderSpecificDefault,
        /// <summary>Schema default SQL requiring provider review.</summary>
        SchemaDefaultNeedsReview,
        /// <summary>Provider migration generator could not emit DDL from the schema snapshot.</summary>
        SchemaProviderGenerationFailed,
        /// <summary>Keyless schema shape with constrained generated behavior.</summary>
        SchemaMissingPrimaryKey,
        /// <summary>Unknown provider target in certification.</summary>
        ProviderTargetUnknown,
        /// <summary>Provider target connection could not be opened or validated.</summary>
        ProviderTargetOpen,
        /// <summary>Provider target capability failed certification.</summary>
        ProviderTargetCapability,
        /// <summary>Certification emitted a finding kind not classified by the translation layer.</summary>
        CertificationUnclassifiedFinding,
        /// <summary>LINQ shape that is translated only with documented provider constraints.</summary>
        ConstrainedLinqShape,
        /// <summary>Unsupported LINQ operators or method/member translations.</summary>
        UnsupportedLinqShape
    }

    /// <summary>
    /// Provider target capabilities that affect whether generated nORM semantics
    /// can be certified for a concrete database/version.
    /// </summary>
    public enum ProviderMobilityProviderFeature
    {
        /// <summary>Minimum supported database server version.</summary>
        ServerVersion,
        /// <summary>Provider-backed JSON translation.</summary>
        JsonTranslation,
        /// <summary>Provider JSON path extraction and validation strategy.</summary>
        JsonPathTranslation,
        /// <summary>nORM-managed temporal history support.</summary>
        TemporalVersioning,
        /// <summary>Generated bulk insert behavior, native or emulated.</summary>
        BulkInsert,
        /// <summary>Provider transaction savepoint support.</summary>
        Savepoints,
        /// <summary>Maximum parameter count and required batching behavior.</summary>
        ParameterLimit,
        /// <summary>Provider-native tenant session context support.</summary>
        NativeTenantSessionContext,
        /// <summary>Provider-native temporal table support.</summary>
        ProviderNativeTemporalTables,
        /// <summary>Provider native or emulated row-value tuple comparison support.</summary>
        RowTupleComparison,
        /// <summary>Provider native or emulated ordered string aggregate support.</summary>
        OrderedStringAggregate,
        /// <summary>Provider native, emulated, or unsupported regular-expression translation support.</summary>
        RegexTranslation,
        /// <summary>Provider identifier escaping strategy.</summary>
        IdentifierEscaping,
        /// <summary>Provider parameter prefix and binding strategy.</summary>
        ParameterBinding,
        /// <summary>Provider paging syntax strategy.</summary>
        PagingTranslation,
        /// <summary>Provider boolean predicate translation strategy.</summary>
        BooleanPredicateTranslation,
        /// <summary>Provider null-safe equality translation strategy.</summary>
        NullSemantics,
        /// <summary>Provider LIKE wildcard escaping strategy.</summary>
        LikeEscapeTranslation,
        /// <summary>Provider string concatenation translation strategy.</summary>
        StringConcatTranslation,
        /// <summary>Provider scalar cast and conversion translation strategy.</summary>
        TypeConversionTranslation,
        /// <summary>Provider string predicate translation strategy.</summary>
        StringPredicateTranslation,
        /// <summary>Provider character code-point translation strategy.</summary>
        CharacterTranslation,
        /// <summary>Provider fixed numeric and temporal formatting translation strategy.</summary>
        FormattingTranslation,
        /// <summary>Provider DateTime comparison normalization strategy.</summary>
        DateTimeComparisonNormalization,
        /// <summary>Provider decimal comparison, sort, and aggregate normalization strategy.</summary>
        DecimalComparisonNormalization,
        /// <summary>Provider TimeSpan comparison normalization strategy.</summary>
        TimeSpanComparisonNormalization,
        /// <summary>Provider DateTime/DateOnly/TimeOnly construction from SQL parts.</summary>
        TemporalConstructionTranslation,
        /// <summary>Provider DateTime/DateOnly/TimeOnly/TimeSpan arithmetic strategy.</summary>
        TemporalArithmeticTranslation,
        /// <summary>Provider temporal tag clock-source strategy.</summary>
        TemporalClockSource,
        /// <summary>Provider generated-key retrieval strategy.</summary>
        GeneratedKeyRetrieval,
        /// <summary>Provider idempotent insert-or-ignore strategy.</summary>
        InsertOrIgnoreTranslation,
        /// <summary>Provider generated CUD subquery rewrite strategy.</summary>
        CudSubqueryRewrite,
        /// <summary>Provider bitwise XOR translation strategy.</summary>
        BitwiseXorTranslation,
        /// <summary>Provider window-function translation strategy.</summary>
        WindowFunctionTranslation,
        /// <summary>Provider case-sensitive string comparison strategy.</summary>
        CaseSensitiveStringComparison,
        /// <summary>Provider maximum SQL statement length and generated SQL splitting strategy.</summary>
        SqlStatementLengthLimit
    }

    /// <summary>
    /// A single provider-mobility classification used by strict runtime checks,
    /// static certification and documentation.
    /// </summary>
    public sealed class ProviderMobilityTranslationDecision
    {
        /// <summary>
        /// Creates a provider-mobility translation decision.
        /// </summary>
        public ProviderMobilityTranslationDecision(
            ProviderMobilityFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix)
        {
            Feature = feature;
            Support = support;
            StrictRuntimeAllowed = strictRuntimeAllowed;
            CertificationSeverity = certificationSeverity ?? throw new ArgumentNullException(nameof(certificationSeverity));
            Reason = reason ?? throw new ArgumentNullException(nameof(reason));
            SuggestedFix = suggestedFix ?? throw new ArgumentNullException(nameof(suggestedFix));
        }

        /// <summary>Feature being classified.</summary>
        public ProviderMobilityFeature Feature { get; }

        /// <summary>Provider mobility support class.</summary>
        public ProviderMobilitySupport Support { get; }

        /// <summary>Whether strict runtime mode may execute this feature.</summary>
        public bool StrictRuntimeAllowed { get; }

        /// <summary>Static certification severity: Error, Warning, or Info.</summary>
        public string CertificationSeverity { get; }

        /// <summary>Short explanation of the portability decision.</summary>
        public string Reason { get; }

        /// <summary>Recommended path to make or keep the code provider-mobile.</summary>
        public string SuggestedFix { get; }
    }

    /// <summary>
    /// A provider/version/capability decision used by certification reports,
    /// startup validation, and provider capability documentation.
    /// </summary>
    public sealed class ProviderMobilityProviderDecision
    {
        /// <summary>
        /// Creates a provider target capability decision.
        /// </summary>
        public ProviderMobilityProviderDecision(
            string providerName,
            ProviderMobilityProviderFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix,
            Version? minimumServerVersion,
            Version? actualServerVersion)
        {
            ProviderName = providerName ?? throw new ArgumentNullException(nameof(providerName));
            Feature = feature;
            Support = support;
            StrictRuntimeAllowed = strictRuntimeAllowed;
            CertificationSeverity = certificationSeverity ?? throw new ArgumentNullException(nameof(certificationSeverity));
            Reason = reason ?? throw new ArgumentNullException(nameof(reason));
            SuggestedFix = suggestedFix ?? throw new ArgumentNullException(nameof(suggestedFix));
            MinimumServerVersion = minimumServerVersion;
            ActualServerVersion = actualServerVersion;
        }

        /// <summary>Provider being classified.</summary>
        public string ProviderName { get; }

        /// <summary>Provider target feature being classified.</summary>
        public ProviderMobilityProviderFeature Feature { get; }

        /// <summary>Provider mobility support class.</summary>
        public ProviderMobilitySupport Support { get; }

        /// <summary>Whether strict runtime mode may use this provider target capability.</summary>
        public bool StrictRuntimeAllowed { get; }

        /// <summary>Certification severity: Error, Warning, or Info.</summary>
        public string CertificationSeverity { get; }

        /// <summary>Short explanation of the provider target decision.</summary>
        public string Reason { get; }

        /// <summary>Recommended remediation or review path.</summary>
        public string SuggestedFix { get; }

        /// <summary>Minimum server version required by the provider, when declared.</summary>
        public Version? MinimumServerVersion { get; }

        /// <summary>Actual connected server version, when known.</summary>
        public Version? ActualServerVersion { get; }
    }

    /// <summary>
    /// Central provider-mobility translation classifier. This is the contract
    /// nORM uses to decide whether a feature is translated, emulated, explicitly
    /// provider-bound, or unsupported.
    /// </summary>
    public static class ProviderMobilityTranslator
    {
        private static readonly IReadOnlyDictionary<ProviderMobilityFeature, ProviderMobilityTranslationDecision> s_decisions =
            BuildDecisions().ToDictionary(static d => d.Feature);

        private static readonly IReadOnlyDictionary<string, ProviderMobilityFeature> s_findingKindMap =
            new Dictionary<string, ProviderMobilityFeature>(StringComparer.OrdinalIgnoreCase)
            {
                ["raw-sql"] = ProviderMobilityFeature.RawSql,
                ["stored-procedure"] = ProviderMobilityFeature.StoredProcedure,
                ["stored-procedure-definition"] = ProviderMobilityFeature.StoredProcedure,
                ["sql-server-specific-sql"] = ProviderMobilityFeature.RawSql,
                ["custom-sql-function"] = ProviderMobilityFeature.CustomSqlFunction,
                ["compile-time-raw-sql"] = ProviderMobilityFeature.CompileTimeRawSql,
                ["dynamic-table-query"] = ProviderMobilityFeature.DynamicTableQuery,
                ["direct-connection"] = ProviderMobilityFeature.DirectConnectionAccess,
                ["direct-provider-access"] = ProviderMobilityFeature.DirectProviderAccess,
                ["direct-command-access"] = ProviderMobilityFeature.DirectCommandAccess,
                ["direct-transaction-access"] = ProviderMobilityFeature.DirectTransactionAccess,
                ["command-interceptor"] = ProviderMobilityFeature.CommandInterceptor,
                ["provider-native-tenant-security"] = ProviderMobilityFeature.ProviderNativeTenantSecurity,
                ["provider-native-temporal"] = ProviderMobilityFeature.ProviderNativeTemporal,
                ["provider-specific-package"] = ProviderMobilityFeature.ProviderBootstrap,
                ["provider-bootstrap-connection"] = ProviderMobilityFeature.ProviderBootstrap,
                ["provider-specific-column-type"] = ProviderMobilityFeature.ProviderSpecificColumnType,
                ["schema-invalid"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-invalid-identifier"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-duplicate-table"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-duplicate-column"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-missing-clr-type"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-invalid-foreign-key"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-snapshot-missing"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-snapshot-invalid-json"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["schema-unsupported-clr-type"] = ProviderMobilityFeature.SchemaUnsupportedClrType,
                ["schema-nonportable-identity"] = ProviderMobilityFeature.SchemaNonPortableIdentity,
                ["schema-provider-specific-default"] = ProviderMobilityFeature.SchemaProviderSpecificDefault,
                ["schema-default-needs-review"] = ProviderMobilityFeature.SchemaDefaultNeedsReview,
                ["schema-provider-generation-failed"] = ProviderMobilityFeature.SchemaProviderGenerationFailed,
                ["schema-missing-primary-key"] = ProviderMobilityFeature.SchemaMissingPrimaryKey,
                ["provider-target-unknown"] = ProviderMobilityFeature.ProviderTargetUnknown,
                ["provider-target-open"] = ProviderMobilityFeature.ProviderTargetOpen,
                ["provider-target-capability"] = ProviderMobilityFeature.ProviderTargetCapability,
                ["scan-path-missing"] = ProviderMobilityFeature.SchemaInvalidMetadata,
                ["certification-unclassified-finding"] = ProviderMobilityFeature.CertificationUnclassifiedFinding,
                ["constrained-linq-shape"] = ProviderMobilityFeature.ConstrainedLinqShape,
                ["unsupported-linq-shape"] = ProviderMobilityFeature.UnsupportedLinqShape,
                ["client-evaluation"] = ProviderMobilityFeature.SilentClientEvaluation,
                ["client-projection-tail"] = ProviderMobilityFeature.ExplicitClientProjectionTail
            };

        /// <summary>All known provider-mobility translation decisions.</summary>
        public static IReadOnlyList<ProviderMobilityTranslationDecision> Decisions { get; } =
            s_decisions.Values.OrderBy(static d => d.Feature.ToString(), StringComparer.Ordinal).ToArray();

        /// <summary>
        /// Gets the provider-mobility decision for a known feature.
        /// </summary>
        public static ProviderMobilityTranslationDecision Decide(ProviderMobilityFeature feature)
            => s_decisions.TryGetValue(feature, out var decision)
                ? decision
                : throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown provider mobility feature.");

        /// <summary>
        /// Gets the provider-mobility decision for a static certification finding kind.
        /// </summary>
        public static bool TryDecideFindingKind(string kind, out ProviderMobilityTranslationDecision decision)
        {
            if (kind != null && s_findingKindMap.TryGetValue(kind, out var feature))
            {
                decision = Decide(feature);
                return true;
            }

            decision = null!;
            return false;
        }

        /// <summary>
        /// Gets the provider-mobility decision for a runtime strict-mode feature name.
        /// </summary>
        public static ProviderMobilityTranslationDecision DecideRuntimeFeature(string feature)
            => Decide(RuntimeFeatureToMobilityFeature(feature));

        /// <summary>
        /// Decides whether a provider target satisfies its declared minimum server version.
        /// </summary>
        public static ProviderMobilityProviderDecision DecideProviderVersion(
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            ArgumentNullException.ThrowIfNull(capabilities);

            var minimum = capabilities.MinimumServerVersion;
            if (minimum == null)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Warning",
                    $"{capabilities.ProviderName} does not declare a minimum server version, so nORM cannot certify a version floor for this provider target.",
                    "Declare ProviderCapabilities.MinimumServerVersion for release-certified providers or keep the target as a reviewed compatibility profile.",
                    null,
                    actualServerVersion);
            }

            if (actualServerVersion == null)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} server version could not be determined during provider startup validation.",
                    $"Use a provider/driver that exposes the server version or verify the target manually before certification. Minimum supported version is {minimum}.",
                    minimum,
                    null);
            }

            if (actualServerVersion < minimum)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} server version {actualServerVersion} is not supported. Minimum supported version is {minimum}.",
                    "Upgrade the database server or target a provider/version profile whose minimum version is satisfied.",
                    minimum,
                    actualServerVersion);
            }

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.ServerVersion,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} server version {actualServerVersion} satisfies the minimum supported version {minimum}.",
                "Keep startup validation enabled for each configured provider target.",
                minimum,
                actualServerVersion);
        }

        /// <summary>
        /// Parses a provider-supplied server version string into a comparable version.
        /// </summary>
        public static Version? ParseProviderVersion(string? versionText)
        {
            if (string.IsNullOrWhiteSpace(versionText))
                return null;

            var text = versionText.Trim();
            var start = -1;
            for (var i = 0; i < text.Length; i++)
            {
                if (char.IsDigit(text[i]))
                {
                    start = i;
                    break;
                }
            }

            if (start < 0)
                return null;

            var end = start;
            while (end < text.Length && (char.IsDigit(text[end]) || text[end] == '.'))
                end++;

            var candidate = text[start..end].Trim('.');
            if (candidate.Length == 0)
                return null;

            var parts = candidate.Split('.', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 1)
                candidate += ".0";

            return Version.TryParse(candidate, out var version) ? version : null;
        }

        /// <summary>
        /// Decides whether a provider capability can participate in provider-mobile certification.
        /// </summary>
        public static ProviderMobilityProviderDecision DecideProviderCapability(
            ProviderCapabilities capabilities,
            ProviderMobilityProviderFeature feature)
        {
            ArgumentNullException.ThrowIfNull(capabilities);

            return feature switch
            {
                ProviderMobilityProviderFeature.ServerVersion => DecideProviderVersion(capabilities, capabilities.MinimumServerVersion),
                ProviderMobilityProviderFeature.JsonTranslation => CapabilityBool(
                    capabilities,
                    feature,
                    capabilities.SupportsJson,
                    "JSON translation is available through nORM provider SQL generation.",
                    "JSON translation is not available for this provider target.",
                    "Avoid JSON query shapes for this target or add provider translations and live parity tests."),
                ProviderMobilityProviderFeature.JsonPathTranslation => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "JSON path extraction strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so JSON path SQL and validation are explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.TemporalVersioning => CapabilityBool(
                    capabilities,
                    feature,
                    capabilities.SupportsTemporalVersioning,
                    "nORM-managed temporal history is supported for this provider target.",
                    "nORM-managed temporal history is not supported for this provider target.",
                    "Disable temporal features for this target or add provider-specific history DDL/triggers with parity tests."),
                ProviderMobilityProviderFeature.BulkInsert => capabilities.SupportsNativeBulkInsert
                    ? PD(capabilities.ProviderName, feature, ProviderMobilitySupport.Portable, true, "Info",
                        "Provider-native bulk insert is available.",
                        "Use the generated nORM bulk API and keep benchmark evidence per provider.",
                        capabilities.MinimumServerVersion, null)
                    : PD(capabilities.ProviderName, feature, ProviderMobilitySupport.Emulated, true, "Warning",
                        "Provider-native bulk insert is not available; nORM must preserve generated bulk semantics through fallback SQL.",
                        "Keep fallback bulk behavior covered by provider parity tests and avoid claiming native bulk for this target.",
                        capabilities.MinimumServerVersion, null),
                ProviderMobilityProviderFeature.Savepoints => CapabilityBool(
                    capabilities,
                    feature,
                    capabilities.SupportsSavepoints,
                    "Transaction savepoints are available for nORM transaction wrappers.",
                    "Transaction savepoints are not available for this provider target.",
                    "Avoid savepoint-dependent workflows or add provider savepoint support before certification."),
                ProviderMobilityProviderFeature.ParameterLimit => capabilities.MaxParameters > 0
                    ? PD(capabilities.ProviderName, feature,
                        capabilities.MaxParameters < 1_000 ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                        true,
                        capabilities.MaxParameters < 1_000 ? "Warning" : "Info",
                        $"{capabilities.ProviderName} supports up to {capabilities.MaxParameters} parameters per command.",
                        "Generated write/query paths must batch or split commands before reaching the provider parameter limit.",
                        capabilities.MinimumServerVersion, null)
                    : PD(capabilities.ProviderName, feature, ProviderMobilitySupport.Unsupported, false, "Error",
                        $"{capabilities.ProviderName} does not expose a valid parameter limit.",
                        "Set ProviderCapabilities.MaxParameters to a positive value and add batching tests.",
                        capabilities.MinimumServerVersion, null),
                ProviderMobilityProviderFeature.NativeTenantSessionContext => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Info",
                    "Provider-native tenant session context is database-specific defense-in-depth infrastructure, not generated-path provider mobility.",
                    "Use generated tenant predicates for provider mobility; keep native RLS/session context as explicit deployment evidence.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.ProviderNativeTemporalTables => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Info",
                    "Provider-native temporal tables are database-specific infrastructure, not nORM-managed provider-neutral temporal history.",
                    "Use nORM-managed temporal history for provider mobility; keep native temporal DDL as explicit provider evidence.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.RowTupleComparison => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Row-value tuple comparison strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so native versus emulated tuple translation is explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.OrderedStringAggregate => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Ordered string aggregate strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so native versus emulated aggregate translation is explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.RegexTranslation => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Regular-expression translation strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so native, UDF-backed, and unsupported regex paths are explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.IdentifierEscaping
                    or ProviderMobilityProviderFeature.ParameterBinding
                    or ProviderMobilityProviderFeature.PagingTranslation
                    or ProviderMobilityProviderFeature.BooleanPredicateTranslation
                    or ProviderMobilityProviderFeature.NullSemantics
                    or ProviderMobilityProviderFeature.LikeEscapeTranslation
                    or ProviderMobilityProviderFeature.StringConcatTranslation
                    or ProviderMobilityProviderFeature.TypeConversionTranslation
                    or ProviderMobilityProviderFeature.StringPredicateTranslation
                    or ProviderMobilityProviderFeature.CharacterTranslation
                    or ProviderMobilityProviderFeature.FormattingTranslation
                    or ProviderMobilityProviderFeature.DateTimeComparisonNormalization
                    or ProviderMobilityProviderFeature.DecimalComparisonNormalization
                    or ProviderMobilityProviderFeature.TimeSpanComparisonNormalization
                    or ProviderMobilityProviderFeature.TemporalConstructionTranslation
                    or ProviderMobilityProviderFeature.TemporalArithmeticTranslation
                    or ProviderMobilityProviderFeature.TemporalClockSource
                    or ProviderMobilityProviderFeature.GeneratedKeyRetrieval
                    or ProviderMobilityProviderFeature.InsertOrIgnoreTranslation
                    or ProviderMobilityProviderFeature.CudSubqueryRewrite
                    or ProviderMobilityProviderFeature.BitwiseXorTranslation
                    or ProviderMobilityProviderFeature.WindowFunctionTranslation
                    or ProviderMobilityProviderFeature.CaseSensitiveStringComparison
                    or ProviderMobilityProviderFeature.SqlStatementLengthLimit => PD(
                        capabilities.ProviderName,
                        feature,
                        ProviderMobilitySupport.Emulated,
                        true,
                        "Warning",
                        $"{feature} strategy requires the concrete nORM provider implementation profile.",
                        "Use DecideProviderImplementationProfile when certifying concrete targets so dialect-specific rewrites are explicit.",
                        capabilities.MinimumServerVersion,
                        null),
                _ => throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown provider target feature.")
            };
        }

        /// <summary>
        /// Builds a provider target capability profile from a provider descriptor.
        /// </summary>
        public static IReadOnlyList<ProviderMobilityProviderDecision> DecideProviderCapabilityProfile(
            ProviderCapabilities capabilities,
            Version? actualServerVersion = null,
            bool requireActualServerVersion = false)
        {
            ArgumentNullException.ThrowIfNull(capabilities);

            var features = Enum.GetValues<ProviderMobilityProviderFeature>();
            var decisions = new List<ProviderMobilityProviderDecision>(features.Length);
            foreach (var feature in features)
            {
                if (feature == ProviderMobilityProviderFeature.ServerVersion)
                {
                    decisions.Add(requireActualServerVersion || actualServerVersion != null
                        ? DecideProviderVersion(capabilities, actualServerVersion)
                        : BuildDescriptorOnlyProviderVersionDecision(capabilities));
                    continue;
                }

                var decision = DecideProviderCapability(capabilities, feature);
                decisions.Add(actualServerVersion is not null && decision.ActualServerVersion is null
                    ? WithActualServerVersion(decision, actualServerVersion)
                    : decision);
            }

            return decisions;
        }

        /// <summary>
        /// Builds a provider target profile from the concrete nORM provider implementation.
        /// This includes capability descriptors plus provider SQL translation strategies
        /// that are not exposed on <see cref="ProviderCapabilities"/>.
        /// </summary>
        public static IReadOnlyList<ProviderMobilityProviderDecision> DecideProviderImplementationProfile(
            DatabaseProvider provider,
            Version? actualServerVersion = null,
            bool requireActualServerVersion = false)
        {
            ArgumentNullException.ThrowIfNull(provider);

            var capabilities = provider.Capabilities;
            var decisions = DecideProviderCapabilityProfile(
                capabilities,
                actualServerVersion,
                requireActualServerVersion).ToList();

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.RowTupleComparison,
                provider.SupportsRowTupleComparison ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                provider.SupportsRowTupleComparison ? "Info" : "Warning",
                provider.SupportsRowTupleComparison
                    ? "Provider supports native row-value tuple comparison for generated keyset/tuple predicates."
                    : "Provider lacks native row-value tuple comparison; nORM rewrites tuple semantics into provider-compatible predicates.",
                provider.SupportsRowTupleComparison
                    ? "Keep tuple comparison live parity tests current."
                    : "Keep emulated tuple comparison covered by provider parity tests and avoid claiming native tuple comparison for this target.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, BuildOrderedStringAggregateDecision(provider, actualServerVersion));
            Replace(decisions, BuildRegexTranslationDecision(provider, actualServerVersion));
            Replace(decisions, BuildJsonPathTranslationDecision(provider, actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.IdentifierEscaping,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} identifiers are escaped through the concrete nORM provider strategy ({provider.Escape("NormProbe")}).",
                "Keep generated SQL paths on nORM identifier escaping and avoid caller-authored delimited SQL.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.ParameterBinding,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} parameters use the '{provider.ParamPrefix}' prefix and provider-owned DbParameter creation.",
                "Use generated nORM query/write APIs so parameter names and provider parameter objects stay owned by nORM.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.PagingTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                provider.UsesFetchOffsetPaging
                    ? $"{capabilities.ProviderName} uses SQL Server-style OFFSET/FETCH paging translation."
                    : $"{capabilities.ProviderName} uses LIMIT/OFFSET paging translation.",
                "Keep OrderBy/Skip/Take live-provider parity tests current for this target.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.BooleanPredicateTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                provider.PrefersBareBooleanPredicates
                    ? $"{capabilities.ProviderName} prefers bare boolean predicates for generated WHERE clauses."
                    : $"{capabilities.ProviderName} translates boolean predicates to provider boolean literals.",
                "Keep boolean predicate translation in generated LINQ; avoid raw provider boolean fragments.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.NullSemantics,
                IsExpandedNullSemantics(provider.NullSafeEqual("a", "b")) ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                "Info",
                IsExpandedNullSemantics(provider.NullSafeEqual("a", "b"))
                    ? $"{capabilities.ProviderName} null-safe equality is preserved by generated SQL expansion ({provider.NullSafeEqual("a", "b")})."
                    : $"{capabilities.ProviderName} uses native null-safe equality syntax ({provider.NullSafeEqual("a", "b")}).",
                "Keep equality/inequality null-semantics parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.LikeEscapeTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} LIKE wildcard escaping uses '{provider.LikeEscapeChar}' and provider-owned REPLACE translation.",
                "Keep StartsWith/EndsWith/Contains LIKE escaping covered by provider parity tests.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.StringConcatTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} string concatenation is translated as {provider.GetConcatSql("left", "right")}.",
                "Keep string concatenation and string interpolation query parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, BuildTypeConversionDecision(provider, actualServerVersion));
            Replace(decisions, BuildStringPredicateDecision(provider, actualServerVersion));
            Replace(decisions, BuildCharacterTranslationDecision(provider, actualServerVersion));
            Replace(decisions, BuildFormattingTranslationDecision(provider, actualServerVersion));

            Replace(decisions, BuildNormalizationDecision(
                capabilities,
                actualServerVersion,
                ProviderMobilityProviderFeature.DateTimeComparisonNormalization,
                provider.NormalizeDateTimeForCompare("value"),
                "DateTime comparison",
                "Keep mixed-offset DateTime comparison parity tests current."));

            Replace(decisions, BuildNormalizationDecision(
                capabilities,
                actualServerVersion,
                ProviderMobilityProviderFeature.DecimalComparisonNormalization,
                provider.NormalizeDecimalForCompare("value"),
                "decimal comparison/sort/aggregate",
                provider.NormalizeDecimalForCompare("value").Equals("value", StringComparison.Ordinal)
                    ? "Keep decimal comparison and aggregate parity tests current."
                    : "SQLite decimal normalization uses REAL for numeric semantics; keep precision caveats documented and tolerance tests current.",
                warnWhenEmulated: provider.NormalizeDecimalForCompare("value") != "value"));

            Replace(decisions, BuildNormalizationDecision(
                capabilities,
                actualServerVersion,
                ProviderMobilityProviderFeature.TimeSpanComparisonNormalization,
                provider.NormalizeTimeSpanForCompare("value"),
                "TimeSpan comparison",
                "Keep cross-column and mixed-magnitude TimeSpan comparison parity tests current."));

            Replace(decisions, BuildTemporalConstructionDecision(provider, actualServerVersion));
            Replace(decisions, BuildTemporalArithmeticDecision(provider, actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.TemporalClockSource,
                provider.UsesDatabaseClockForTemporalTags ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                provider.UsesDatabaseClockForTemporalTags ? "Info" : "Warning",
                provider.UsesDatabaseClockForTemporalTags
                    ? $"{capabilities.ProviderName} temporal tags and history triggers use the database clock for consistent AsOf windows."
                    : $"{capabilities.ProviderName} temporal tag timestamps are caller-bound while history may use provider execution time.",
                provider.UsesDatabaseClockForTemporalTags
                    ? "Keep temporal tag and AsOf live-provider parity tests current."
                    : "Prefer a database-clock tag implementation before certifying temporal history for this provider target.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.GeneratedKeyRetrieval,
                provider.SupportsCommandGeneratedKeyRetrieval ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                "Info",
                provider.SupportsCommandGeneratedKeyRetrieval
                    ? $"{capabilities.ProviderName} can retrieve generated keys from command execution."
                    : $"{capabilities.ProviderName} uses provider identity-returning SQL or follow-up identity retrieval where required.",
                "Keep single-insert generated-key parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, BuildInsertOrIgnoreDecision(provider, actualServerVersion));
            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.CudSubqueryRewrite,
                provider.CudWhereInSubqueryNeedsDoubleWrap ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                provider.CudWhereInSubqueryNeedsDoubleWrap ? "Warning" : "Info",
                provider.CudWhereInSubqueryNeedsDoubleWrap
                    ? $"{capabilities.ProviderName} needs a generated double-wrap around self-referencing CUD subqueries."
                    : $"{capabilities.ProviderName} accepts nORM's standard generated CUD subquery shape.",
                "Keep ExecuteUpdate/ExecuteDelete, bulk CUD, and tenant-write parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.BitwiseXorTranslation,
                IsAlgebraicXorRewrite(provider.GetBitwiseXorSql("left", "right"))
                    ? ProviderMobilitySupport.Emulated
                    : ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} bitwise XOR is translated as {provider.GetBitwiseXorSql("left", "right")}.",
                "Keep bitwise operator parity tests current for generated LINQ.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.WindowFunctionTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} is within nORM's supported window-function floor for ROW_NUMBER and aggregate-over-partition translation.",
                "Keep GroupBy, first/last per group, paging, and window-function parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.CaseSensitiveStringComparison,
                provider.ForceCaseSensitiveStringComparison("value").Equals("value", StringComparison.Ordinal)
                    ? ProviderMobilitySupport.Portable
                    : ProviderMobilitySupport.Emulated,
                true,
                "Info",
                provider.ForceCaseSensitiveStringComparison("value").Equals("value", StringComparison.Ordinal)
                    ? $"{capabilities.ProviderName} default string comparison is already case-sensitive for generated equality/IN semantics."
                    : $"{capabilities.ProviderName} applies provider-specific case-sensitive comparison wrapping ({provider.ForceCaseSensitiveStringComparison("value")}).",
                "Keep string equality and IN case-sensitivity parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.SqlStatementLengthLimit,
                provider.MaxSqlLength == int.MaxValue ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                "Info",
                provider.MaxSqlLength == int.MaxValue
                    ? $"{capabilities.ProviderName} does not expose a bounded SQL statement length through nORM's provider contract."
                    : $"{capabilities.ProviderName} limits individual SQL statements to {provider.MaxSqlLength} characters; generated SQL must split before reaching that bound.",
                "Keep generated SQL batching/splitting tests current for large IN lists, bulk operations, and migrations.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            return decisions
                .OrderBy(static decision => decision.Feature.ToString(), StringComparer.Ordinal)
                .ToArray();
        }

        internal static string BuildStrictViolationMessage(string runtimeFeature)
        {
            var decision = DecideRuntimeFeature(runtimeFeature);
            return $"{runtimeFeature} is outside nORM's strict provider mobility contract. " +
                   $"{decision.Reason} {decision.SuggestedFix}";
        }

        internal static string BuildProviderVersionViolationMessage(
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            var decision = DecideProviderVersion(capabilities, actualServerVersion);
            return $"{decision.Reason} {decision.SuggestedFix}";
        }

        private static ProviderMobilityFeature RuntimeFeatureToMobilityFeature(string feature)
        {
            if (feature.Contains("FromSql", StringComparison.Ordinal) ||
                feature.Contains("QueryUnchanged", StringComparison.Ordinal))
                return ProviderMobilityFeature.RawSql;
            if (feature.Contains("StoredProcedure", StringComparison.Ordinal))
                return ProviderMobilityFeature.StoredProcedure;
            if (feature.Contains("CompiledQueryCommand", StringComparison.Ordinal) ||
                feature.Contains("ExecuteCompiledQueryList", StringComparison.Ordinal) ||
                feature.Contains("CompiledQueryMaterializer", StringComparison.Ordinal))
                return ProviderMobilityFeature.CompileTimeRawSql;
            if (feature.Equals("Query", StringComparison.Ordinal))
                return ProviderMobilityFeature.DynamicTableQuery;
            if (feature.Equals("Connection", StringComparison.Ordinal))
                return ProviderMobilityFeature.DirectConnectionAccess;
            if (feature.Equals("Provider", StringComparison.Ordinal))
                return ProviderMobilityFeature.DirectProviderAccess;
            if (feature.Equals("CurrentTransaction", StringComparison.Ordinal) ||
                feature.Equals("Transaction", StringComparison.Ordinal))
                return ProviderMobilityFeature.DirectTransactionAccess;
            if (feature.Contains("external DbTransaction", StringComparison.Ordinal))
                return ProviderMobilityFeature.ExternalTransactionWrite;
            if (feature.Contains("Savepoint", StringComparison.Ordinal))
                return ProviderMobilityFeature.RawTransactionSavepoint;
            if (feature.Contains("command interceptors", StringComparison.OrdinalIgnoreCase))
                return ProviderMobilityFeature.CommandInterceptor;
            if (feature.Contains("NativeTenant", StringComparison.Ordinal))
                return ProviderMobilityFeature.ProviderNativeTenantSecurity;
            if (feature.Contains("ProviderNativeTemporal", StringComparison.Ordinal))
                return ProviderMobilityFeature.ProviderNativeTemporal;

            return ProviderMobilityFeature.UnsupportedLinqShape;
        }

        private static IEnumerable<ProviderMobilityTranslationDecision> BuildDecisions()
        {
            yield return D(ProviderMobilityFeature.GeneratedLinqQuery, ProviderMobilitySupport.Portable, true, "Info",
                "nORM owns expression translation and provider SQL generation for the supported LINQ matrix.",
                "Keep data access on typed Query<T>(), compiled LINQ, and documented query modifiers.");
            yield return D(ProviderMobilityFeature.GeneratedWrite, ProviderMobilitySupport.Portable, true, "Info",
                "nORM owns generated insert/update/delete, SaveChanges, bulk, and tenant-aware write predicates.",
                "Use generated nORM write APIs instead of caller-owned DbCommand or raw SQL.");
            yield return D(ProviderMobilityFeature.GeneratedTenantBoundary, ProviderMobilitySupport.Portable, true, "Info",
                "nORM injects tenant predicates and includes tenant identity in generated query/cache/write paths.",
                "Use generated nORM query/write APIs; keep privileged raw paths outside certification or add database-native RLS separately.");
            yield return D(ProviderMobilityFeature.NormManagedTemporal, ProviderMobilitySupport.Emulated, true, "Info",
                "nORM preserves temporal behavior through provider-neutral history tables, tags and triggers.",
                "Use nORM-managed temporal storage for provider-mobile applications.");
            yield return D(ProviderMobilityFeature.WrappedSavepoint, ProviderMobilitySupport.Emulated, true, "Info",
                "nORM exposes a provider-neutral DbContextTransaction wrapper and owns provider savepoint dispatch.",
                "Use DbContextTransaction.CreateSavepointAsync/RollbackToSavepointAsync.");
            yield return D(ProviderMobilityFeature.ExplicitClientProjectionTail, ProviderMobilitySupport.Emulated, true, "Warning",
                "The server query remains provider-translated; only the final projection tail runs in CLR after filtering, ordering and paging.",
                "Keep ClientEvaluationPolicy.Warn as an explicit reviewed choice and avoid client work before server filtering or paging.");
            yield return D(ProviderMobilityFeature.SilentClientEvaluation, ProviderMobilitySupport.Unsupported, false, "Error",
                "Silent client evaluation hides portability and performance boundaries.",
                "Use ClientEvaluationPolicy.Throw, or Warn for explicit top-level projection tails.");
            yield return D(ProviderMobilityFeature.RawSql, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Raw SQL is caller-authored provider language; nORM cannot prove equivalent semantics on every provider.",
                "Replace with generated nORM LINQ/query APIs when possible, or keep it as explicit compatibility-mode provider code.");
            yield return D(ProviderMobilityFeature.StoredProcedure, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Stored procedures are provider-deployed code with provider-specific DDL, permissions and execution semantics.",
                "Move data access semantics to generated nORM APIs or maintain per-provider procedure implementations.");
            yield return D(ProviderMobilityFeature.CustomSqlFunction, ProviderMobilitySupport.ProviderBound, false, "Error",
                "User-authored SQL function format strings are dialect fragments.",
                "Use built-in nORM/provider translations or add nORM-owned provider translations with live parity tests.");
            yield return D(ProviderMobilityFeature.DynamicTableQuery, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Dynamic table queries reflect live provider schema into runtime CLR types, so nORM cannot certify a provider-neutral model contract.",
                "Use typed Query<T>() and nORM schema snapshots for strict provider mobility.");
            yield return D(ProviderMobilityFeature.DirectConnectionAccess, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Direct DbConnection access exposes provider ADO.NET behavior and caller-owned command language.",
                "Use generated nORM APIs from data access code; keep provider bootstrap in the composition root.");
            yield return D(ProviderMobilityFeature.DirectProviderAccess, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Direct DatabaseProvider access allows provider-specific SQL generation decisions outside the mobility layer.",
                "Use generated nORM APIs or provider-neutral capability/certification reports.");
            yield return D(ProviderMobilityFeature.DirectCommandAccess, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Direct DbCommand usage is caller-authored provider command construction.",
                "Use generated nORM query/write APIs.");
            yield return D(ProviderMobilityFeature.DirectTransactionAccess, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Raw DbTransaction access exposes provider ADO.NET transaction handles.",
                "Use Database.BeginTransactionAsync and DbContextTransaction wrapper APIs.");
            yield return D(ProviderMobilityFeature.ExternalTransactionWrite, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Write overloads with caller-owned DbTransaction bypass nORM's transaction ownership contract.",
                "Use Database.BeginTransactionAsync so nORM owns transaction binding.");
            yield return D(ProviderMobilityFeature.RawTransactionSavepoint, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Raw savepoint overloads require a caller-owned DbTransaction handle.",
                "Use DbContextTransaction savepoint wrapper methods.");
            yield return D(ProviderMobilityFeature.CommandInterceptor, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Command interceptors can inspect, rewrite, replace or suppress generated provider commands.",
                "Keep interceptors outside strict certification or prove an app-specific compatibility profile.");
            yield return D(ProviderMobilityFeature.ProviderNativeTenantSecurity, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Provider-native tenant security uses database-specific session state, RLS DDL and roles.",
                "Use generated-path tenant enforcement for provider mobility; add native RLS as explicit defense-in-depth deployment work.");
            yield return D(ProviderMobilityFeature.ProviderNativeTemporal, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Provider-native temporal/versioning uses database-specific DDL and query syntax.",
                "Use nORM-managed temporal history for provider mobility.");
            yield return D(ProviderMobilityFeature.CompileTimeRawSql, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Compile-time raw SQL and command materializer paths still embed caller-authored provider SQL or provider command handles.",
                "Use Norm.CompileQuery LINQ for provider-mobile compiled queries.");
            yield return D(ProviderMobilityFeature.ProviderBootstrap, ProviderMobilitySupport.ProviderBound, true, "Warning",
                "Applications must choose and open a concrete provider at the composition root, but this is not generated data-access semantics.",
                "Keep provider packages and connection construction isolated from strict-certified repositories/services.");
            yield return D(ProviderMobilityFeature.ProviderSpecificColumnType, ProviderMobilitySupport.ProviderBound, false, "Error",
                "Provider-specific column type strings bypass nORM's provider-neutral migration type mapping.",
                "Use CLR types, nORM migrations and provider-neutral schema metadata.");
            yield return D(ProviderMobilityFeature.SchemaUnsupportedClrType, ProviderMobilitySupport.Unsupported, false, "Error",
                "The schema contains a CLR type that is not in nORM's provider-mobile scalar type set.",
                "Use a supported scalar CLR type, enum, or value converter whose provider type is in nORM's provider-mobile type map.");
            yield return D(ProviderMobilityFeature.SchemaInvalidMetadata, ProviderMobilitySupport.Unsupported, false, "Error",
                "The schema snapshot is incomplete, invalid, duplicated, or cannot be inspected deterministically.",
                "Regenerate the snapshot from nORM mapping metadata and fix invalid identifiers, duplicate objects, missing CLR types, or invalid relations.");
            yield return D(ProviderMobilityFeature.SchemaNonPortableIdentity, ProviderMobilitySupport.Unsupported, false, "Error",
                "The schema uses an identity configuration that nORM cannot emit consistently across supported providers.",
                "Use an integral identity column type for provider-mobile generated migrations.");
            yield return D(ProviderMobilityFeature.SchemaProviderSpecificDefault, ProviderMobilitySupport.ProviderBound, false, "Error",
                "The schema default contains provider-specific SQL.",
                "Use an application-stamped value, a simple literal default, or an explicit provider-specific migration outside strict certification.");
            yield return D(ProviderMobilityFeature.SchemaDefaultNeedsReview, ProviderMobilitySupport.ProviderBound, true, "Warning",
                "The schema default looks function-based and may have provider-specific semantics.",
                "Verify the default on every target provider or replace it with an application-stamped value before strict certification.");
            yield return D(ProviderMobilityFeature.SchemaProviderGenerationFailed, ProviderMobilitySupport.Unsupported, false, "Error",
                "A provider migration generator could not emit DDL from the schema snapshot.",
                "Fix schema metadata so every target provider migration generator can emit DDL deterministically.");
            yield return D(ProviderMobilityFeature.SchemaMissingPrimaryKey, ProviderMobilitySupport.Emulated, true, "Warning",
                "The schema contains a keyless table; read-only queries may work, but generated writes, includes, tenant, and temporal behavior are constrained.",
                "Add a primary key for full generated behavior or document the shape as explicitly keyless/read-only.");
            yield return D(ProviderMobilityFeature.ProviderTargetUnknown, ProviderMobilitySupport.Unsupported, false, "Error",
                "The requested provider target is not recognized by nORM provider mobility certification.",
                "Use a supported provider target or add an explicit provider target descriptor before certification.");
            yield return D(ProviderMobilityFeature.ProviderTargetOpen, ProviderMobilitySupport.Unsupported, false, "Error",
                "The provider target could not be opened and validated.",
                "Fix the target connection string, driver, credentials, network access, or server version before claiming live provider mobility evidence.");
            yield return D(ProviderMobilityFeature.ProviderTargetCapability, ProviderMobilitySupport.Unsupported, false, "Error",
                "A provider target capability failed certification.",
                "Fix the target provider/version/capability profile or remove the target from the certified provider set.");
            yield return D(ProviderMobilityFeature.CertificationUnclassifiedFinding, ProviderMobilitySupport.Unsupported, false, "Error",
                "Provider mobility certification emitted a finding kind that is not classified by the translation layer.",
                "Add the finding kind to ProviderMobilityTranslator before relying on the certification report.");
            yield return D(ProviderMobilityFeature.ConstrainedLinqShape, ProviderMobilitySupport.Emulated, true, "Warning",
                "This LINQ shape has provider-specific constraints and is not automatically release-green for every target.",
                "Check the provider target profile and live-provider evidence, or rewrite to an unconstrained generated LINQ shape for strict all-provider portability.");
            yield return D(ProviderMobilityFeature.UnsupportedLinqShape, ProviderMobilitySupport.Unsupported, false, "Error",
                "nORM cannot preserve this LINQ shape safely across providers.",
                "Use a supported LINQ shape, precompute values in CLR, or materialize explicitly before applying CLR-only logic.");
        }

        private static ProviderMobilityTranslationDecision D(
            ProviderMobilityFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix)
            => new(feature, support, strictRuntimeAllowed, certificationSeverity, reason, suggestedFix);

        private static void Replace(List<ProviderMobilityProviderDecision> decisions, ProviderMobilityProviderDecision decision)
        {
            var index = decisions.FindIndex(existing => existing.Feature == decision.Feature);
            if (index >= 0)
                decisions[index] = decision;
            else
                decisions.Add(decision);
        }

        private static ProviderMobilityProviderDecision WithActualServerVersion(
            ProviderMobilityProviderDecision decision,
            Version actualServerVersion)
            => new(
                decision.ProviderName,
                decision.Feature,
                decision.Support,
                decision.StrictRuntimeAllowed,
                decision.CertificationSeverity,
                decision.Reason,
                decision.SuggestedFix,
                decision.MinimumServerVersion,
                actualServerVersion);

        private static ProviderMobilityProviderDecision BuildDescriptorOnlyProviderVersionDecision(ProviderCapabilities capabilities)
        {
            var minimum = capabilities.MinimumServerVersion;
            return minimum == null
                ? PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Warning",
                    $"{capabilities.ProviderName} does not declare a minimum server version, so nORM cannot certify a version floor for this provider target.",
                    "Declare ProviderCapabilities.MinimumServerVersion for release-certified providers or keep the target as a reviewed compatibility profile.",
                    null,
                    null)
                : PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    $"{capabilities.ProviderName} declares minimum supported server version {minimum}. No live target was opened, so this is descriptor evidence rather than actual server-version evidence.",
                    "Pass the provider connection string to certification when a release artifact must prove the actual connected server version.",
                    minimum,
                    null);
        }

        private static bool IsExpandedNullSemantics(string sql)
            => sql.Contains(" OR ", StringComparison.OrdinalIgnoreCase) &&
               sql.Contains(" IS NULL", StringComparison.OrdinalIgnoreCase);

        private static bool IsAlgebraicXorRewrite(string sql)
            => sql.Contains("|", StringComparison.Ordinal) &&
               sql.Contains("&", StringComparison.Ordinal) &&
               sql.Contains("-", StringComparison.Ordinal);

        private static ProviderMobilityProviderDecision BuildJsonPathTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            try
            {
                var jsonSql = provider.TranslateJsonPathAccess("doc", "$.customer.name");
                try
                {
                    _ = provider.TranslateJsonPathAccess("doc", "$.bad'value");
                }
                catch (ArgumentException)
                {
                    return PD(
                        capabilities.ProviderName,
                        ProviderMobilityProviderFeature.JsonPathTranslation,
                        ProviderMobilitySupport.Portable,
                        true,
                        "Info",
                        $"{capabilities.ProviderName} translates JSON path access through provider-owned SQL ({jsonSql}) and rejects unsafe path text.",
                        "Keep JSON path validation and live-provider JSON parity tests current.",
                        capabilities.MinimumServerVersion,
                        actualServerVersion);
                }

                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.JsonPathTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} JSON path translation did not reject unsafe path text during capability probing.",
                    "Harden provider JSON path validation before certifying JSON query shapes for this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
            catch (Exception ex) when (ex is ArgumentException or NormUnsupportedFeatureException)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.JsonPathTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    ex.Message,
                    "Add provider JSON path translation and validation tests before certifying JSON query shapes for this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
        }

        private static ProviderMobilityProviderDecision BuildTypeConversionDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var conversions = new[]
            {
                provider.GetToStringSql("value"),
                provider.GetIntCastSql("value"),
                provider.GetIntCastSql("value", asLong: true),
                provider.GetRealCastSql("value"),
                provider.GetRealCastSql("value", asDecimal: true),
                provider.GetBoolCastSql("value"),
                provider.GetTruncateToIntSql("value")
            };

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.TypeConversionTranslation,
                conversions.All(static sql => !string.IsNullOrWhiteSpace(sql))
                    ? ProviderMobilitySupport.Portable
                    : ProviderMobilitySupport.Unsupported,
                conversions.All(static sql => !string.IsNullOrWhiteSpace(sql)),
                conversions.All(static sql => !string.IsNullOrWhiteSpace(sql)) ? "Info" : "Error",
                $"{capabilities.ProviderName} exposes provider-owned CAST/conversion SQL for string, integer, real, decimal, boolean, and truncation paths.",
                "Keep Convert.*, casts, enum casts, ToString, and numeric conversion parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildStringPredicateDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var emptySql = provider.IsNullOrEmptySql("value");
            var sqlServerByteExact = provider is SqlServerProvider;

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.StringPredicateTranslation,
                sqlServerByteExact ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                sqlServerByteExact ? "Warning" : "Info",
                sqlServerByteExact
                    ? $"{capabilities.ProviderName} uses byte-exact empty-string SQL ({emptySql}) to avoid SQL Server trailing-space equality drift."
                    : $"{capabilities.ProviderName} translates string null/empty predicates through provider-owned SQL ({emptySql}).",
                "Keep IsNullOrEmpty, IsNullOrWhiteSpace, Length, Contains, StartsWith, EndsWith, and trim predicate parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildCharacterTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var codeSql = provider.GetCharCodeSql("ch");
            var charSql = provider.GetCharFromCodeSql("65");

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.CharacterTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} translates character classification through provider-owned code-point SQL ({codeSql}; {charSql}).",
                "Keep char.IsDigit/IsLetter/IsWhiteSpace/IsControl/IsPunctuation/IsSymbol parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildFormattingTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var decimalSql = provider.FormatFixedDecimalSql("amount", 2);
            var dateSql = provider.FormatDateUsingDotNetPattern("stamp", "yyyy-MM-dd");
            var supported = !string.IsNullOrWhiteSpace(decimalSql) && !string.IsNullOrWhiteSpace(dateSql);

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.FormattingTranslation,
                supported ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Unsupported,
                supported,
                supported ? "Info" : "Error",
                supported
                    ? $"{capabilities.ProviderName} translates fixed decimal and date/time format strings through provider-owned SQL ({decimalSql}; {dateSql})."
                    : $"{capabilities.ProviderName} does not expose both fixed decimal and supported date/time format translation.",
                "Keep ToString(format) parity tests for numeric and temporal values current; add provider mappings before certifying new format tokens.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildInsertOrIgnoreDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var sql = provider.GetInsertOrIgnoreSql("JoinTable", "LeftId", "RightId", "@p0", "@p1");
            var emulated = provider is SqlServerProvider;

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.InsertOrIgnoreTranslation,
                emulated ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                emulated ? "Warning" : "Info",
                emulated
                    ? $"{capabilities.ProviderName} preserves insert-or-ignore semantics through generated IF NOT EXISTS SQL ({sql})."
                    : $"{capabilities.ProviderName} uses a native idempotent insert primitive ({sql}).",
                "Keep many-to-many sync and idempotent join insert parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildOrderedStringAggregateDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            if (!provider.SupportsNativeOrderedStringAggregate)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.OrderedStringAggregate,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Provider lacks native ordered string aggregate support; nORM must preserve aggregate semantics through provider-specific rewrites or bounded fallback behavior.",
                    "Keep string aggregate ordering caveats documented and covered by provider parity tests.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }

            var featureMinimum = provider is SqlServerProvider ? new Version(14, 0) : capabilities.MinimumServerVersion;
            if (featureMinimum != null && actualServerVersion != null && actualServerVersion < featureMinimum)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.OrderedStringAggregate,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} ordered string aggregate translation requires server version {featureMinimum} or newer; actual version is {actualServerVersion}.",
                    "Upgrade the target server or avoid ordered string aggregate LINQ shapes for this provider target.",
                    featureMinimum,
                    actualServerVersion);
            }

            if (featureMinimum != null && actualServerVersion == null && featureMinimum > (capabilities.MinimumServerVersion ?? featureMinimum))
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.OrderedStringAggregate,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Warning",
                    $"{capabilities.ProviderName} ordered string aggregate translation requires server version {featureMinimum} or newer, which is above the provider floor {capabilities.MinimumServerVersion}.",
                    "Pass a live target connection to certification before relying on ordered string aggregate translation for this provider target.",
                    featureMinimum,
                    null);
            }

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.OrderedStringAggregate,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                featureMinimum != null
                    ? $"Provider supports native ordered string aggregate translation at server version {actualServerVersion ?? featureMinimum}."
                    : "Provider supports native ordered string aggregate translation.",
                "Keep aggregate ordering parity tests current.",
                featureMinimum,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildRegexTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            try
            {
                var matchSql = provider.GetRegexMatchSql("value", "'^[A-Z]'");
                var replaceSql = provider.GetRegexReplaceSql("value", "'[0-9]+'", "'#'");
                var sqliteManagedBacked = provider is SqliteProvider;
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.RegexTranslation,
                    sqliteManagedBacked ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    sqliteManagedBacked
                        ? $"{capabilities.ProviderName} emits regex SQL ({matchSql}; {replaceSql}) backed by deterministic nORM-registered managed SQLite functions."
                        : $"{capabilities.ProviderName} translates Regex.IsMatch/Regex.Replace through provider regex primitives ({matchSql}; {replaceSql}).",
                    sqliteManagedBacked
                        ? "Keep SQLite managed regex function registration and live-provider parity tests current."
                        : "Keep Regex.IsMatch/Regex.Replace shape and live-provider parity tests current.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
            catch (NormUnsupportedFeatureException ex)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.RegexTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Warning",
                    ex.Message,
                    $"Avoid Regex LINQ shapes for {capabilities.ProviderName}, rewrite simple patterns to supported string predicates, or add an explicit provider-owned regex function with live parity tests.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
        }

        private static ProviderMobilityProviderDecision BuildTemporalConstructionDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            try
            {
                var dateTime = provider.GetDateTimeFromPartsSql("y", "m", "d");
                _ = provider.GetDateTimeFromPartsSql("y", "m", "d", "hh", "mm", "ss");
                _ = provider.GetDateTimeFromPartsSql("y", "m", "d", "hh", "mm", "ss", "ms");
                _ = provider.GetDateOnlyFromPartsSql("y", "m", "d");
                var timeOnly = provider.GetTimeOnlyFromPartsSql("hh", "mm", "ss");
                _ = provider.GetTimeOnlyFromPartsSql("hh", "mm", "ss", "ms");
                _ = provider.GetDateTimeOffsetFromPartsSql("y", "m", "d", "hh", "mm", "ss", TimeSpan.Zero);

                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.TemporalConstructionTranslation,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    $"{capabilities.ProviderName} exposes provider-owned DateTime/DateOnly/TimeOnly/DateTimeOffset from-parts translation ({dateTime}; {timeOnly}).",
                    "Keep DateTime/DateOnly/TimeOnly/DateTimeOffset from-parts parity tests current.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
            catch (NormUnsupportedFeatureException ex)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.TemporalConstructionTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    ex.Message,
                    "Add provider from-parts translations and live/provider-shape tests before certifying temporal construction on this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
        }

        private static ProviderMobilityProviderDecision BuildTemporalArithmeticDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var missing = new List<string>();

            Require("DateOnly.AddDays", provider.AddDaysToDateOnlySql("d", "1"));
            Require("DateOnly.AddMonths", provider.AddMonthsToDateOnlySql("d", "1"));
            Require("DateOnly.AddYears", provider.AddYearsToDateOnlySql("d", "1"));
            Require("TimeOnly.AddSeconds", provider.AddSecondsToTimeOnlySql("t", "1"));
            Require("TimeOnly +/- TimeSpan column", provider.AddTimeSpanColumnToTimeOnlySql("t", "span", subtract: false));
            Require("DateTime +/- TimeSpan column", provider.AddTimeSpanColumnToDateTimeSql("dt", "span", subtract: false));
            Require("DateTimeOffset +/- TimeSpan column", provider.AddTimeSpanColumnToDateTimeOffsetSql("dto", "span", subtract: false));

            try
            {
                _ = provider.GetDateTimeDifferenceSecondsSql("end", "start");
                _ = provider.GetTimeOnlyDifferenceSecondsSql("end", "start");
                _ = provider.GetTimeSpanColumnSecondsSql("span");
                _ = provider.GetDateTimeOffsetWithOffsetSql("dto", TimeSpan.Zero);
                _ = provider.GetDateTimeOffsetLocalDateTimeSql("dto", TimeSpan.Zero);
                _ = provider.GetDateTimeOffsetUtcEpochSecondsSql("dto");
            }
            catch (NormUnsupportedFeatureException ex)
            {
                missing.Add(ex.Message);
            }

            if (missing.Count > 0)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.TemporalArithmeticTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} is missing temporal arithmetic translations: {string.Join("; ", missing)}.",
                    "Add the missing provider hooks and live/provider-shape tests before certifying temporal arithmetic on this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }

            var sqliteTextArithmetic = provider is SqliteProvider;
            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.TemporalArithmeticTranslation,
                sqliteTextArithmetic ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                sqliteTextArithmetic ? "Warning" : "Info",
                sqliteTextArithmetic
                    ? $"{capabilities.ProviderName} preserves temporal arithmetic through generated TEXT/julianday/strftime/substr normalization."
                    : $"{capabilities.ProviderName} preserves temporal arithmetic through provider-native date/time/interval primitives.",
                sqliteTextArithmetic
                    ? "Keep SQLite temporal precision caveats documented and tolerance tests current."
                    : "Keep temporal arithmetic live-provider parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);

            void Require(string label, string? sql)
            {
                if (string.IsNullOrWhiteSpace(sql))
                    missing.Add(label);
            }
        }

        private static ProviderMobilityProviderDecision BuildNormalizationDecision(
            ProviderCapabilities capabilities,
            Version? actualServerVersion,
            ProviderMobilityProviderFeature feature,
            string translatedSql,
            string label,
            string suggestedFix,
            bool warnWhenEmulated = false)
        {
            var isIdentity = translatedSql.Equals("value", StringComparison.Ordinal);
            return PD(
                capabilities.ProviderName,
                feature,
                isIdentity ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                !isIdentity && warnWhenEmulated ? "Warning" : "Info",
                isIdentity
                    ? $"{capabilities.ProviderName} uses native {label} semantics without generated normalization."
                    : $"{capabilities.ProviderName} normalizes {label} with provider SQL: {translatedSql}.",
                suggestedFix,
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision CapabilityBool(
            ProviderCapabilities capabilities,
            ProviderMobilityProviderFeature feature,
            bool supported,
            string supportedReason,
            string unsupportedReason,
            string unsupportedFix)
            => supported
                ? PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    supportedReason,
                    "Keep provider capability tests and live-provider parity evidence current.",
                    capabilities.MinimumServerVersion,
                    null)
                : PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    unsupportedReason,
                    unsupportedFix,
                    capabilities.MinimumServerVersion,
                    null);

        private static ProviderMobilityProviderDecision PD(
            string providerName,
            ProviderMobilityProviderFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix,
            Version? minimumServerVersion,
            Version? actualServerVersion)
            => new(providerName, feature, support, strictRuntimeAllowed, certificationSeverity, reason, suggestedFix, minimumServerVersion, actualServerVersion);
    }
}
