using System.Collections.Generic;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
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
    }
}
