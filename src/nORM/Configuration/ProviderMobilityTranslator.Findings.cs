using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
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
    }
}
