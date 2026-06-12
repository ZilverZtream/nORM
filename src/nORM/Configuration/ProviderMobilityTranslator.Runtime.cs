using System;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        /// <summary>
        /// Gets the provider-mobility decision for a runtime strict-mode feature name.
        /// </summary>
        public static ProviderMobilityTranslationDecision DecideRuntimeFeature(string feature)
            => Decide(RuntimeFeatureToMobilityFeature(feature));

        internal static string BuildStrictViolationMessage(string runtimeFeature)
        {
            var decision = DecideRuntimeFeature(runtimeFeature);
            return $"{runtimeFeature} is outside nORM's strict provider mobility contract. " +
                   $"{decision.Reason} {decision.SuggestedFix}";
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
    }
}
