#nullable enable

namespace nORM.Configuration
{
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
}
