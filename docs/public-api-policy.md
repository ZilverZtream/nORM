# Public API Policy

nORM is pre-1.0 (0.x); the public API is being stabilized toward a future 1.0
(see `RELEASE.md`). Public API changes must still be intentional, reviewed, and
reflected in the shipped API baseline.

## Baseline

The public API baseline is stored in:

```text
tests/PublicApi.Shipped.txt
```

`PublicApiSnapshotTests.Public_api_matches_v1_baseline` compares the exported
surface of `nORM.dll` against that file. A normal test run fails when public
types, members, fields, events, constructors, or method signatures change.

`PublicApiClassificationTests` uses `docs/namespace-policy.md` as the support-tier
source of truth. It verifies every non-comment entry in
`tests/PublicApi.Shipped.txt` resolves to a shipped public type whose namespace
has a documented tier, and it fails when the namespace policy has missing or
stale entries.

## Updating The Baseline

Only update the baseline for a reviewed public API change:

```powershell
$env:NORM_UPDATE_PUBLIC_API = '1'
dotnet test tests\nORM.Tests.csproj -c Release --filter "FullyQualifiedName~PublicApiSnapshotTests"
Remove-Item Env:\NORM_UPDATE_PUBLIC_API
```

The API diff must be reviewed as part of the pull request or release branch
change. Accidental public API exposure should be fixed in code instead of added
to the baseline.

## v1.0 Rules

- Breaking changes require an explicit v1.0 readiness decision before the final
  release branch.
- New public members need XML documentation and tests that exercise the supported
  behavior.
- Experimental surface should be internal or clearly documented before v1.0.
- Provider-specific behavior must be documented when it differs between SQLite,
  SQL Server, PostgreSQL, and MySQL.

## Reviewed Public API Additions

The following public surface was added during v1 preparation and is intentionally part of the
v1.0 contract:

| Member | Status | Tested by | Documented in |
|---|---|---|---|
| `nORM.Core.Norm.CompileTerminalQuery<TContext, TParam, TResult>(...)` | Stable | `CompiledTerminalQueryTests` | `README.md` compiled queries section |
| `nORM.Configuration.DbContextOptions.MaxQueryJoinDepth` / `MaxQueryWhereConditions` / `MaxQueryParameterCount` / `MaxQueryComplexityCost` (nullable int overrides for memory-scaled query admission limits) | Stable | `QueryComplexityLimitConfigurationTests` | `docs/production-operations.md` Query Admission Limits section |
| `nORM.Mapping.RenameColumnAttribute` | Stable | `MigrationRenameTests`, `MigrationRenameDocContractTests` | `README.md` migration section |
| `nORM.Configuration.EntityTypeBuilder<TEntity>.PropertyBuilder.HasMaxLength(int)` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Configuration.EntityTypeBuilder<TEntity>.PropertyBuilder.IsUnicode(bool)` / `IsFixedLength(bool)` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Configuration.EntityTypeBuilder<TEntity>.PropertyBuilder.HasPrecision(int)` / `HasPrecision(int, int)` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Configuration.EntityTypeBuilder<TEntity>.HasOne<TDependent>(...)` and nested one-to-one relationship builders | Stable | `RelationshipConfigurationTests`, `IncludeContractTests`, `ScaffoldingAndNavigationCoverageTests` | `docs/scaffolding.md` |
| `nORM.Configuration.IEntityTypeConfiguration.MaxLengths` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Configuration.IEntityTypeConfiguration.UnicodeSettings` / `FixedLengthSettings` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Configuration.IEntityTypeConfiguration.Precisions` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Configuration.PrecisionConfiguration` | Stable | `SchemaSnapshotTests` | `docs/scaffolding.md` |
| `nORM.Migration.ColumnSchema.MaxLength` / `IsUnicode` / `IsFixedLength` | Stable | `SchemaSnapshotTests`, `MigrationDefaultsIdentityTests` | `docs/scaffolding.md` |
| `nORM.Migration.ColumnSchema.PreviousName` | Stable | `MigrationRenameTests`, `SchemaSnapshotTests` | `README.md` migration section |
| `nORM.Migration.SchemaDiff.RenamedColumns` | Stable | `MigrationRenameTests` | `README.md` migration section |
| `nORM.Providers.SqlServerProvider(IDbParameterFactory)` | Stable - dialect-only mode | `TestBase.CreateProvider`, `ProviderCapabilitiesTests`, cross-provider parity suite | `docs/provider-packages.md`; matches the existing `PostgresProvider(IDbParameterFactory)` and `MySqlProvider(IDbParameterFactory)` constructors |
| `nORM.SourceGeneration.CompiledMaterializerStore.AddPermanent<T>` | Stable - source-generator registration helper | `SourceGeneratorIntegrationTests`, `SourceGenMaterializerCorrectnesTests` | `docs/source-generation.md` |
| `nORM.Scaffolding.ScaffoldOptions.Tables` / `nORM.Scaffolding.ScaffoldOptions.Schemas` | Stable - scaffold selection filters | `DatabaseScaffolderFilterTests`, `LiveProviderScaffoldingFilterTests`, `LiveProviderScaffoldCliSchemaFilterTests` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.UseNullableReferenceTypes` | Stable - scaffold output mode | `ScaffoldAsync_WithNullableReferenceTypesDisabled_EmitsNullableDisabledCode`, `Scaffold_project_nullable_disable_generates_nullable_disabled_code`, `Scaffold_project_inherits_nullable_enable_from_directory_build_props` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.UsePluralizer` | Stable - scaffold naming mode | `ScaffoldAsync_WithPluralTableNames_SingularizesEntitiesAndKeepsPluralQueryProperties`, `ScaffoldAsync_WithPluralizerDisabled_PreservesEntityAndQueryNames`, `Scaffold_no_pluralize_preserves_entity_and_query_names` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.UseDatabaseNames` | Stable - scaffold naming mode | `ScaffoldAsync_WithUseDatabaseNames_PreservesLegalDatabaseNames`, `ScaffoldAsync_WithUseDatabaseNames_DoesNotDoublePluralizePreservedNames`, `Dotnet_norm_scaffold_preserves_database_names_on_live_provider` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.NoRelationships` | Stable - scaffold relationship suppression mode | `ScaffoldAsync_WithNoRelationships_EmitsScalarForeignKeysWithoutNavigations`, `Scaffold_no_relationships_omits_navigation_properties_and_model_relationships` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.ContextDirectory` / `nORM.Scaffolding.ScaffoldOptions.ContextOutputDirectory` / `nORM.Scaffolding.ScaffoldOptions.ContextNamespace` | Stable - scaffold context placement | `DatabaseScaffolderPublicApiTests`, `LiveProviderScaffoldingOutputOptionTests`, `LiveProviderScaffoldCliProjectConfigurationTests` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.OverwriteFiles` / `nORM.Scaffolding.ScaffoldOptions.DryRun` / `nORM.Scaffolding.ScaffoldOptions.FailOnWarnings` | Stable - scaffold output safety and CI modes | `DatabaseScaffolderBasicScaffoldTests`, `DatabaseScaffolderOutputTests`, `DatabaseScaffolderKeylessSafetyTests`, `LiveProviderScaffoldingOutputOptionTests`, `LiveProviderScaffoldingWarningOptionTests` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.EmitRoutineStubs` / `nORM.Scaffolding.ScaffoldOptions.EmitSequenceStubs` | Stable - provider-bound scaffold wrapper opt-ins | `LiveProviderScaffoldingRoutineTests`, `LiveProviderScaffoldingRoutineOutputTests`, `LiveProviderScaffoldCliRoutineSequenceTests` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Scaffolding.ScaffoldOptions.EmitViewEntities` / `nORM.Scaffolding.ScaffoldOptions.EmitQueryArtifacts` | Stable - scaffold query-artifact opt-ins | `DatabaseScaffolderDiagnosticsTests`, `DatabaseScaffolderOutputTests`, `LiveProviderScaffoldingQueryArtifactTests`, `LiveProviderScaffoldCliQueryArtifactTests` | `docs/scaffolding.md`, `src/dotnet-norm/README.md`, `README.md` |
| `nORM.Internal.ConcurrentLruCache<TKey, TValue>` | Stable for v1.0 compatibility; deprecated namespace closed to new public additions | `ConcurrentLruCachePublicApiTests`, `ConcurrentLruCacheStressTests`, `NamespacePolicyContractTests` | `docs/namespace-policy.md`; planned v1.x relocation to `nORM.Caching` with type forwarding |
| `nORM.Internal.ParameterOptimizer` | Stable for v1.0 compatibility; deprecated namespace closed to new public additions | `ParameterOptimizerPublicApiTests`, `CompileTimeQueryParameterParityTests`, `NamespacePolicyContractTests` | `docs/namespace-policy.md`; planned v1.x relocation to `nORM.Diagnostics` with type forwarding |
| `Microsoft.Extensions.DependencyInjection.NormServiceCollectionExtensions.AddNorm(...)` / `AddNorm<TContext>(...)` / `AddNormFactory<TContext>(...)` | Stable - hosting / DI integration | `NormServiceCollectionExtensionsTests` | `README.md` dependency-injection section |
| `nORM.Core.INormDbContextFactory<TContext>` | Stable - caller-owned context factory | `NormServiceCollectionExtensionsTests` | `README.md` dependency-injection section |
| `nORM.Core.NormQueryable.Set<T>(this DbContext)` | Stable - EF Core-style query entry-point alias for `Query<T>` | `NormQueryableSetTests` | `README.md` Quick Start (LINQ Queries) |
| `nORM.Core.PropertyValues` (`this[string]` / `Properties` / `GetValue<T>` / `SetValues` / `ToObject`) | Stable - EF Core-style entity property bag | `EntityEntryPropertyValuesContractTests` | `docs/api/nORM.Core.PropertyValues.yml` |
| `nORM.Core.EntityEntry.CurrentValues` / `nORM.Core.EntityEntry.OriginalValues` | Stable - EF Core-style current/original value bags | `EntityEntryPropertyValuesContractTests` | `docs/api/nORM.Core.EntityEntry.yml` |
| `nORM.Core.EntityEntry.Reload()` / `nORM.Core.EntityEntry.ReloadAsync(CancellationToken)` | Stable - EF Core-style refresh from the database | `EntityEntryReloadContractTests` | `docs/api/nORM.Core.EntityEntry.yml` |

`PublicApiSnapshotTests.Public_api_matches_v1_baseline` pins the exact shape of each entry;
any future change requires updating `tests/PublicApi.Shipped.txt` and this table together.
