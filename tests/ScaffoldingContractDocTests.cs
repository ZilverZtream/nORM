using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the scaffolding scope documented in <c>docs/scaffolding.md</c> against the public
/// surface in <c>nORM.Scaffolding.*</c>. The doc is the consumer-facing scope statement;
/// runtime evidence lives in the focused <c>DatabaseScaffolder*</c> test files and the
/// per-provider scaffolder tests inside the CLI suite.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public partial class ScaffoldingContractDocTests
{
    [Fact]
    public void Doc_describes_bounded_v1_contract()
    {
        var doc = ReadDoc();
        Assert.Contains("Scaffolding Contract", doc, StringComparison.Ordinal);
        Assert.Contains("bounded v1", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("not a database-first completeness claim", doc, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("SQL Server")]
    [InlineData("PostgreSQL")]
    [InlineData("MySQL")]
    [InlineData("SQLite")]
    public void Doc_lists_provider_support(string provider)
    {
        var doc = ReadDoc();
        Assert.Contains(provider, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_provider_catalog_store_type_mapping()
    {
        var doc = ReadDoc();
        var staticSource = ReadStaticEntityScaffoldSource();
        var dynamicSource = ReadDynamicEntitySource();
        var liveProviderSource = ReadLiveProviderScaffoldingParitySource();
        var liveCliSource = ReadLiveProviderScaffoldCliParitySource();

        Assert.Contains("Provider catalog `date`/`time` store types map to CLR temporal types where the mapping is unambiguous", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server and PostgreSQL `date`/`time`, MySQL `date`, PostgreSQL `interval`,", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server `datetimeoffset`, and SQLite declared", doc, StringComparison.Ordinal);
        Assert.Contains("`DATE`/`TIME`/`DATETIME`/`TIMESTAMP`/`DATETIMEOFFSET`", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL `TIME` is left to", doc, StringComparison.Ordinal);
        Assert.Contains("provider metadata rather than guessed", doc, StringComparison.Ordinal);
        Assert.Contains("Real-provider static, runtime dynamic, and real CLI scaffold tests cover this", doc, StringComparison.Ordinal);
        Assert.Contains("GetColumnStoreTypesAsync", staticSource, StringComparison.Ordinal);
        Assert.Contains("ColumnStoreTypesByTable", staticSource, StringComparison.Ordinal);
        Assert.Contains("TryMapStoreType(provider, columnStoreType", staticSource, StringComparison.Ordinal);
        Assert.Contains("GetColumnStoreTypes", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("TryMapStoreType(connection, columnStoreType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_maps_temporal_catalog_store_types_on_live_provider", liveProviderSource, StringComparison.Ordinal);
        Assert.Contains("Dynamic_scaffolding_maps_temporal_catalog_store_types_on_live_provider", liveProviderSource, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_maps_temporal_store_types_on_live_provider", liveCliSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_bounds_schema_preservation_by_provider()
    {
        var doc = ReadDoc();
        var routineStubWriter = ReadRoutineStubWriterSource();
        var sequenceStubWriter = ReadSequenceStubWriterSource();

        Assert.Contains("Schema-qualified table names are preserved for SQL Server, PostgreSQL, and", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL scaffolding uses the current database for", doc, StringComparison.Ordinal);
        Assert.Contains("does not emit the database/catalog name as a model schema", doc, StringComparison.Ordinal);
        Assert.Contains("same table name appears in multiple schemas", doc, StringComparison.Ordinal);
        Assert.Contains("include the schema name", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-bound routine and sequence stubs follow the same rule", doc, StringComparison.Ordinal);
        Assert.Contains("schema-prefixed generated method and DTO names", doc, StringComparison.Ordinal);
        Assert.Contains("Same-schema routine overloads use discovered input-type", doc, StringComparison.Ordinal);
        Assert.Contains("schema-prefixed generated method", doc, StringComparison.Ordinal);
        Assert.Contains("avoid the enclosing entity type name", doc, StringComparison.Ordinal);
        Assert.Contains("GetSchemaAwareRoutineMemberName", routineStubWriter, StringComparison.Ordinal);
        Assert.Contains("BuildSameSchemaOverloadSuffixes", routineStubWriter, StringComparison.Ordinal);
        Assert.Contains("TryBuildRoutineSignatureNamePart", routineStubWriter, StringComparison.Ordinal);
        Assert.Contains("FindDuplicateNames", routineStubWriter, StringComparison.Ordinal);
        Assert.Contains("Default_\" + routine.Name", routineStubWriter, StringComparison.Ordinal);
        Assert.Contains("routine.Schema + \"_\" + routine.Name", routineStubWriter, StringComparison.Ordinal);
        Assert.Contains("GetSchemaAwareSequenceMemberName", sequenceStubWriter, StringComparison.Ordinal);
        Assert.Contains("Default_\" + sequence.Name", sequenceStubWriter, StringComparison.Ordinal);
        Assert.Contains("sequence.Schema + \"_\" + sequence.Name", sequenceStubWriter, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_scaffold_length_round_trip()
    {
        var doc = ReadDoc();
        var snapshot = ReadSchemaSnapshotSource();
        var entityTypeBuilder = ReadEntityTypeBuilderSource();
        var entityTypeConfiguration = ReadRepoFile("src", "nORM", "Configuration", "IEntityTypeConfiguration.cs");
        var scaffolder = ReadDatabaseScaffolderSource();
        var featureConfigurationBuilder = ReadFeatureConfigurationBuilderSource();
        var contextWriterSource = ReadContextWriterSource();
        var sqlServerMigration = ReadRepoFile("src", "nORM", "Migration", "SqlServerMigrationSqlGenerator.cs");
        var postgresMigration = ReadRepoFile("src", "nORM", "Migration", "PostgresMigrationSqlGenerator.cs");
        var mysqlMigration = ReadRepoFile("src", "nORM", "Migration", "MySqlMigrationSqlGenerator.cs");

        Assert.Contains("migration generators round-trip bounded string/binary facets", doc, StringComparison.Ordinal);
        Assert.Contains("Property(...).HasMaxLength(n)", doc, StringComparison.Ordinal);
        Assert.Contains("Property(...).IsUnicode(false)", doc, StringComparison.Ordinal);
        Assert.Contains("Property(...).IsFixedLength()", doc, StringComparison.Ordinal);
        Assert.Contains("Property(...).HasPrecision(p, s)", doc, StringComparison.Ordinal);
        Assert.Contains("Property(...).HasPrecision(p)", doc, StringComparison.Ordinal);
        Assert.Contains("HasMaxLength(int length)", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("IsUnicode(bool unicode", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("IsFixedLength(bool fixedLength", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("HasPrecision(int precision", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("SetMaxLength", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("SetUnicode", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("SetFixedLength", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("SetPrecision", entityTypeBuilder, StringComparison.Ordinal);
        Assert.Contains("IReadOnlyDictionary<PropertyInfo, int> MaxLengths", entityTypeConfiguration, StringComparison.Ordinal);
        Assert.Contains("IReadOnlyDictionary<PropertyInfo, bool> UnicodeSettings", entityTypeConfiguration, StringComparison.Ordinal);
        Assert.Contains("IReadOnlyDictionary<PropertyInfo, bool> FixedLengthSettings", entityTypeConfiguration, StringComparison.Ordinal);
        Assert.Contains("IReadOnlyDictionary<PropertyInfo, PrecisionConfiguration> Precisions", entityTypeConfiguration, StringComparison.Ordinal);
        Assert.Contains("ScaffoldColumnFacetConfiguration", scaffolder, StringComparison.Ordinal);
        Assert.Contains("BuildColumnFacetConfigurations", featureConfigurationBuilder, StringComparison.Ordinal);
        Assert.Contains("ScaffoldPrecisionConfiguration", scaffolder, StringComparison.Ordinal);
        Assert.Contains("BuildPrecisionConfigurations", featureConfigurationBuilder, StringComparison.Ordinal);
        Assert.Contains(".IsUnicode(", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains(".IsFixedLength(", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains(".HasPrecision(", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("public int? MaxLength", snapshot, StringComparison.Ordinal);
        Assert.Contains("public bool? IsUnicode", snapshot, StringComparison.Ordinal);
        Assert.Contains("public bool IsFixedLength", snapshot, StringComparison.Ordinal);
        Assert.Contains("MaxLengthAttribute", snapshot, StringComparison.Ordinal);
        Assert.Contains("StringLengthAttribute", snapshot, StringComparison.Ordinal);
        Assert.Contains("NarrowsMaxLength", snapshot, StringComparison.Ordinal);
        Assert.Contains("TryGetBoundedStringOrBinaryType", sqlServerMigration, StringComparison.Ordinal);
        Assert.Contains("TryGetBoundedStringType", postgresMigration, StringComparison.Ordinal);
        Assert.Contains("TryGetBoundedStringOrBinaryType", mysqlMigration, StringComparison.Ordinal);
        Assert.Contains("TryGetDecimalWithPrecisionType", sqlServerMigration, StringComparison.Ordinal);
        Assert.Contains("TryGetDecimalWithPrecisionType", postgresMigration, StringComparison.Ordinal);
        Assert.Contains("TryGetDecimalWithPrecisionType", mysqlMigration, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_separates_supported_from_not_yet_stable_scope()
    {
        var doc = ReadDoc();
        Assert.Contains("Supported", doc, StringComparison.Ordinal);
        Assert.Contains("Not Yet Stable", doc, StringComparison.Ordinal);
        Assert.Contains("bridge columns are", doc, StringComparison.Ordinal);
        Assert.Contains("neither an exact FK-column primary key nor a generated surrogate key", doc, StringComparison.Ordinal);
        Assert.Contains("exact FK-column unique index", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Source_pins_explicit_system_usings_for_generated_scaffold_artifacts()
    {
        var source = ReadStaticEntityScaffoldSource();
        var entityWriterSource = ReadEntityWriterSource();
        var contextWriterSource = ReadContextWriterSource();
        var skippedDiscoverySource = ReadSkippedObjectDiscoverySource();
        var entityStart = source.IndexOf("public static async Task<string> BuildAsync", StringComparison.Ordinal);
        var entityEnd = source.IndexOf("public static string BuildSchemaProbeSql", entityStart, StringComparison.Ordinal);

        Assert.True(entityStart >= 0);
        Assert.True(entityEnd > entityStart);

        var entitySource = source[entityStart..entityEnd];
        Assert.Contains("sb.AppendLine(\"using System;\");", entityWriterSource, StringComparison.Ordinal);
        Assert.Contains("context.RoutineStubs.Count > 0", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("sb.AppendLine(\"using System;\");", contextWriterSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Source_pins_scaffold_provider_dispatch_helper()
    {
        var helper = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderKind.cs");
        var dispatchSource = ReadScaffoldProviderDispatchSource();
        var keyDiscoverySource = ReadKeyDiscoverySource();

        Assert.Contains("internal static class ScaffoldProviderKind", helper, StringComparison.Ordinal);
        Assert.Contains("IsSqlite(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("IsSqlServer(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("IsPostgres(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("IsMySql(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsSqlite(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsSqlServer(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsPostgres(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsMySql(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("GetSqlitePrimaryKeyConstraintNameMapAsync", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ExtractPrimaryKeyConstraintName", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("MySqlPrimaryKeyConstraintNameSql", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("information_schema.table_constraints", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("constraint_type = 'PRIMARY KEY'", keyDiscoverySource, StringComparison.Ordinal);
        Assert.DoesNotContain("provider.GetType().Name", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("providerName", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("provider is SqliteProvider", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("IsMySqlProvider", dispatchSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_dynamic_rowversion_metadata()
    {
        var doc = ReadDoc();
        var source = ReadDynamicEntitySource();

        Assert.Contains("computed expression/storage, identity, and rowversion", doc, StringComparison.Ordinal);
        Assert.Contains("Computed/generated-column expression and", doc, StringComparison.Ordinal);
        Assert.Contains("computed expression/storage", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL runtime dynamic table names that include a catalog qualifier", doc, StringComparison.Ordinal);
        Assert.Contains("Unqualified dynamic table names fail deterministically", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server or PostgreSQL catalog probe finds exactly one matching schema", doc, StringComparison.Ordinal);
        Assert.Contains("non-null", doc, StringComparison.Ordinal);
        Assert.Contains("reference-column `[Required]` parity", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldComputedColumn", source, StringComparison.Ordinal);
        Assert.Contains("QueryComputedColumnMap", source, StringComparison.Ordinal);
        Assert.Contains("ExtractSqliteGeneratedColumns", source, StringComparison.Ordinal);
        Assert.Contains("NormalizeComputedColumnSql", source, StringComparison.Ordinal);
        Assert.Contains("column.ComputedColumn?.Sql", source, StringComparison.Ordinal);
        Assert.Contains("ResolveUniqueUnqualifiedSchema", source, StringComparison.Ordinal);
        Assert.Contains("GetMatchingObjectSchemas", source, StringComparison.Ordinal);
        Assert.Contains("GetSqliteMatchingObjectSchemas", source, StringComparison.Ordinal);
        Assert.Contains("QuerySchemaNameList", source, StringComparison.Ordinal);
        Assert.Contains("GetComputedColumns", source, StringComparison.Ordinal);
        Assert.Contains("sys.computed_columns", source, StringComparison.Ordinal);
        Assert.Contains("generation_expression AS ComputedSql", source, StringComparison.Ordinal);
        Assert.Contains("is_generated <> 'NEVER'", source, StringComparison.Ordinal);
        Assert.Contains("generation_expression IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("table_schema = COALESCE(@schemaName, DATABASE())", source, StringComparison.Ordinal);
        Assert.Contains("IsRowVersion", source, StringComparison.Ordinal);
        Assert.Contains("GetRowVersionColumns", source, StringComparison.Ordinal);
        Assert.Contains("TimestampAttribute", source, StringComparison.Ordinal);
        Assert.Contains("RequiredAttribute", source, StringComparison.Ordinal);
        Assert.Contains("MaxLengthAttribute", source, StringComparison.Ordinal);
        Assert.Contains("NormalizeScaffoldClrType", source, StringComparison.Ordinal);
        Assert.Contains("GetScaffoldMaxLength(normalizedClrType, row)", source, StringComparison.Ordinal);
        Assert.Contains("IsUnboundedScaffoldMaxLength", source, StringComparison.Ordinal);
        Assert.Contains("'timestamp', 'rowversion'", source, StringComparison.Ordinal);
        Assert.Contains("RV", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Dynamic_scaffolding_provider_dispatch_is_centralized()
    {
        var source = ReadDynamicEntitySource();
        var helperSource = ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityConnectionKind.cs");
        var dispatchSource = source.Replace(helperSource, string.Empty, StringComparison.Ordinal);

        Assert.Contains("internal static class DynamicEntityConnectionKind", source, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityConnectionKind.IsSqlite(connection)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityConnectionKind.IsSqlServer(connection)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityConnectionKind.IsPostgres(connection)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityConnectionKind.IsMySql(connection)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityConnectionKind.EscapeIdentifier(connection", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityConnectionKind.EscapeQualified(connection", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("connection.GetType().Name", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("IsSqliteConnection", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("IsSqlServerConnection", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("IsPostgresConnection", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("IsMySqlConnection", dispatchSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_pins_inverse_many_to_many_scaffolding()
    {
        var doc = ReadDoc();
        var source = ReadDatabaseScaffolderSource();
        var contextWriterSource = ReadContextWriterSource();
        var manyToManyDiscoverySource = ReadManyToManyDiscoverySource();
        var sqlServerMigration = ReadRepoFile("src", "nORM", "Migration", "SqlServerMigrationSqlGenerator.cs");
        var postgresMigration = ReadRepoFile("src", "nORM", "Migration", "PostgresMigrationSqlGenerator.cs");
        var sqliteMigration = ReadRepoFile("src", "nORM", "Migration", "SqliteMigrationSqlGenerator.cs");

        Assert.Contains("Both entity sides receive collection navigations", doc, StringComparison.Ordinal);
        Assert.Contains("non-null foreign-key constraints", doc, StringComparison.Ordinal);
        Assert.Contains("Single-column,", doc, StringComparison.Ordinal);
        Assert.Contains("alternate-key pure junction", doc, StringComparison.Ordinal);
        Assert.Contains("key-selector `UsingTable` overload", doc, StringComparison.Ordinal);
        Assert.Contains("WithMany(inverse)", doc, StringComparison.Ordinal);
        Assert.Contains("schema-aware `UsingTable`", doc, StringComparison.Ordinal);
        Assert.Contains("schema-aware `OwnsMany`", doc, StringComparison.Ordinal);
        Assert.Contains("Fluent `OwnsOne` owned scalar mappings", doc, StringComparison.Ordinal);
        Assert.Contains("expression indexes, and shadow columns is", doc, StringComparison.Ordinal);
        Assert.Contains("SchemaSnapshotBuilder.Build(ctx)", doc, StringComparison.Ordinal);
        Assert.Contains("idempotent schema creation", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite attached-schema foreign-key clauses", doc, StringComparison.Ordinal);
        Assert.Contains("Self-referencing pure join tables receive distinct", doc, StringComparison.Ordinal);
        Assert.Contains("role-based navigations", doc, StringComparison.Ordinal);
        Assert.Contains("from the join FK columns", doc, StringComparison.Ordinal);
        Assert.Contains("JoinTableSchema", source, StringComparison.Ordinal);
        Assert.Contains(".WithMany(p => p.", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("isSelfJoin", manyToManyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("leftCollectionBase += \"By\"", manyToManyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("IF SCHEMA_ID", sqlServerMigration, StringComparison.Ordinal);
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS", postgresMigration, StringComparison.Ordinal);
        Assert.Contains("EscTableNameOnly(fk.PrincipalTable)", sqliteMigration, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_deterministic_scaffold_output()
    {
        var doc = ReadDoc();
        var source = ReadDatabaseScaffolderSource();
        var contextWriterSource = ReadContextWriterSource();
        var relationshipDiscoverySource = ReadRelationshipDiscoverySource();
        var checkFeatureConfigurationBuilderSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCheckFeatureConfigurationBuilder.cs");
        var syntheticFeatureNameMarkerSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSyntheticFeatureNameMarker.cs");
        var foreignKeyDiscoverySource = ReadForeignKeyDiscoverySource();
        var indexDiscoverySource = ReadIndexDiscoverySource();
        var keyDiscoverySource = ReadKeyDiscoverySource();
        var sqlServerUnsupportedSource = ReadSqlServerUnsupportedFeatureSource();
        var postgresUnsupportedSource = ReadPostgresUnsupportedFeatureSource();
        var mySqlUnsupportedSource = ReadMySqlUnsupportedFeatureSource();
        var builderSource = ReadEntityTypeBuilderSource();
        var configurationSource = ReadRepoFile("src", "nORM", "Configuration", "IEntityTypeConfiguration.cs");
        var scalarDefaultConfigurationSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldScalarFeatureConfigurationBuilder.Defaults.cs");
        var sqlServerMigrationSource = ReadRepoFile("src", "nORM", "Migration", "SqlServerMigrationSqlGenerator.cs");
        var snapshotSource = ReadSchemaSnapshotSource();
        var liveCliSource = ReadLiveProviderScaffoldCliParitySource();

        Assert.Contains("generated output are ordered deterministically", doc, StringComparison.Ordinal);
        Assert.Contains("Relationship navigations and fluent relationship", doc, StringComparison.Ordinal);
        Assert.Contains("provider-reported foreign key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server foreign-key names marked", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL default", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL default", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server CHECK constraint", doc, StringComparison.Ordinal);
        Assert.Contains("<table>_<columns>_check", doc, StringComparison.Ordinal);
        Assert.Contains("<table>_chk_<n>", doc, StringComparison.Ordinal);
        Assert.Contains("CK_<Entity>_<hash>", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite autoindex", doc, StringComparison.Ordinal);
        Assert.Contains("<table>_<columns>_key", doc, StringComparison.Ordinal);
        Assert.Contains("first key column", doc, StringComparison.Ordinal);
        Assert.Contains("<column>_UNIQUE", doc, StringComparison.Ordinal);
        Assert.Contains("UX_<Table>_<Columns>", doc, StringComparison.Ordinal);
        Assert.Contains("action-aware `UsingTable` overload", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server, PostgreSQL, and SQLite primary-key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server system-generated names", doc, StringComparison.Ordinal);
        Assert.Contains("explicit non-system default-constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("HasDefaultValueSql(..., constraintName: ...)", doc, StringComparison.Ordinal);
        Assert.Contains("ConstraintName", source, StringComparison.Ordinal);
        Assert.Contains("fk.is_system_named AS IsSyntheticConstraintName", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("dep.relname || '_'", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("CHAR_LENGTH(kcu.table_name) + 6", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("REGEXP '^[0-9]+$'", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("foreignKey.IsSyntheticConstraintName ? null", relationshipDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("MarkSystemNamedCheckConstraintFeaturesAsync", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MarkNamedDefaultConstraintFeaturesAsync", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("SqlServerNamedDefaultConstraintSql", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("dc.is_system_named = 0", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("defaultConstraintName", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MarkDefaultNamedCheckConstraintFeaturesAsync", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MarkDefaultNamedCheckConstraintFeaturesAsync", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldSyntheticFeatureNameMarker", syntheticFeatureNameMarkerSource, StringComparison.Ordinal);
        Assert.Contains("isSyntheticName", syntheticFeatureNameMarkerSource, StringComparison.Ordinal);
        Assert.Contains("BuildGeneratedCheckConstraintName", checkFeatureConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeSyntheticIndexNames", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("RequiresExplicitManyToManyReferentialActions", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("constraintName:", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("BuildScaffoldDefaultConstraintNameMap", scalarDefaultConfigurationSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", builderSource, StringComparison.Ordinal);
        Assert.Contains("HasDefaultValueSql(string sql, string? constraintName)", builderSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", configurationSource, StringComparison.Ordinal);
        Assert.Contains("DefaultValueConstraintNames", configurationSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", snapshotSource, StringComparison.Ordinal);
        Assert.Contains("DefaultConstraintName", snapshotSource, StringComparison.Ordinal);
        Assert.Contains("DefaultConstraintName", sqlServerMigrationSource, StringComparison.Ordinal);
        Assert.Contains("BuildAddDefaultConstraintSql", sqlServerMigrationSource, StringComparison.Ordinal);
        Assert.Contains("constraintName:", liveCliSource, StringComparison.Ordinal);
        Assert.Contains("sqlite_autoindex_", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("kc.unique_index_id = i.index_id", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("unique_constraint.conname", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("FIRST_VALUE(s.column_name)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("_UNIQUE", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("GetPrimaryKeyConstraintNamesAsync", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("GetSqlitePrimaryKeyConstraintNameMapAsync", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("kc.is_system_named = 0", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("cls.relname || '_pkey'", keyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("PrimaryKeyConstraintName", builderSource, StringComparison.Ordinal);
        Assert.Contains("PrimaryKeyConstraintName", configurationSource, StringComparison.Ordinal);
        Assert.Contains("PrimaryKeyConstraintName", snapshotSource, StringComparison.Ordinal);
        Assert.Contains("ThenBy(r => r.CollectionNavigationName", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("ThenBy(r => r.ReferenceNavigationName", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("ThenBy(j => j.LeftCollectionNavigationName", contextWriterSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_partial_scaffold_customization()
    {
        var doc = ReadDoc();
        var cliReadme = ReadRepoFile("src", "dotnet-norm", "README.md");
        var rootReadme = ReadRepoFile("README.md");
        var contextWriterSource = ReadContextWriterSource();

        Assert.Contains("Generated entity classes and generated contexts are `partial`", doc, StringComparison.Ordinal);
        Assert.Contains("OnModelCreatingPartial(ModelBuilder)", doc, StringComparison.Ordinal);
        Assert.Contains("separate partial context", doc, StringComparison.Ordinal);
        Assert.Contains("string constructors; both require", doc, StringComparison.Ordinal);
        Assert.Contains("without embedding a connection string", doc, StringComparison.Ordinal);
        Assert.Contains("partial entity/context classes", cliReadme, StringComparison.Ordinal);
        Assert.Contains("OnModelCreatingPartial(ModelBuilder)", cliReadme, StringComparison.Ordinal);
        Assert.Contains("generated context constructors for both `DbConnection` and connection strings", cliReadme, StringComparison.Ordinal);
        Assert.Contains("never hard-code the", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Generated contexts expose both `DbConnection` and connection-string", rootReadme, StringComparison.Ordinal);
        Assert.Contains("never embed the scaffold connection string", rootReadme, StringComparison.Ordinal);
        Assert.Contains("public partial class", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("base(connectionString, provider, ConfigureOptions(options))", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("OnModelCreatingPartial(mb)", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("static partial void OnModelCreatingPartial(ModelBuilder modelBuilder)", contextWriterSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_sqlite_rowid_key_normalization()
    {
        var doc = ReadDoc();
        var source = ReadStaticEntityScaffoldSource();

        Assert.Contains("SQLite rowid integer primary keys are normalized to non-null `long`", doc, StringComparison.Ordinal);
        Assert.Contains("PRAGMA", source, StringComparison.Ordinal);
        Assert.Contains("table_xinfo", source, StringComparison.Ordinal);
        Assert.Contains("typeof(long)", source, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsSqlite(provider)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_literal_dotted_identifier_scaffolding()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Mapping", "IdentifierEscaping.cs");

        Assert.Contains("Literal table and column identifiers", doc, StringComparison.Ordinal);
        Assert.Contains("not silently reinterpreted as", doc, StringComparison.Ordinal);
        Assert.Contains("EscapeSingle", source, StringComparison.Ordinal);
        Assert.Contains("EscapeTable", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_names_the_public_scaffolder_types()
    {
        var doc = ReadDoc();
        Assert.Contains("DatabaseScaffolder", doc, StringComparison.Ordinal);
        Assert.Contains("DynamicEntityTypeGenerator", doc, StringComparison.Ordinal);
        Assert.Contains("dotnet-norm scaffold", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Generated_api_docs_include_scaffold_options_and_overloads()
    {
        var namespaceDoc = ReadRepoFile("docs", "api", "nORM.Scaffolding.yml");
        var scaffolderDoc = ReadRepoFile("docs", "api", "nORM.Scaffolding.DatabaseScaffolder.yml");
        var optionsDoc = ReadRepoFile("docs", "api", "nORM.Scaffolding.ScaffoldOptions.yml");
        var toc = ReadRepoFile("docs", "api", "toc.yml");

        Assert.Contains("nORM.Scaffolding.ScaffoldOptions", namespaceDoc, StringComparison.Ordinal);
        Assert.Contains("nORM.Scaffolding.ScaffoldOptions", toc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync(DbConnection, DatabaseProvider, string, string, ScaffoldOptions)", scaffolderDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync(DbConnection, DatabaseProvider, string, string, string, ScaffoldOptions?)", scaffolderDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.Schemas", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.UsePluralizer", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.UseDatabaseNames", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.UseNullableReferenceTypes", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextDirectory", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextNamespace", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextOutputDirectory", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.EmitRoutineStubs", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.EmitSequenceStubs", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.EmitViewEntities", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.EmitQueryArtifacts", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("supported query artifacts such as views", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("ordinary", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("views/materialized views are scaffolded", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("virtual", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("tables and synonyms remain opt-in", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("stored procedure/function", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("standalone sequence", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("all discovered user tables and supported query artifacts", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("Null or blank", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("filters are treated as empty", optionsDoc, StringComparison.Ordinal);
        Assert.Contains("FailOnWarnings", optionsDoc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_does_not_use_scaffolding_as_provider_mobility_evidence()
    {
        var doc = ReadDoc();
        Assert.Contains("Do not use scaffolding as evidence for provider mobility by itself.", doc, StringComparison.Ordinal);
        Assert.Contains("live provider gates", doc, StringComparison.Ordinal);
    }
}
