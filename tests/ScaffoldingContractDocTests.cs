using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the scaffolding scope documented in <c>docs/scaffolding.md</c> against the public
/// surface in <c>nORM.Scaffolding.*</c>. The doc is the consumer-facing scope statement;
/// runtime evidence lives in <c>ScaffoldingAndNavigationCoverageTests</c> and the per-provider
/// scaffolder tests inside the CLI suite.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldingContractDocTests
{
    private static string ReadDoc()
    {
        var asmDir = Path.GetDirectoryName(typeof(ScaffoldingContractDocTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", "scaffolding.md");
        Assert.True(File.Exists(path));
        return File.ReadAllText(path);
    }

    private static string ReadRepoFile(params string[] pathParts)
    {
        var asmDir = Path.GetDirectoryName(typeof(ScaffoldingContractDocTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(new[] { repoRoot }.Concat(pathParts).ToArray());
        Assert.True(File.Exists(path));
        return File.ReadAllText(path);
    }

    private static string ReadJoinTableDiagnosticSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableDiagnosticBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableReasonBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCompositeForeignKeyMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyShape.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableShape.cs"));

    private static string ReadRelationshipDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipNavigationNameBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyShape.cs"));

    private static string ReadProviderSpecificTypeClassifierSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderSpecificTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerAliasTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlStringLiteralParser.cs"));

    private static string ReadStaticEntityScaffoldSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceInfo.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldObjectSelectionBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityNameBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnPropertyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPrimaryKeyConfigurationBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticReportBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDataReaderHelper.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyShape.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipDiagnosticBuilder.cs"));

    private static string ReadDynamicEntitySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaResolver.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDataReaderHelper.cs"));

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
    public void Doc_bounds_schema_preservation_by_provider()
    {
        var doc = ReadDoc();
        Assert.Contains("Schema-qualified table names are preserved for SQL Server, PostgreSQL, and", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL scaffolding uses the current database for", doc, StringComparison.Ordinal);
        Assert.Contains("does not emit the database/catalog name as a model schema", doc, StringComparison.Ordinal);
        Assert.Contains("same table name appears in multiple schemas", doc, StringComparison.Ordinal);
        Assert.Contains("include the schema name", doc, StringComparison.Ordinal);
        Assert.Contains("avoid the enclosing entity type name", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_scaffold_length_round_trip()
    {
        var doc = ReadDoc();
        var snapshot = ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.cs");
        var entityTypeBuilder = ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.cs");
        var entityTypeConfiguration = ReadRepoFile("src", "nORM", "Configuration", "IEntityTypeConfiguration.cs");
        var scaffolder = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");
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
        Assert.Contains("BuildColumnFacetConfigurations", scaffolder, StringComparison.Ordinal);
        Assert.Contains("ScaffoldPrecisionConfiguration", scaffolder, StringComparison.Ordinal);
        Assert.Contains("BuildPrecisionConfigurations", scaffolder, StringComparison.Ordinal);
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
    public void Doc_and_source_pin_provider_owned_temporal_and_view_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadStaticEntityScaffoldSource();
        var joinDiagnosticBuilderSource = ReadJoinTableDiagnosticSource();
        var postgresUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.cs");
        var sqlServerUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.cs");
        var mySqlUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlUnsupportedFeatureDiscovery.cs");
        var skippedDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectDiscovery.cs");
        var skippedMetadataSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.cs");
        var columnDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.cs");
        var diagnosticsWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.cs");
        var parserSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSemicolonParser.cs");
        var sqlMetadataParserSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.cs");
        var unsupportedMetadataSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.cs");
        var providerTypeClassifierSource = ReadProviderSpecificTypeClassifierSource();
        Assert.Contains("SQL Server provider-native temporal tables", doc, StringComparison.Ordinal);
        Assert.Contains("views", doc, StringComparison.Ordinal);
        Assert.Contains("virtual tables", doc, StringComparison.Ordinal);
        Assert.Contains("shadow tables", doc, StringComparison.Ordinal);
        Assert.Contains("routines", doc, StringComparison.Ordinal);
        Assert.Contains("sequences", doc, StringComparison.Ordinal);
        Assert.Contains("synonyms", doc, StringComparison.Ordinal);
        Assert.Contains("materialized views", doc, StringComparison.Ordinal);
        Assert.Contains("events", doc, StringComparison.Ordinal);
        Assert.Contains("skipped", doc, StringComparison.Ordinal);
        Assert.Contains("tables without primary keys", doc, StringComparison.Ordinal);
        Assert.Contains("HasCollation", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific column types", doc, StringComparison.Ordinal);
        Assert.Contains("decimal precision/scale preservation", doc, StringComparison.Ordinal);
        Assert.Contains("Column(TypeName", doc, StringComparison.Ordinal);
        Assert.Contains("schema snapshot precision parsing", doc, StringComparison.Ordinal);
        Assert.Contains("domain-wrapped numeric type text", doc, StringComparison.Ordinal);
        Assert.Contains("provider metadata-backed identity", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL identity/serial", doc, StringComparison.Ordinal);
        Assert.Contains("owned sequences are treated as", doc, StringComparison.Ordinal);
        Assert.Contains("dependent or principal table is intentionally filtered out", doc, StringComparison.Ordinal);
        Assert.Contains("FilterForeignKeysToScaffoldedTables", source, StringComparison.Ordinal);
        Assert.Contains("rowversion/timestamp", doc, StringComparison.Ordinal);
        Assert.Contains("identity seed/increment", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server `PERSISTED`", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL stored generated", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL `VIRTUAL GENERATED`/`STORED", doc, StringComparison.Ordinal);
        Assert.Contains("unrecognized FK referential actions", doc, StringComparison.Ordinal);
        Assert.Contains("relationships that do not target the generated principal primary key or an", doc, StringComparison.Ordinal);
        Assert.Contains("exact ordered unfiltered unique index", doc, StringComparison.Ordinal);
        Assert.Contains("HasNonNullableColumns", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesScaffoldablePrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesUniqueIndex", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes)", source, StringComparison.Ordinal);
        Assert.Contains("HasExactUniqueIndex(indexes, tableKey, foreignKeyColumnSet)", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("GetIdentityColumnNamesAsync", source, StringComparison.Ordinal);
        Assert.Contains("auto_increment", columnDiscoverySource, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nextval(%", columnDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("dependency.deptype IN ('a', 'i')", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("column_default NOT LIKE 'nextval(%'", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("temporal_type <> 0", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("skippedDatabaseObjects", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("MissingPrimaryKey", source, StringComparison.Ordinal);
        Assert.Contains("Collation", source, StringComparison.Ordinal);
        Assert.Contains("ProviderSpecificColumnType", source, StringComparison.Ordinal);
        Assert.Contains("PrecisionScale", source, StringComparison.Ordinal);
        Assert.Contains("EndsWithDelimitedTypeName", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("RowVersion", source, StringComparison.Ordinal);
        Assert.Contains("IdentityStrategy", source, StringComparison.Ordinal);
        Assert.Contains("TryTrimTrailingComputedStorageToken", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("quote == '[' ? ']'", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("TryConsumeSqlKeyword(candidate, ref keywordIndex, \"CHECK\")", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("TryConsumeSqlKeyword(detail, ref keywordIndex, \"IDENTITY\")", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("cc.is_persisted", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("generation_expression || ' STORED'", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("stored generated", mySqlUnsupportedSource, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("virtual generated", mySqlUnsupportedSource, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ReferentialAction", source, StringComparison.Ordinal);
        Assert.Contains("RelationshipPrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("'Routine'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("outputParameters", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("parameterModes", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("pa.is_output = 1 THEN 'OUT'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("decimal precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("decimal precision and optional scale", doc, StringComparison.Ordinal);
        Assert.Contains("exclude provider metadata return rows from callable input counts", doc, StringComparison.Ordinal);
        Assert.Contains("p.parameter_mode IS NOT NULL", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("WITHIN GROUP (ORDER BY pa.parameter_id)", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ty.name", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("base_ty.name", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("p.data_type", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("domain_name", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("domain_schema", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("column_type", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("unsigned", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("legacy display widths", doc, StringComparison.Ordinal);
        Assert.Contains("NormalizeMySqlUnsignedTypeDetail", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildSequenceMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildSynonymMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildEventMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildQueryObjectMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildUnsupportedFeatureMetadata", source, StringComparison.Ordinal);
        Assert.Contains("ScaffoldUnsupportedFeatureMetadataBuilder.BuildMetadata", source, StringComparison.Ordinal);
        Assert.Contains("TryParseRelationshipPrincipalKeyDetail", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("TryParseReferentialActionDetail", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("InferSqliteVirtualTableShadowOwner", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineSemicolonValues", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldSemicolonParser.ParseRoutine", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("SelectBestOrderedSemicolonValueMarkers", parserSource, StringComparison.Ordinal);
        Assert.Contains("SelectOrderedSemicolonValueMarkers", parserSource, StringComparison.Ordinal);
        Assert.Contains("GetRoutineSemicolonValueKeyOrders", parserSource, StringComparison.Ordinal);
        Assert.Contains("IsKnownSemicolonValueKey", parserSource, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineParameters", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("TryParseRoutineParameterMode", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("trimmed.Length - 1", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("SplitRoutineResultColumns", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("TryParseRoutineResultColumnParts", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("information_schema.parameters", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("specific_name", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'Sequence'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'VirtualTable'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'VirtualTableShadow'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'Synonym'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'MaterializedView'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'Event'", skippedDiscoverySource, StringComparison.Ordinal);
    }

    [Fact]
    public void Source_pins_explicit_system_usings_for_generated_scaffold_artifacts()
    {
        var source = ReadStaticEntityScaffoldSource();
        var entityWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityWriter.cs");
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");
        var skippedDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectDiscovery.cs");
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
    public void Doc_defines_warning_report_json_shape()
    {
        var doc = ReadDoc();
        Assert.Contains("Warning Report Shape", doc, StringComparison.Ordinal);
        Assert.Contains("Diagnostic Code Catalog", doc, StringComparison.Ordinal);
        Assert.Contains("nORM.ScaffoldWarnings.json", doc, StringComparison.Ordinal);
        Assert.Contains("summary", doc, StringComparison.Ordinal);
        Assert.Contains("totalWarnings", doc, StringComparison.Ordinal);
        Assert.Contains("sectionCounts", doc, StringComparison.Ordinal);
        Assert.Contains("compositeForeignKeys", doc, StringComparison.Ordinal);
        Assert.Contains("possibleManyToManyJoinTables", doc, StringComparison.Ordinal);
        Assert.Contains("providerOwnedSchemaFeatures", doc, StringComparison.Ordinal);
        Assert.Contains("skippedDatabaseObjects", doc, StringComparison.Ordinal);
        Assert.Contains("code", doc, StringComparison.Ordinal);
        Assert.Contains("severity", doc, StringComparison.Ordinal);
        Assert.Contains("category", doc, StringComparison.Ordinal);
        Assert.Contains("metadata", doc, StringComparison.Ordinal);
        Assert.Contains("reasons", doc, StringComparison.Ordinal);
        Assert.Contains("payload-columns", doc, StringComparison.Ordinal);
        Assert.Contains("nullable-foreign-key", doc, StringComparison.Ordinal);
        Assert.Contains("foreign-key-metadata-incomplete", doc, StringComparison.Ordinal);
        Assert.Contains("primary-key-not-exact-bridge-columns", doc, StringComparison.Ordinal);
        Assert.Contains("missing-exact-unique-index", doc, StringComparison.Ordinal);
        Assert.Contains("principal-key-not-scaffoldable", doc, StringComparison.Ordinal);
        Assert.Contains("provider-owned-write-blocking-schema", doc, StringComparison.Ordinal);
        Assert.Contains("providerOwnedWriteBlockingSchema", doc, StringComparison.Ordinal);
        Assert.Contains("parameterCount", doc, StringComparison.Ordinal);
        Assert.Contains("outputParameterCount", doc, StringComparison.Ordinal);
        Assert.Contains("routineType", doc, StringComparison.Ordinal);
        Assert.Contains("stubSupported", doc, StringComparison.Ordinal);
        Assert.Contains("generated value `clrType`", doc, StringComparison.Ordinal);
        Assert.Contains("queryArtifactSupported", doc, StringComparison.Ordinal);
        Assert.Contains("targetKind", doc, StringComparison.Ordinal);
        Assert.Contains("eventType", doc, StringComparison.Ordinal);
        Assert.Contains("intervalField", doc, StringComparison.Ordinal);
        Assert.Contains("shadowOf", doc, StringComparison.Ordinal);
        Assert.Contains("readOnlyEntity", doc, StringComparison.Ordinal);
        Assert.Contains("generated `properties`", doc, StringComparison.Ordinal);
        Assert.Contains("columnCount", doc, StringComparison.Ordinal);
        Assert.Contains("navigationSuppressed", doc, StringComparison.Ordinal);
        Assert.Contains("generatedNavigationSupported", doc, StringComparison.Ordinal);
        Assert.Contains("relationshipSuppressed", doc, StringComparison.Ordinal);
        Assert.Contains("dependent/principal table and column details", doc, StringComparison.Ordinal);
        Assert.Contains("scaffoldable unique index", doc, StringComparison.Ordinal);
        Assert.Contains("foreignKeyColumns", doc, StringComparison.Ordinal);
        Assert.Contains("primaryKeyColumns", doc, StringComparison.Ordinal);
        Assert.Contains("payloadColumns", doc, StringComparison.Ordinal);
        Assert.Contains("nullableForeignKeyColumns", doc, StringComparison.Ordinal);
        Assert.Contains("bridge-key booleans", doc, StringComparison.Ordinal);
        Assert.Contains("computedSql", doc, StringComparison.Ordinal);
        Assert.Contains("indexSql", doc, StringComparison.Ordinal);
        Assert.Contains("expressionSql", doc, StringComparison.Ordinal);
        Assert.Contains("prefix-column lengths", doc, StringComparison.Ordinal);
        Assert.Contains("hasNullsNotDistinct", doc, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultNullOrdering", doc, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultOperatorClass", doc, StringComparison.Ordinal);
        Assert.Contains("provider `collation`", doc, StringComparison.Ordinal);
        Assert.Contains("provider/object-kind metadata", doc, StringComparison.Ordinal);
        Assert.Contains("triggerName", doc, StringComparison.Ordinal);
        Assert.Contains("providerOwnedDdl", doc, StringComparison.Ordinal);
        Assert.Contains("triggerSql", doc, StringComparison.Ordinal);
        Assert.Contains("providerNativeTemporal", doc, StringComparison.Ordinal);
        Assert.Contains("generatedTemporalConfigurationSupported", doc, StringComparison.Ordinal);
        Assert.Contains("historyTable", doc, StringComparison.Ordinal);
        Assert.Contains("provider-owned feature, query-object, routine, sequence,", doc, StringComparison.Ordinal);
        Assert.Contains("IReadOnlyDictionary<string, object?>", doc, StringComparison.Ordinal);
        Assert.Contains("exact scaffolded", doc, StringComparison.Ordinal);
        Assert.Contains("without the expected parameter object or dictionary shape", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL set-returning functions", doc, StringComparison.Ordinal);
        Assert.Contains("Query-artifact generated types are marked with", doc, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", doc, StringComparison.Ordinal);
        Assert.Contains("stale `nORM.ScaffoldWarnings.*` files", doc, StringComparison.Ordinal);
        var source = ReadStaticEntityScaffoldSource();
        var joinDiagnosticBuilderSource = ReadJoinTableDiagnosticSource();
        var skippedDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectDiscovery.cs");
        var diagnosticsWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.cs");
        var routineWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.cs");
        var unsupportedMetadataSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.cs");
        Assert.Contains("code =", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("severity =", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("category =", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("Metadata = BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("providerObjectKind", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("proretset", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("queryArtifactTableKeys", source, StringComparison.Ordinal);
        Assert.Contains("reasons = table.Reasons", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("BuildPossibleJoinTableReasons", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("BuildCompositeForeignKeyMetadata", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("BuildPossibleJoinTableMetadata", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("AddIndexFeatureMetadata", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("AddMetadataBooleanValue", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ParsePrefixIndexColumns", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("Metadata { get; init; }", source, StringComparison.Ordinal);
        Assert.Contains("hasExactBridgePrimaryKey", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("hasGeneratedSurrogatePrimaryKey", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("hasExactForeignKeyUniqueIndex", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("totalWarnings =", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("sectionCounts = new", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("RequireScaffoldedRoutineParameters", routineWriterSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Routine_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetRevenue",
            "Routine",
            "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@tenantId:IN:int,@total:OUT:decimal; resultColumns=Id:int:0|Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("SQL Server", metadata["provider"]);
        Assert.Equal("stored procedure", metadata["routineType"]);
        Assert.Equal(2, metadata["parameterCount"]);
        Assert.Equal(1, metadata["outputParameterCount"]);
        Assert.Equal("@tenantId", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("@total", parameters[1]["name"]);
        Assert.Equal("OUT", parameters[1]["mode"]);
        Assert.Equal("decimal", parameters[1]["dataType"]);
        Assert.Equal("Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_colon_characters_in_provider_names()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant:id:IN:int; resultColumns=Order:Id:int:0|Line:Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant:id", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("Order:Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Line:Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_mode_like_text_inside_parameter_names()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant:IN:name:IN:int; resultColumns=",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;

        Assert.Equal("@tenant:IN:name", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_semicolon_characters_inside_values()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant;id:IN:int; resultColumns=Order;Id:int:0|Line;Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant;id", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("Order;Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Line;Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_splits_lists_after_complete_entries()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=2; outputParameters=0; parameterModes=@tenant,id:IN:int,@search,text:IN:nvarchar(40); resultColumns=Order|Id:int:0|Line|Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant,id", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("@search,text", parameters[1]["name"]);
        Assert.Equal("IN", parameters[1]["mode"]);
        Assert.Equal("nvarchar(40)", parameters[1]["dataType"]);
        Assert.Equal("Order|Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Line|Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_unknown_key_like_text_inside_values()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant; note=retained:IN:int; resultColumns=Order; note=retained:int:0",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant; note=retained", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("Order; note=retained", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_allowlisted_key_text_inside_values()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "public",
            "calculate_odd",
            "Routine",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=tenant; dataType=retained:IN:integer; dataType=integer",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;

        Assert.Equal("tenant; dataType=retained", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("integer", parameters[0]["dataType"]);
        Assert.Equal("integer", metadata["dataType"]);
        Assert.Equal("scalar-function", metadata["callShape"]);
    }

    [Fact]
    public void Sequence_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var sequence = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "OrderNo",
            "Sequence",
            "SQL Server sequence; dataType=bigint",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { sequence })!;

        Assert.Equal("SQL Server", metadata["provider"]);
        Assert.Equal("bigint", metadata["dataType"]);
        Assert.Equal("long", metadata["clrType"]);
        Assert.Equal(true, metadata["stubSupported"]);
    }

    [Fact]
    public void Synonym_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var synonym = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "OrderAlias",
            "Synonym",
            "SQL Server synonym; baseObject=[dbo].[Orders; note=retained]; baseType=U",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { synonym })!;

        Assert.Equal("SQL Server", metadata["provider"]);
        Assert.Equal("[dbo].[Orders; note=retained]", metadata["baseObject"]);
        Assert.Equal("U", metadata["baseType"]);
        Assert.Equal("Table", metadata["targetKind"]);
        Assert.Equal(true, metadata["queryArtifactSupported"]);
    }

    [Fact]
    public void Event_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var scheduledEvent = Activator.CreateInstance(
            skippedObjectType,
            null,
            "RefreshLedger",
            "Event",
            "MySQL event; eventType=RECURRING; status=ENABLED; intervalValue=1; intervalField=DAY; starts=2026-01-01 00:00:00",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { scheduledEvent })!;

        Assert.Equal("MySQL", metadata["provider"]);
        Assert.Equal("RECURRING", metadata["eventType"]);
        Assert.Equal("ENABLED", metadata["status"]);
        Assert.Equal("1", metadata["intervalValue"]);
        Assert.Equal("DAY", metadata["intervalField"]);
        Assert.Equal("2026-01-01 00:00:00", metadata["starts"]);
    }

    [Fact]
    public void Query_object_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var view = Activator.CreateInstance(
            skippedObjectType,
            "reporting",
            "OpenOrders",
            "View",
            "PostgreSQL view",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { view })!;

        Assert.Equal("PostgreSQL", metadata["provider"]);
        Assert.Equal("View", metadata["targetKind"]);
        Assert.Equal(true, metadata["queryArtifactSupported"]);
    }

    [Fact]
    public void Virtual_table_shadow_warning_metadata_includes_owner()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var shadow = Activator.CreateInstance(
            skippedObjectType,
            "main",
            "SearchDocs_content",
            "VirtualTableShadow",
            "SQLite virtual table shadow table",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { shadow })!;

        Assert.Equal("SQLite", metadata["provider"]);
        Assert.Equal("VirtualTableShadow", metadata["targetKind"]);
        Assert.Equal(false, metadata["queryArtifactSupported"]);
        Assert.Equal("SearchDocs", metadata["shadowOf"]);
    }

    [Fact]
    public void Provider_owned_feature_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = scaffolder.GetMethod("BuildUnsupportedFeatureMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        IReadOnlyDictionary<string, object?> Metadata(string kind, string name, string detail, string tableKey = "dbo.Orders")
        {
            var feature = Activator.CreateInstance(featureType, tableKey, kind, name, detail)!;
            return (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { feature })!;
        }

        var missingPrimaryKey = Metadata("MissingPrimaryKey", "OrderReport", "Table has no primary key.", "dbo.OrderReport");
        Assert.Equal(true, missingPrimaryKey["readOnlyEntity"]);
        Assert.Equal(false, missingPrimaryKey["generatedWritesSupported"]);
        Assert.Equal(false, missingPrimaryKey["generatedNavigationSupported"]);
        Assert.Equal("dbo.OrderReport", missingPrimaryKey["table"]);
        Assert.Equal("missing-primary-key", missingPrimaryKey["reason"]);

        var principalKey = Metadata("RelationshipPrincipalKey", "FK_Order_Customer", "FK references crm.Customer.(TenantId, Code); generated principal key is Id");
        Assert.Equal(true, principalKey["navigationSuppressed"]);
        Assert.Equal("principal-key-not-scaffoldable", principalKey["reason"]);
        Assert.Equal("dbo.Orders", principalKey["dependentTable"]);
        Assert.Equal("crm.Customer", principalKey["principalTable"]);
        Assert.Equal(new[] { "TenantId", "Code" }, (string[])principalKey["principalColumns"]!);

        var dependentKey = Metadata("RelationshipDependentKey", "FK_Report_Order", "Dependent table has no generated primary key.", "report.OrderReport");
        Assert.Equal(true, dependentKey["navigationSuppressed"]);
        Assert.Equal("dependent-table-keyless", dependentKey["reason"]);
        Assert.Equal(false, dependentKey["generatedNavigationSupported"]);
        Assert.Equal("report.OrderReport", dependentKey["dependentTable"]);

        var referentialAction = Metadata("ReferentialAction", "FK_Order_Customer", "ON DELETE SET DEFAULT; ON UPDATE NO ACTION");
        Assert.Equal(true, referentialAction["navigationSuppressed"]);
        Assert.Equal(false, referentialAction["generatedNavigationSupported"]);
        Assert.Equal("referential-action-not-scaffoldable", referentialAction["reason"]);
        Assert.Equal("SET DEFAULT", referentialAction["onDelete"]);
        Assert.Equal("NO ACTION", referentialAction["onUpdate"]);

        var precisionScale = Metadata("PrecisionScale", "Amount", "decimal(18,2)");
        Assert.Equal(18, precisionScale["precision"]);
        Assert.Equal(2, precisionScale["scale"]);

        var precisionOnly = Metadata("PrecisionScale", "Amount", "numeric(10)");
        Assert.Equal(10, precisionOnly["precision"]);
        Assert.Null(precisionOnly["scale"]);

        var computed = Metadata("Computed", "Total", "(Price * Quantity) STORED");
        Assert.Equal("Price * Quantity", computed["computedSql"]);
        Assert.Equal(true, computed["stored"]);

        Assert.Equal(true, Metadata("RowVersion", "Version", "rowversion")["concurrencyToken"]);
        Assert.Equal(true, Metadata("RowVersion", "Version", "rowversion")["databaseGenerated"]);

        var identity = Metadata("IdentityStrategy", "Id", "IDENTITY(100,10)");
        Assert.Equal("IDENTITY(100,10)", identity["identityStrategy"]);
        Assert.Equal(true, identity["readOnlyEntity"]);
        Assert.Equal(false, identity["generatedWritesSupported"]);
        Assert.Equal("provider-specific-identity-strategy", identity["reason"]);
        Assert.Equal(100L, identity["seed"]);
        Assert.Equal(10L, identity["increment"]);

        var providerSpecificColumn = Metadata("ProviderSpecificColumnType", "Shape", "geometry");
        Assert.Equal("geometry", providerSpecificColumn["providerType"]);
        Assert.Equal(true, providerSpecificColumn["readOnlyEntity"]);
        Assert.Equal(false, providerSpecificColumn["generatedWritesSupported"]);
        Assert.Equal("provider-specific-column-type", providerSpecificColumn["reason"]);
        Assert.Equal("Latin1_General_CI_AS", Metadata("Collation", "Name", "Latin1_General_CI_AS")["collation"]);
        var defaultSql = Metadata("Default", "CreatedAt", "now()");
        Assert.Equal("now()", defaultSql["defaultSql"]);
        Assert.Equal(true, defaultSql["readOnlyEntity"]);
        Assert.Equal(false, defaultSql["generatedWritesSupported"]);
        Assert.Equal("provider-specific-default", defaultSql["reason"]);
        Assert.Equal("Amount > 0", Metadata("CheckConstraint", "CK_Orders_Amount", "CHECK (Amount > 0)")["checkSql"]);
        var partialIndex = Metadata("PartialIndex", "IX_Orders_Open", "CREATE INDEX IX_Orders_Open ON Orders (Status) WHERE Status = 'Open'");
        Assert.Equal(true, partialIndex["filtered"]);
        Assert.Equal("Status = 'Open'", partialIndex["filterSql"]);
        Assert.Equal(false, partialIndex["isUnique"]);

        var expressionIndex = Metadata("ExpressionIndex", "IX_Orders_LowerName", "CREATE UNIQUE INDEX IX ON Orders (lower(Name)) WHERE Name IS NOT NULL");
        Assert.Equal(true, expressionIndex["expressionBased"]);
        Assert.Equal("lower(Name)", expressionIndex["expressionSql"]);
        Assert.Equal("lower(Name)", expressionIndex["keySql"]);
        Assert.Equal("Name IS NOT NULL", expressionIndex["filterSql"]);
        Assert.Equal(true, expressionIndex["isUnique"]);
        Assert.StartsWith("CREATE UNIQUE INDEX", Assert.IsType<string>(expressionIndex["indexSql"]));

        var mySqlExpression = Metadata("ExpressionIndex", "IX_Orders_LowerName", "MySQL expression index; expression=LOWER(`Name`)");
        Assert.Equal("MySQL", mySqlExpression["provider"]);
        Assert.Equal("LOWER(`Name`)", mySqlExpression["expressionSql"]);
        Assert.Equal(true, Metadata("IncludedColumnIndex", "IX_Orders_Customer", "SQL Server index with included columns")["includedColumns"]);
        Assert.Equal(true, Metadata("DescendingIndex", "IX_Orders_Created", "SQL Server descending index key")["descending"]);
        var prefixIndex = Metadata("PrefixIndex", "IX_Orders_Code", "MySQL prefix index; prefixColumns=Code:8/80");
        Assert.Equal(true, prefixIndex["prefixIndex"]);
        var prefixColumns = Assert.IsAssignableFrom<IReadOnlyList<IReadOnlyDictionary<string, object?>>>(prefixIndex["prefixColumns"]);
        var prefixColumn = Assert.Single(prefixColumns);
        Assert.Equal("Code", prefixColumn["name"]);
        Assert.Equal(8, prefixColumn["prefixLength"]);
        Assert.Equal(80, prefixColumn["declaredLength"]);
        var sqlServerProviderIndex = Metadata("ProviderSpecificIndex", "IX_Orders_Search", "SQL Server provider-specific index; indexType=NONCLUSTERED COLUMNSTORE");
        Assert.Equal(true, sqlServerProviderIndex["providerSpecific"]);
        Assert.Equal("SQL Server", sqlServerProviderIndex["provider"]);
        Assert.Equal("NONCLUSTERED COLUMNSTORE", sqlServerProviderIndex["indexType"]);

        var mySqlProviderIndex = Metadata("ProviderSpecificIndex", "IX_Orders_Search", "MySQL provider-specific index; indexType=FULLTEXT");
        Assert.Equal("MySQL", mySqlProviderIndex["provider"]);
        Assert.Equal("FULLTEXT", mySqlProviderIndex["indexType"]);

        var postgresProviderIndex = Metadata(
            "ProviderSpecificIndex",
            "IX_Orders_Search",
            "PostgreSQL btree index with provider-specific key options; accessMethod=btree; hasNullsNotDistinct=true; hasNonDefaultOperatorClass=false; hasIndexCollation=true; hasNonDefaultNullOrdering=true; indexSql=CREATE UNIQUE INDEX IX ON Orders (Name NULLS FIRST) NULLS NOT DISTINCT");
        Assert.Equal(true, postgresProviderIndex["providerSpecific"]);
        Assert.Equal("PostgreSQL", postgresProviderIndex["provider"]);
        Assert.Equal("btree", postgresProviderIndex["accessMethod"]);
        Assert.Equal(true, postgresProviderIndex["hasNullsNotDistinct"]);
        Assert.Equal(false, postgresProviderIndex["hasNonDefaultOperatorClass"]);
        Assert.Equal(true, postgresProviderIndex["hasIndexCollation"]);
        Assert.Equal(true, postgresProviderIndex["hasNonDefaultNullOrdering"]);
        Assert.Equal(true, postgresProviderIndex["isUnique"]);
        Assert.Equal("Name NULLS FIRST", postgresProviderIndex["keySql"]);

        var trigger = Metadata("Trigger", "TR_Orders_Audit", "PostgreSQL trigger; timing=BEFORE; event=INSERT; orientation=ROW");
        Assert.Equal("PostgreSQL", trigger["provider"]);
        Assert.Equal("Trigger", trigger["providerObjectKind"]);
        Assert.Equal("dbo.Orders", trigger["table"]);
        Assert.Equal("TR_Orders_Audit", trigger["triggerName"]);
        Assert.Equal(true, trigger["providerOwnedDdl"]);
        Assert.Equal(false, trigger["generatedModelConfigurationSupported"]);
        Assert.Equal(true, trigger["readOnlyEntity"]);
        Assert.Equal(false, trigger["generatedWritesSupported"]);
        Assert.Equal("provider-owned-trigger", trigger["reason"]);
        Assert.Equal(false, trigger["definitionAvailable"]);
        Assert.Equal("BEFORE", trigger["timing"]);
        Assert.Equal("INSERT", trigger["event"]);
        Assert.Equal("ROW", trigger["orientation"]);

        var sqlServerTrigger = Metadata("Trigger", "TR_Orders_Block", "SQL Server trigger; timing=INSTEAD OF; isDisabled=true; isInsteadOf=true");
        Assert.Equal("SQL Server", sqlServerTrigger["provider"]);
        Assert.Equal(true, sqlServerTrigger["isDisabled"]);
        Assert.Equal(true, sqlServerTrigger["isInsteadOf"]);

        var temporal = Metadata("TemporalTable", "Orders", "SQL Server system-versioned temporal table; temporalType=system-versioned; historyTable=dbo.OrdersHistory");
        Assert.Equal("SQL Server", temporal["provider"]);
        Assert.Equal("TemporalTable", temporal["providerObjectKind"]);
        Assert.Equal("dbo.Orders", temporal["table"]);
        Assert.Equal(true, temporal["providerNativeTemporal"]);
        Assert.Equal(false, temporal["generatedTemporalConfigurationSupported"]);
        Assert.Equal(true, temporal["readOnlyEntity"]);
        Assert.Equal(false, temporal["generatedWritesSupported"]);
        Assert.Equal("provider-native-temporal", temporal["reason"]);
        Assert.Equal("system-versioned", temporal["temporalType"]);
        Assert.Equal("dbo.OrdersHistory", temporal["historyTable"]);
    }

    [Fact]
    public void Referential_action_diagnostic_metadata_includes_relationship_shape()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var foreignKeyType = scaffolder.GetNestedType("ScaffoldForeignKey", BindingFlags.NonPublic)!;
        var addMethod = scaffolder.GetMethod("AddReferentialActionDiagnostics", BindingFlags.NonPublic | BindingFlags.Static)!;
        var metadataMethod = scaffolder.GetMethod("BuildUnsupportedFeatureMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;
        var features = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(featureType))!;
        var foreignKeys = Array.CreateInstance(foreignKeyType, 2);
        foreignKeys.SetValue(Activator.CreateInstance(
            foreignKeyType,
            "sales",
            "OrderLine",
            "TenantId",
            "sales",
            "Order",
            "TenantId",
            "FK_OrderLine_Order",
            2,
            "PROVIDER CASCADE",
            "NO ACTION",
            false)!, 0);
        foreignKeys.SetValue(Activator.CreateInstance(
            foreignKeyType,
            "sales",
            "OrderLine",
            "OrderId",
            "sales",
            "Order",
            "OrderId",
            "FK_OrderLine_Order",
            2,
            "PROVIDER CASCADE",
            "NO ACTION",
            false)!, 1);

        addMethod.Invoke(null, new object[]
        {
            features,
            foreignKeys
        });

        var feature = Assert.Single(features.Cast<object>());
        var metadata = (IReadOnlyDictionary<string, object?>)metadataMethod.Invoke(null, new[] { feature })!;

        Assert.Equal("sales.OrderLine", metadata["dependentTable"]);
        Assert.Equal(new[] { "TenantId", "OrderId" }, (string[])metadata["dependentColumns"]!);
        Assert.Equal("sales.Order", metadata["principalTable"]);
        Assert.Equal(new[] { "TenantId", "OrderId" }, (string[])metadata["principalColumns"]!);
        Assert.Equal(2, metadata["columnCount"]);
        Assert.Equal(true, metadata["navigationSuppressed"]);
        Assert.Equal(false, metadata["generatedNavigationSupported"]);
        Assert.Equal("referential-action-not-scaffoldable", metadata["reason"]);
        Assert.Equal("PROVIDER CASCADE", metadata["onDelete"]);
        Assert.Equal("NO ACTION", metadata["onUpdate"]);
    }

    [Fact]
    public void Doc_catalogs_all_scaffold_diagnostic_codes()
    {
        var doc = ReadDoc();
        var expectedCodes = new[]
        {
            "SCF001", "SCF002",
            "SCF100", "SCF101", "SCF102", "SCF103", "SCF104", "SCF106", "SCF107",
            "SCF108", "SCF109", "SCF110", "SCF111", "SCF112", "SCF113", "SCF114", "SCF115",
            "SCF116", "SCF117", "SCF118", "SCF119", "SCF199",
            "SCF200", "SCF201", "SCF202", "SCF203", "SCF204", "SCF205", "SCF206", "SCF207",
            "SCF299"
        };

        foreach (var code in expectedCodes)
            Assert.Contains($"`{code}`", doc, StringComparison.Ordinal);

        Assert.Contains("SQLite declared `UUID`, `JSON`, and `XML`, SQL Server `xml`, PostgreSQL `citext`/`json`/`jsonb`/`xml`/`uuid` plus safe scalar arrays/simple enums, and MySQL `json`/`year`/simple `enum(...)` plus bounded simple `set(...)` are scaffolded as supported storage", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL unsigned integer and decimal/numeric columns, SQL Server alias types over scaffoldable scalar/binary bases, and PostgreSQL domains over safe scalar/array/enum base types preserve generated writes and bounded facets", doc, StringComparison.Ordinal);
        Assert.Contains("malformed enum literal lists remain", doc, StringComparison.Ordinal);
        Assert.Contains("Unsafe provider-specific declarations such as SQL Server/SQLite/MySQL spatial types like `GEOMETRY`/`POINT`, PostgreSQL network/search types such as `inet`, and larger or ambiguous MySQL `set(...)` declarations remain diagnostics and make the generated entity `[ReadOnlyEntity]`", doc, StringComparison.Ordinal);
        Assert.Contains("Valid `NO ACTION`, `CASCADE`, `SET NULL`, `RESTRICT`, and `SET DEFAULT` actions are emitted in generated fluent configuration", doc, StringComparison.Ordinal);
        Assert.Contains("Ordinary SQL Server/PostgreSQL included-column indexes are emitted with `IndexAttribute.IsIncluded`", doc, StringComparison.Ordinal);
        Assert.Contains("Ordinary PostgreSQL column indexes with non-default `NULLS FIRST/LAST` ordering are emitted with `IndexAttribute.NullSortOrder`", doc, StringComparison.Ordinal);
        Assert.Contains("ordinary PostgreSQL unique column indexes with `NULLS NOT DISTINCT` are emitted with `IndexAttribute.NullsNotDistinct`", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_provider_specific_index_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadStaticEntityScaffoldSource();
        var diagnosticsWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.cs");
        var featureConfigurationBuilderSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.cs");
        var expressionIndexConfigurationBuilderSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldExpressionIndexConfigurationBuilder.cs");
        var indexDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.cs");
        var postgresUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.cs");
        var mySqlUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlUnsupportedFeatureDiscovery.cs");
        var unsupportedMetadataSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.cs");

        Assert.Contains("filtered/partial", doc, StringComparison.Ordinal);
        Assert.Contains("filtered/partial predicates for the same", doc, StringComparison.Ordinal);
        Assert.Contains("expression", doc, StringComparison.Ordinal);
        Assert.Contains("included-column", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite attached-schema partial index predicates", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL prefix indexes", doc, StringComparison.Ordinal);
        Assert.Contains("descending", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PartialIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ExpressionIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("IncludedColumnIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("DescendingIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("PrefixIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ProviderSpecificIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("supported FK referential actions are emitted", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("unrecognized/provider-specific FK referential action tokens", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("Ordinary SQL Server/PostgreSQL included-column indexes are emitted with IndexAttribute.IsIncluded", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits only key-column index metadata", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("non-default FK referential actions", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("ExtractCreateIndexWhereClause(feature.Detail)", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("IsCreateIndexUnique(feature.Detail)", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("FindCreateIndexKeyListOpen", source, StringComparison.Ordinal);
        Assert.Contains("FindSqlKeywordOutsideQuotes", source, StringComparison.Ordinal);
        Assert.Contains("GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("i.type IN (1, 2)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("am.amname = 'btree'", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ix.indoption[key.ord - 1] & 1", indexDiscoverySource, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_get_indexdef(ix.indexrelid) ILIKE '% DESC%'", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("UPPER(COALESCE(NULLIF(s.index_type, ''), 'BTREE')) = 'BTREE'", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("bad.sub_part IS NOT NULL", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("bad.sub_part < bad_col.character_maximum_length", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("sub_part IS NOT NULL", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("SHOW INDEX FROM", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("ReaderHasColumn(reader, \"Expression\")", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MySQL expression index", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("expressionIndexKeys", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("!string.IsNullOrWhiteSpace(index.FilterSql)", source, StringComparison.Ordinal);
        Assert.Contains("Mixed functional indexes are not partially emitted", doc, StringComparison.Ordinal);
        Assert.Contains("ordinary PostgreSQL B-tree expression indexes", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL expression indexes exposed by `SHOW INDEX`", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific access methods or non-default B-tree key options", doc, StringComparison.Ordinal);
        Assert.Contains("`NULLS NOT DISTINCT` unique column indexes are preserved", doc, StringComparison.Ordinal);
        Assert.Contains("Expression indexes with included columns or `NULLS NOT DISTINCT` uniqueness also", doc, StringComparison.Ordinal);
        Assert.Contains("including descending expression keys and filtered/partial predicates", doc, StringComparison.Ordinal);
        Assert.Contains("`NULLS NOT DISTINCT` uniqueness also", doc, StringComparison.Ordinal);
        Assert.Contains("unrepresentableExpressionIndexes", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("pg_get_indexdef(ix.indexrelid)::text", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("provider-specific B-tree operator classes/collations", doc, StringComparison.Ordinal);
        Assert.Contains("expression indexes with non-default B-tree key options", doc, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_attribute option_att", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("option_att.attnum IS NOT NULL", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("option_opclass.opcdefault = false", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("ix.indcollation[option_key.ord - 1] <> option_att.attcollation", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL B-tree indexes with non-default", doc, StringComparison.Ordinal);
        Assert.Contains("with non-default `NULLS FIRST/LAST` ordering", doc, StringComparison.Ordinal);
        Assert.Contains("indnullsnotdistinct", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL expression-index `NULLS NOT DISTINCT` uniqueness remain provider-owned diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("hasNullsNotDistinct", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("NullsNotDistinct", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("IndexNullSortOrder", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ParseIndexNullSortOrder", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("NullSortOrder", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultOperatorClass", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("hasIndexCollation", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultNullOrdering", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("unrepresentableExpressionIndexes.Contains", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
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
    public void Doc_pins_inverse_many_to_many_scaffolding()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");
        var manyToManyDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldManyToManyJoinDiscovery.cs");
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
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");
        var relationshipDiscoverySource = ReadRelationshipDiscoverySource();
        var checkFeatureConfigurationBuilderSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCheckFeatureConfigurationBuilder.cs");
        var foreignKeyDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.cs");
        var indexDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.cs");
        var keyDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.cs");
        var sqlServerUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.cs");
        var builderSource = ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.cs");
        var configurationSource = ReadRepoFile("src", "nORM", "Configuration", "IEntityTypeConfiguration.cs");
        var snapshotSource = ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.cs");

        Assert.Contains("generated output are ordered deterministically", doc, StringComparison.Ordinal);
        Assert.Contains("Relationship navigations and fluent relationship", doc, StringComparison.Ordinal);
        Assert.Contains("provider-reported foreign key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server foreign-key names marked", doc, StringComparison.Ordinal);
        Assert.Contains("generated-versus-declared name flag", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server CHECK constraint", doc, StringComparison.Ordinal);
        Assert.Contains("CK_<Entity>_<hash>", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite autoindex", doc, StringComparison.Ordinal);
        Assert.Contains("UX_<Table>_<Columns>", doc, StringComparison.Ordinal);
        Assert.Contains("action-aware `UsingTable` overload", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server and PostgreSQL primary-key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server system-generated names", doc, StringComparison.Ordinal);
        Assert.Contains("ConstraintName", source, StringComparison.Ordinal);
        Assert.Contains("fk.is_system_named AS IsSyntheticConstraintName", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("foreignKey.IsSyntheticConstraintName ? null", relationshipDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("MarkSystemNamedCheckConstraintFeaturesAsync", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("BuildGeneratedCheckConstraintName", checkFeatureConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeSyntheticIndexNames", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("RequiresExplicitManyToManyReferentialActions", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", builderSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", configurationSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", snapshotSource, StringComparison.Ordinal);
        Assert.Contains("sqlite_autoindex_", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("kc.unique_index_id = i.index_id", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("GetPrimaryKeyConstraintNamesAsync", source, StringComparison.Ordinal);
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
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");

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
        Assert.Contains("provider is SqliteProvider", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Docs_and_gate_pin_recent_scaffold_hardening_scope()
    {
        var doc = ReadDoc();
        var rootReadme = ReadRepoFile("README.md");
        var cliReadme = ReadRepoFile("src", "dotnet-norm", "README.md");
        var releaseGates = ReadRepoFile("docs", "release-gates.md");
        var source = ReadStaticEntityScaffoldSource();
        var joinDiagnosticBuilderSource = ReadJoinTableDiagnosticSource();
        var relationshipDiscoverySource = ReadRelationshipDiscoverySource();
        var featureMapBuilderSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureMapBuilder.cs");
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");
        var diagnosticsWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.cs");
        var manyToManyDiscoverySource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldManyToManyJoinDiscovery.cs");
        var postgresUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.cs");
        var sqlServerUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.cs");
        var mySqlUnsupportedSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlUnsupportedFeatureDiscovery.cs");
        var dynamicSource = ReadDynamicEntitySource();
        var providerTypeClassifierSource = ReadProviderSpecificTypeClassifierSource();
        var unsupportedMetadataSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.cs");
        var defaultValidator = ReadRepoFile("src", "nORM", "Migration", "DefaultValueValidator.cs");
        var liveScaffoldTests = ReadRepoFile("tests", "LiveProviderScaffoldingParityTests.cs");
        var liveScaffoldCliTests = ReadRepoFile("tests", "LiveProviderScaffoldCliParityTests.cs");

        Assert.Contains("Repeated scaffolds of the same schema", cliReadme, StringComparison.Ordinal);
        Assert.Contains("ordered deterministically", cliReadme, StringComparison.Ordinal);
        Assert.Contains("stale `nORM.ScaffoldWarnings.*`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("role-based self-referencing FK and self-join navigations", cliReadme, StringComparison.Ordinal);
        Assert.Contains("SQLite rowid integer primary keys are generated as non-null `long`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("unsafe provider-specific columns such as SQL Server/SQLite spatial", rootReadme, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL `inet`, or MySQL spatial columns", rootReadme, StringComparison.Ordinal);
        Assert.Contains("unsafe provider-specific columns such as SQL Server/SQLite spatial", cliReadme, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL `inet`, or MySQL spatial columns", cliReadme, StringComparison.Ordinal);
        Assert.Contains("unsafe MySQL `set(...)` declarations", rootReadme, StringComparison.Ordinal);
        Assert.Contains("unsafe MySQL `set(...)` declarations", cliReadme, StringComparison.Ordinal);
        Assert.Contains("exact ordered unfiltered unique indexes", rootReadme, StringComparison.Ordinal);
        Assert.Contains("exact ordered unfiltered unique index", cliReadme, StringComparison.Ordinal);
        Assert.Contains("one-to-one reference navigations for exact unique dependent FKs", rootReadme, StringComparison.Ordinal);
        Assert.Contains("one-to-one reference navigations when the dependent FK columns are exact unique", cliReadme, StringComparison.Ordinal);
        Assert.Contains("complex/provider-specific defaults", rootReadme, StringComparison.Ordinal);
        Assert.Contains("unmodeled complex/provider-specific defaults", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Safe SQL defaults, including vetted PostgreSQL typed-cast defaults", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Safe SQL defaults, including vetted PostgreSQL typed-cast defaults", cliReadme, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL domains over safe scalar/array/enum base types remain diagnostics", rootReadme, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL domains over safe scalar/array/enum base types remain diagnostics", cliReadme, StringComparison.Ordinal);
        Assert.Contains("SQL Server alias types over scaffoldable scalar/binary bases remain diagnostics", rootReadme, StringComparison.Ordinal);
        Assert.Contains("SQL Server alias types over scaffoldable scalar/binary bases remain diagnostics", cliReadme, StringComparison.Ordinal);
        Assert.Contains("TryMapSqlServerAliasBaseClrType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeSqlServerAliasBaseTypeName", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("GetSqlServerAliasBaseMaxLength", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("GetSqlServerAliasColumnBaseTypes", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("TryMapSqlServerAliasBaseClrType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetSqlServerAliasBaseMaxLengthFromTypeText", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("alias base", doc, StringComparison.Ordinal);
        Assert.Contains("bounded string/binary length", doc, StringComparison.Ordinal);
        Assert.Contains("preserves bounded string/numeric facets where provider", rootReadme, StringComparison.Ordinal);
        Assert.Contains("preserves bounded string/numeric facets where provider", cliReadme, StringComparison.Ordinal);
        Assert.Contains("the generated entity read-only", cliReadme, StringComparison.Ordinal);
        Assert.Contains("unknown provider-specific referential actions", rootReadme, StringComparison.Ordinal);
        Assert.Contains("unknown provider-specific referential actions", cliReadme, StringComparison.Ordinal);
        Assert.Contains("unparsed provider-specific identity strategies that make the generated entity read-only", rootReadme, StringComparison.Ordinal);
        Assert.Contains("unparsed provider-specific identity strategies that make the generated entity read-only", cliReadme, StringComparison.Ordinal);
        Assert.Contains("stale warning reports", rootReadme, StringComparison.Ordinal);
        Assert.Contains("SQLite rowid integer primary keys are normalized to non-null `long`", doc, StringComparison.Ordinal);
        Assert.Contains("reference-column `[Required]` parity", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server alias/user-defined column types are reported with schema/name", doc, StringComparison.Ordinal);
        Assert.Contains("base system type, and bounded facets when available", doc, StringComparison.Ordinal);
        Assert.Contains("Unsigned decimal/numeric columns keep `decimal` storage plus precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("larger or", doc, StringComparison.Ordinal);
        Assert.Contains("ambiguous `set(...)` definitions remain diagnostics and make static or", doc, StringComparison.Ordinal);
        Assert.Contains("malformed comma structure", doc, StringComparison.Ordinal);
        Assert.Contains("runtime dynamic generated entity types read-only", doc, StringComparison.Ordinal);
        Assert.Contains("Required reference navigations", doc, StringComparison.Ordinal);
        Assert.Contains("Optional reference navigations remain nullable", doc, StringComparison.Ordinal);
        Assert.Contains("one-to-one `HasOne(...).WithOne(...)` relationship", doc, StringComparison.Ordinal);
        Assert.Contains("required and optional single-column FK column sets", doc, StringComparison.Ordinal);
        Assert.Contains("optional composite FK column sets", doc, StringComparison.Ordinal);
        Assert.Contains("dependent FKs keep nullable scalar FK columns", doc, StringComparison.Ordinal);
        Assert.Contains("nullable scalar FK columns and the dependent reference", doc, StringComparison.Ordinal);
        Assert.Contains("shared primary-key one-to-one", doc, StringComparison.Ordinal);
        Assert.Contains("dependent PK is also the FK", doc, StringComparison.Ordinal);
        Assert.Contains("FK-role-based reference navigation", doc, StringComparison.Ordinal);
        Assert.Contains("do not produce possible-join-table warnings", doc, StringComparison.Ordinal);
        Assert.Contains("composite FK relationships use the distinguishing FK", doc, StringComparison.Ordinal);
        Assert.Contains("shared tenant or partition column", doc, StringComparison.Ordinal);
        Assert.Contains("database-name preservation with role-named FK navigations", doc, StringComparison.Ordinal);
        Assert.Contains("one-to-one reference navigation generation for required and", doc, StringComparison.Ordinal);
        Assert.Contains("shared-primary-key", doc, StringComparison.Ordinal);
        Assert.Contains("role-named multiple unique dependent FKs", doc, StringComparison.Ordinal);
        Assert.Contains("otherwise the principal side remains a collection navigation", doc, StringComparison.Ordinal);
        Assert.Contains("nullable and non-null alternate-key FK generation", doc, StringComparison.Ordinal);
        Assert.Contains("Self-referencing pure join tables", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_one_to_one_for_unique_dependent_fk_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_optional_one_to_one_for_nullable_unique_dependent_fk_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_role_named_one_to_one_for_multiple_unique_dependent_fks_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_one_to_one_for_shared_primary_key_fk_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_one_to_one_for_composite_unique_dependent_fk_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_optional_one_to_one_for_nullable_composite_unique_dependent_fk_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_role_named_navigations_for_multiple_composite_fks_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_preserves_database_names_and_role_navigations_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_generates_fk_to_nullable_unique_index_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_database_names_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_mixed_single_fk_and_many_to_many_shape_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_composite_primary_key_fk_model_shape_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_composite_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_many_to_many_referential_actions_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_shared_tenant_alternate_key_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_alternate_key_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_self_referencing_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_surrogate_key_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_database_generated_bridge_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_composite_surrogate_key_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_keeps_composite_payload_join_as_explicit_entity_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_rejects_filtered_unique_surrogate_join_table_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_schema_qualified_many_to_many_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_composite_fk_to_unique_index_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_role_named_composite_fks_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_role_named_one_to_one_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_required_and_optional_unique_dependent_one_to_one_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_shared_primary_key_one_to_one_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_composite_unique_dependent_one_to_one_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_optional_composite_unique_dependent_one_to_one_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_suppresses_keyless_dependent_relationship_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_json_fail_on_warnings_reports_live_provider_diagnostics", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_provider_option_accepts_ef_provider_package_names_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_honors_connection_provider_option_precedence_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_dbcontext_scaffold_accepts_ef_provider_package_names_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_valid_unique_identifiers_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_respects_project_namespace_context_dir_and_nullable_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_reads_directory_build_props_project_metadata_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_project_metadata_overrides_directory_build_props_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_accepts_project_directory_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_accepts_project_and_force_short_aliases_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_accepts_short_aliases_and_qualified_context_namespace_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_infers_current_directory_project_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_inferred_current_directory_project_reads_named_connection_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_inferred_current_directory_project_user_secrets_override_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_no_project_reads_named_connection_current_directory_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_no_project_pass_through_environment_selects_current_directory_appsettings_environment_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_uses_dotnet_ef_config_startup_named_connection_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_cli_options_override_dotnet_ef_config_defaults_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_startup_project_named_connection_overrides_target_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_startup_project_directory_named_connection_overrides_target_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_startup_project_named_connection_shorthand_overrides_target_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_startup_project_user_secrets_override_target_user_secrets_and_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_omitted_context_uses_named_connection_leaf_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_named_connection_shorthand_reads_project_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_named_connection_user_secrets_override_project_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_named_connection_environment_overrides_project_appsettings_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_named_connection_environment_overrides_project_user_secrets_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_named_connection_environment_overrides_startup_and_target_user_secrets_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_pass_through_environment_selects_appsettings_environment_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_startup_project_pass_through_environment_selects_startup_appsettings_environment_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_startup_project_dotnet_environment_selects_startup_appsettings_environment_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_ambient_environment_selects_appsettings_environment_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_enforces_output_safety_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_accepts_csv_and_multi_value_table_filters_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_accepts_csv_and_multi_value_schema_filters_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_unions_schema_and_table_filters_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_accepts_no_onconfiguring_data_annotations_and_no_pluralize_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_emits_routine_stubs_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_emits_advanced_routine_stubs_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_emits_routine_output_and_non_query_wrappers_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_emits_sequence_stubs_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_table_filter_suppresses_unselected_principal_relationship_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_table_filter_emits_view_query_artifact_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_emits_provider_query_artifacts_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_default_discovery_emits_table_and_view_query_artifacts_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_non_nullable_alternate_key_relationship_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_generates_nullable_alternate_key_relationship_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_suppresses_synthetic_fk_constraint_names_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_promotes_safe_feature_metadata_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_suppresses_synthetic_check_constraint_names_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_suppresses_synthetic_unique_constraint_index_names_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_reports_trigger_diagnostics_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_reports_provider_specific_column_diagnostics_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_safe_provider_specific_columns_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_writable_provider_specific_diagnostics_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_fk_referential_actions_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_restrict_fk_referential_actions_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_set_default_fk_referential_actions_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_scalar_facets_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_index_metadata_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_provider_index_metadata_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("--use-database-names --no-pluralize", doc, StringComparison.Ordinal);
        Assert.Contains("Invalid table/column identifiers", doc, StringComparison.Ordinal);
        Assert.Contains("duplicate normalized entity", doc, StringComparison.Ordinal);
        Assert.Contains("object-member column names", doc, StringComparison.Ordinal);
        Assert.Contains("Project-targeted scaffold", doc, StringComparison.Ordinal);
        Assert.Contains("`--project`, project-relative `--output-dir`", doc, StringComparison.Ordinal);
        Assert.Contains("project nullable-reference settings", doc, StringComparison.Ordinal);
        Assert.Contains("Inherited", doc, StringComparison.Ordinal);
        Assert.Contains("`Directory.Build.props` metadata", doc, StringComparison.Ordinal);
        Assert.Contains("root namespace, nullable-reference settings", doc, StringComparison.Ordinal);
        Assert.Contains("user-secrets connection", doc, StringComparison.Ordinal);
        Assert.Contains("Project-file", doc, StringComparison.Ordinal);
        Assert.Contains("metadata overrides conflicting", doc, StringComparison.Ordinal);
        Assert.Contains("AssemblyName fallback", doc, StringComparison.Ordinal);
        Assert.Contains("Passing a single-project", doc, StringComparison.Ordinal);
        Assert.Contains("directory to `--project` instead of a `.csproj` file", doc, StringComparison.Ordinal);
        Assert.Contains("relative output and context paths remain project-relative", doc, StringComparison.Ordinal);
        Assert.Contains("Short `-p` and `-f` aliases", doc, StringComparison.Ordinal);
        Assert.Contains("overwrite stale project-relative generated files", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style short aliases", doc, StringComparison.Ordinal);
        Assert.Contains("`-o`, `-n`, `-c`, and `-t`", doc, StringComparison.Ordinal);
        Assert.Contains("`--context-namespace` overrides", doc, StringComparison.Ordinal);
        Assert.Contains("final context class-name segment", doc, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", doc, StringComparison.Ordinal);
        Assert.Contains("infers that current-directory project", doc, StringComparison.Ordinal);
        Assert.Contains("inferred current-directory project appsettings", doc, StringComparison.Ordinal);
        Assert.Contains("inferred current-directory project user secrets", doc, StringComparison.Ordinal);
        Assert.Contains("override inferred-project appsettings", doc, StringComparison.Ordinal);
        Assert.Contains("No-project current-directory appsettings", doc, StringComparison.Ordinal);
        Assert.Contains("no-project current-directory environment-specific appsettings", doc, StringComparison.Ordinal);
        Assert.Contains("current-directory-relative output and context paths", doc, StringComparison.Ordinal);
        Assert.Contains("project-relative output and context paths", doc, StringComparison.Ordinal);
        Assert.Contains("builds the generated project", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style `.config/dotnet-ef.json`", doc, StringComparison.Ordinal);
        Assert.Contains("config-supplied `project`, `startupProject`, and `context`", doc, StringComparison.Ordinal);
        Assert.Contains("`Name=...` named connections from startup-project `appsettings.json`", doc, StringComparison.Ordinal);
        Assert.Contains("Explicit CLI", doc, StringComparison.Ordinal);
        Assert.Contains("`--project`, `--startup-project`, and", doc, StringComparison.Ordinal);
        Assert.Contains("`--context` values", doc, StringComparison.Ordinal);
        Assert.Contains("override conflicting", doc, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", doc, StringComparison.Ordinal);
        Assert.Contains("Explicit `--startup-project`/`-s` named-connection", doc, StringComparison.Ordinal);
        Assert.Contains("startup-project", doc, StringComparison.Ordinal);
        Assert.Contains("`appsettings.json` wins over target-project", doc, StringComparison.Ordinal);
        Assert.Contains("target-project `appsettings.json`", doc, StringComparison.Ordinal);
        Assert.Contains("single-project startup directories are accepted", doc, StringComparison.Ordinal);
        Assert.Contains("startup-project shorthand `Name=AppDb`", doc, StringComparison.Ordinal);
        Assert.Contains("resolves through startup-project", doc, StringComparison.Ordinal);
        Assert.Contains("`ConnectionStrings:AppDb` entries", doc, StringComparison.Ordinal);
        Assert.Contains("Startup-project", doc, StringComparison.Ordinal);
        Assert.Contains("user secrets declared through `UserSecretsId`", doc, StringComparison.Ordinal);
        Assert.Contains("startup-project user-secret connection wins over", doc, StringComparison.Ordinal);
        Assert.Contains("target-project user secrets", doc, StringComparison.Ordinal);
        Assert.Contains("Omitting `--context` with a named connection", doc, StringComparison.Ordinal);
        Assert.Contains("uses that named leaf", doc, StringComparison.Ordinal);
        Assert.Contains("duplicate `Context` suffix", doc, StringComparison.Ordinal);
        Assert.Contains("Lowercase named-connection shorthand", doc, StringComparison.Ordinal);
        Assert.Contains("target-project", doc, StringComparison.Ordinal);
        Assert.Contains("`ConnectionStrings:AppDb` appsettings", doc, StringComparison.Ordinal);
        Assert.Contains("Named-connection environment variable precedence", doc, StringComparison.Ordinal);
        Assert.Contains("`ConnectionStrings__Name`", doc, StringComparison.Ordinal);
        Assert.Contains("also override project user secrets", doc, StringComparison.Ordinal);
        Assert.Contains("startup-project and target-project user secrets", doc, StringComparison.Ordinal);
        Assert.Contains("environment-selected live database", doc, StringComparison.Ordinal);
        Assert.Contains("Project user secrets declared through `UserSecretsId`", doc, StringComparison.Ordinal);
        Assert.Contains("user-secret connection wins over target-project", doc, StringComparison.Ordinal);
        Assert.Contains("secret-selected", doc, StringComparison.Ordinal);
        Assert.Contains("Ambient", doc, StringComparison.Ordinal);
        Assert.Contains("startup-project", doc, StringComparison.Ordinal);
        Assert.Contains("`appsettings.Production.json`", doc, StringComparison.Ordinal);
        Assert.Contains("target-project environment files", doc, StringComparison.Ordinal);
        Assert.Contains("`ASPNETCORE_ENVIRONMENT=Staging`", doc, StringComparison.Ordinal);
        Assert.Contains("`DOTNET_ENVIRONMENT=Development`", doc, StringComparison.Ordinal);
        Assert.Contains("startup-project `appsettings.Development.json`", doc, StringComparison.Ordinal);
        Assert.Contains("environment-specific appsettings lookup builds", doc, StringComparison.Ordinal);
        Assert.Contains("Output-safety semantics", doc, StringComparison.Ordinal);
        Assert.Contains("successful `--dry-run --json`", doc, StringComparison.Ordinal);
        Assert.Contains("`--no-overwrite`", doc, StringComparison.Ordinal);
        Assert.Contains("`--force --json` removes stale", doc, StringComparison.Ordinal);
        Assert.Contains("CSV `--tables` filters", doc, StringComparison.Ordinal);
        Assert.Contains("multi-value `--table First Second`", doc, StringComparison.Ordinal);
        Assert.Contains("filter parsing and provider", doc, StringComparison.Ordinal);
        Assert.Contains("catalog matching stay aligned", doc, StringComparison.Ordinal);
        Assert.Contains("CSV `--schemas` filters", doc, StringComparison.Ordinal);
        Assert.Contains("multi-value `--schema Accounting Sales` filters", doc, StringComparison.Ordinal);
        Assert.Contains("real CLI across all four providers", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL's current database as a filter-only catalog", doc, StringComparison.Ordinal);
        Assert.Contains("unqualified model metadata", doc, StringComparison.Ordinal);
        Assert.Contains("Combined `--schema` plus explicit `--table`", doc, StringComparison.Ordinal);
        Assert.Contains("schema-selected tables and explicitly selected tables are generated", doc, StringComparison.Ordinal);
        Assert.Contains("outside the selected SQL Server/PostgreSQL schemas stay", doc, StringComparison.Ordinal);
        Assert.Contains("`--no-onconfiguring` emits no `OnConfiguring`", doc, StringComparison.Ordinal);
        Assert.Contains("hard-coded scaffold connection string", doc, StringComparison.Ordinal);
        Assert.Contains("`--data-annotations` is accepted", doc, StringComparison.Ordinal);
        Assert.Contains("`--no-pluralize` keeps", doc, StringComparison.Ordinal);
        Assert.Contains("common", doc, StringComparison.Ordinal);
        Assert.Contains("output switches `--json`, `--verbose`, `--no-color`, and `--prefix-output`", doc, StringComparison.Ordinal);
        Assert.Contains("successful machine-readable summary", doc, StringComparison.Ordinal);
        Assert.Contains("\"--json \"", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("\"--verbose \"", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("\"--no-color \"", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("\"--prefix-output \"", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("alternate-key many-to-many", doc, StringComparison.Ordinal);
        Assert.Contains("--fail-on-warnings --json", doc, StringComparison.Ordinal);
        Assert.Contains("Explicit `--provider <EF Core provider package>`", doc, StringComparison.Ordinal);
        Assert.Contains("parser precedence", doc, StringComparison.Ordinal);
        Assert.Contains("`--connection` and `--provider` values", doc, StringComparison.Ordinal);
        Assert.Contains("override conflicting positional", doc, StringComparison.Ordinal);
        Assert.Contains("values, and a single positional provider", doc, StringComparison.Ordinal);
        Assert.Contains("single positional provider after `--connection`", doc, StringComparison.Ordinal);
        Assert.Contains("dbcontext scaffold <connection> <provider-package>", doc, StringComparison.Ordinal);
        Assert.Contains("compatibility design-time switch bundle", doc, StringComparison.Ordinal);
        Assert.Contains("`--no-build`, `--framework`", doc, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-bound routine stubs are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("--emit-routine-stubs", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server/PostgreSQL/MySQL routine comments", doc, StringComparison.Ordinal);
        Assert.Contains("Advanced routine wrappers", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server scalar/table-valued", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL array/UUID routine parameters", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL unsigned", doc, StringComparison.Ordinal);
        Assert.Contains("output-parameter routine factories", doc, StringComparison.Ordinal);
        Assert.Contains("no-result stored procedure non-query wrappers", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-bound sequence stubs are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("--emit-sequence-stubs", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server/PostgreSQL sequence comments", doc, StringComparison.Ordinal);
        Assert.Contains("relationships to unselected principal tables are", doc, StringComparison.Ordinal);
        Assert.Contains("mixed one-to-many plus many-to-many model shape", doc, StringComparison.Ordinal);
        Assert.Contains("composite index metadata through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("direct composite primary-key FK model shape", doc, StringComparison.Ordinal);
        Assert.Contains("generated composite `HasKey` and fluent FK selectors", doc, StringComparison.Ordinal);
        Assert.Contains("selected ordinary", doc, StringComparison.Ordinal);
        Assert.Contains("views are emitted as read-only query artifacts", doc, StringComparison.Ordinal);
        Assert.Contains("Default and schema-scoped CLI discovery also", doc, StringComparison.Ordinal);
        Assert.Contains("ordinary views are emitted with discovered tables", doc, StringComparison.Ordinal);
        Assert.Contains("Provider query-artifact opt-in is", doc, StringComparison.Ordinal);
        Assert.Contains("--emit-query-artifacts", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite virtual tables", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server local table synonyms", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL materialized", doc, StringComparison.Ordinal);
        Assert.Contains("nullable", doc, StringComparison.Ordinal);
        Assert.Contains("nullable and non-null alternate-key FK relationships", doc, StringComparison.Ordinal);
        Assert.Contains("non-default FK delete/update referential actions", doc, StringComparison.Ordinal);
        Assert.Contains("RESTRICT and SET DEFAULT referential actions where providers expose them", doc, StringComparison.Ordinal);
        Assert.Contains("safe defaults, table CHECK constraints", doc, StringComparison.Ordinal);
        Assert.Contains("computed/generated columns", doc, StringComparison.Ordinal);
        Assert.Contains("provider-native table/column comments", doc, StringComparison.Ordinal);
        Assert.Contains("explicit SQL Server/PostgreSQL primary-key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("primary-key constraint-name", releaseGates, StringComparison.Ordinal);
        Assert.Contains("preservation while MySQL's fixed `PRIMARY` metadata", releaseGates, StringComparison.Ordinal);
        Assert.Contains("parentPkName", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_builds_composite_primary_key_fk_model_shape_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Routine <summary> & description", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Sequence <summary> & description", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("SQL Server local-synonym comments", doc, StringComparison.Ordinal);
        Assert.Contains("view/materialized-view query-artifact comments", doc, StringComparison.Ordinal);
        Assert.Contains("generated XML documentation", doc, StringComparison.Ordinal);
        Assert.Contains("downgraded to provider-owned diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server/PostgreSQL/MySQL routine comments", releaseGates, StringComparison.Ordinal);
        Assert.Contains("SQL Server/PostgreSQL sequence comments", releaseGates, StringComparison.Ordinal);
        Assert.Contains("Trigger-owned tables are", doc, StringComparison.Ordinal);
        Assert.Contains("trigger metadata across all four live providers", doc, StringComparison.Ordinal);
        Assert.Contains("Unsafe provider-specific columns are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("Safe provider-specific columns are also verified through the real CLI across", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite UUID/JSON/XML declared types, SQL", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL UUID/array columns, and MySQL JSON/YEAR/enum/set", doc, StringComparison.Ordinal);
        Assert.Contains("Writable provider-specific diagnostics are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server alias types, PostgreSQL domains over safe base types, and MySQL", doc, StringComparison.Ordinal);
        Assert.Contains("SCF104 remains in", doc, StringComparison.Ordinal);
        Assert.Contains("SCF104", doc, StringComparison.Ordinal);
        Assert.Contains("Decimal precision plus", doc, StringComparison.Ordinal);
        Assert.Contains("bounded string/binary length, Unicode, and fixed-length facets", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite kept in the same command-path build gate", doc, StringComparison.Ordinal);
        Assert.Contains("Ordinary", doc, StringComparison.Ordinal);
        Assert.Contains("ordered unique composite indexes", doc, StringComparison.Ordinal);
        Assert.Contains("generated `[Index]` attributes", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-bound filtered/partial predicates", doc, StringComparison.Ordinal);
        Assert.Contains("included-column index metadata", doc, StringComparison.Ordinal);
        Assert.Contains("real CLI where each provider exposes the feature", doc, StringComparison.Ordinal);
        Assert.Contains("Multiple composite FKs to", doc, StringComparison.Ordinal);
        Assert.Contains("role-named navigations", doc, StringComparison.Ordinal);
        Assert.Contains("payload-bridge diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("Direct composite alternate-key FKs are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("Multiple unique dependent", doc, StringComparison.Ordinal);
        Assert.Contains("role-named one-to-one relationships", doc, StringComparison.Ordinal);
        Assert.Contains("Single-column required and nullable", doc, StringComparison.Ordinal);
        Assert.Contains("combined real CLI scaffold", doc, StringComparison.Ordinal);
        Assert.Contains("Composite unique-dependent FKs", doc, StringComparison.Ordinal);
        Assert.Contains("required one-to-one relationships", doc, StringComparison.Ordinal);
        Assert.Contains("nullable composite unique-dependent FKs", doc, StringComparison.Ordinal);
        Assert.Contains("optional one-to-one relationships", doc, StringComparison.Ordinal);
        Assert.Contains("Shared primary-key FK shapes", doc, StringComparison.Ordinal);
        Assert.Contains("one-to-one relationships through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("Keyless dependent FK shapes", doc, StringComparison.Ordinal);
        Assert.Contains("navigation-free", doc, StringComparison.Ordinal);
        Assert.Contains("RelationshipDependentKey", doc, StringComparison.Ordinal);
        Assert.Contains("Self-referencing pure join tables are also verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("distinct role-based skip navigations", doc, StringComparison.Ordinal);
        Assert.Contains("Pure composite primary-key join tables are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("exact composite FK column arrays", doc, StringComparison.Ordinal);
        Assert.Contains("Shared-tenant alternate-key join tables are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("composite alternate-key principal selectors", doc, StringComparison.Ordinal);
        Assert.Contains("Generated-surrogate pure join", doc, StringComparison.Ordinal);
        Assert.Contains("exact unique FK-pair index", doc, StringComparison.Ordinal);
        Assert.Contains("Database-generated bridge columns", doc, StringComparison.Ordinal);
        Assert.Contains("do not incorrectly force payload join entities", doc, StringComparison.Ordinal);
        Assert.Contains("Composite generated-surrogate pure join", doc, StringComparison.Ordinal);
        Assert.Contains("exact composite FK-pair index", doc, StringComparison.Ordinal);
        Assert.Contains("Composite payload join tables are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("payload-column diagnostics without composite-FK rejection", doc, StringComparison.Ordinal);
        Assert.Contains("Filtered unique surrogate join tables are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("remain explicit entities instead of unsafe skip navigations", doc, StringComparison.Ordinal);
        Assert.Contains("Schema-qualified many-to-many join tables are verified through the real CLI", doc, StringComparison.Ordinal);
        Assert.Contains("schema-aware `UsingTable` mapping", doc, StringComparison.Ordinal);

        Assert.Contains("Scaffold_sqlite_output_builds_as_consumer_project", releaseGates, StringComparison.Ordinal);
        Assert.Contains("Scaffold_clean_run_removes_stale_warning_reports_without_printing_summary", releaseGates, StringComparison.Ordinal);
        Assert.Contains("LiveProviderScaffoldingParityTests", releaseGates, StringComparison.Ordinal);
        Assert.Contains("LiveProviderScaffoldCliParityTests", releaseGates, StringComparison.Ordinal);
        Assert.Contains("already-built `nORM.dll`", releaseGates, StringComparison.Ordinal);

        Assert.Contains("NormalizeScaffoldClrType", source, StringComparison.Ordinal);
        Assert.Contains("effectiveAllowNull", source, StringComparison.Ordinal);
        Assert.Contains("reference.IsRequired", source, StringComparison.Ordinal);
        Assert.Contains("IsRequired = isRequired", relationshipDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("isUniqueDependentKey", relationshipDiscoverySource, StringComparison.Ordinal);
        Assert.Contains(".HasOne(p => p.", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("isSelfRelationship", relationshipDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("isSelfJoin", manyToManyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("LeftCollectionNavigationName", source, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes)", source, StringComparison.Ordinal);
        Assert.Contains("IsSafeMySqlUnsignedDecimalType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("'bigint', 'decimal', 'numeric'", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("IsSafeSqlServerAliasColumnType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("TryGetSqlServerAliasBaseTypeText", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("IsSafeSqlServerAliasBaseType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("c.max_length / 2", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric')", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric')", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("base_ty.name NOT IN", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsSafePostgresDomainColumnType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("TryNormalizePostgresParameterizedProbeCastType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("TryNormalizeSafePostgresDomainProbeCastType(detail, out var safeCastType)", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("TryParsePostgresDomainEnumValues", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("!typeText.StartsWith(\"ENUM\"", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("character_maximum_length::text", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("numeric_precision::text", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("c.domain_name IS NULL", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.DoesNotContain("c.domain_name IS NOT NULL", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("providerSpecificDefaultTableKeys.Contains(tableKey)", source, StringComparison.Ordinal);
        Assert.Contains("BuildProviderOwnedWriteBlockedTableKeys", featureMapBuilderSource, StringComparison.Ordinal);
        Assert.Contains("providerOwnedWriteBlockedTableKeys.Contains(canonicalJoinTableKey)", manyToManyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("provider-owned-write-blocking-schema", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("providerOwnedWriteBlockingSchema", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("candidateQueryArtifacts", source, StringComparison.Ordinal);
        Assert.Contains("emittedQueryArtifacts", source, StringComparison.Ordinal);
        Assert.Contains("selectedTableKeys", source, StringComparison.Ordinal);
        Assert.Contains("provider-specific-default", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("QUOTE(column_default)", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("providerSpecificIdentityStrategyTableKeys.Contains(tableKey)", source, StringComparison.Ordinal);
        Assert.Contains("provider-specific-identity-strategy", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("referential-action-not-scaffoldable", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("HasOnlyScaffoldableReferentialActions", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("unmodeled complex/provider-specific defaults remain diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL `citext`/`json`/`jsonb`/`xml`/`uuid`", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific schema for provider-mobility review", doc, StringComparison.Ordinal);
        Assert.Contains("scaffoldable citext/JSON/XML/UUID scalar storage", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("CURRENT_TIMESTAMP()", doc, StringComparison.Ordinal);
        Assert.Contains("CURRENT_TIMESTAMP(6)", doc, StringComparison.Ordinal);
        Assert.Contains("Unsupported/provider-specific FK referential action discovered", doc, StringComparison.Ordinal);
        Assert.Contains("Valid `NO ACTION`, `CASCADE`, `SET NULL`, `RESTRICT`, and `SET DEFAULT` actions are emitted", doc, StringComparison.Ordinal);
        Assert.Contains("'draft'::text", doc, StringComparison.Ordinal);
        Assert.Contains("now()::timestamp without time zone", doc, StringComparison.Ordinal);
        Assert.Contains("now() AT TIME ZONE 'utc'", doc, StringComparison.Ordinal);
        Assert.Contains("timezone('utc', now())", doc, StringComparison.Ordinal);
        Assert.Contains("Safe PostgreSQL cast suffixes", defaultValidator, StringComparison.Ordinal);
        Assert.Contains("Strict PostgreSQL UTC timestamp defaults", defaultValidator, StringComparison.Ordinal);
        Assert.Contains("at\\s+time\\s+zone\\s+'utc'", defaultValidator, StringComparison.Ordinal);
        Assert.Contains("timezone\\s*\\(", defaultValidator, StringComparison.Ordinal);
        Assert.Contains("character\\s+varying", defaultValidator, StringComparison.Ordinal);
        Assert.Contains("timestamp\\s+(?:with|without)\\s+time\\s+zone", defaultValidator, StringComparison.Ordinal);
        Assert.Contains("unparsed strategies remain diagnostics and make the generated entity `[ReadOnlyEntity]`", doc, StringComparison.Ordinal);
        Assert.Contains("referential-action-not-scaffoldable", doc, StringComparison.Ordinal);
        Assert.Contains("provider-owned write-blocking schema", doc, StringComparison.Ordinal);
        Assert.Contains("only selected query artifacts count as emitted", doc, StringComparison.Ordinal);
        Assert.Contains("unselected SQLite virtual-table shadow storage is not reported", doc, StringComparison.Ordinal);
        Assert.Contains("NormalizeScaffoldClrType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("effectiveAllowNull", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("existingPropertyNames.Add(EscapeCSharpIdentifier(ToPascalCase(tableName)))", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetScaffoldMaxLength(normalizedClrType, row)", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsUnboundedScaffoldMaxLength", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldDecimalPrecision", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetDecimalPrecisions", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("QueryDecimalPrecisionMap", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("nameof(ColumnAttribute.TypeName)", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("column.DecimalPrecision", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("RequiredAttribute", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("BuildSchemaDescriptor", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetSqliteDeclaredColumnTypes", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetPostgresDomainColumnCastTypes", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsSafePostgresUserDefinedScalarColumnType", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("data_type = 'ARRAY'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("AND COALESCE(udt_name, '') IN (", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("'_timestamptz'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("AND COALESCE(c.udt_name, '') NOT IN ('citext', 'json', 'jsonb', 'xml', 'uuid')", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeMySqlUnsignedTypeDetail", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsReadOnlyDynamicObject", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsDynamicQueryObject", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsProviderOwnedSynonym", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("sys.synonyms", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsProviderNativeTemporalTable", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("HasProviderOwnedTriggers", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("HasUnmodeledDefaults", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("TryNormalizeDynamicDefaultSql", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("AND is_identity <> 'YES'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("QUOTE(column_default)", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("HasWriteBlockingProviderSpecificColumns", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("ty.name IN ('geography', 'geometry', 'hierarchyid', 'sql_variant')", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("c.udt_name IN ('inet', 'cidr', 'macaddr', 'macaddr8', 'tsvector', 'tsquery')", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("'geometrycollection'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("'multipolygon'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("HasWriteBlockingMySqlSetColumns", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("TryParseBoundedMySqlSetValues", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("data_type = 'set'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("c.data_type = 'ARRAY'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("COALESCE(c.udt_name, '') NOT IN", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("'_int4'", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsUnsafeSqliteProviderSpecificDeclaredType", source, StringComparison.Ordinal);
        Assert.Contains("IsUnsafeSqliteProviderSpecificDeclaredType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("ContainsSqliteDeclaredTypeToken", source, StringComparison.Ordinal);
        Assert.Contains("ContainsSqliteDeclaredTypeToken", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("!DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalized)", source, StringComparison.Ordinal);
        Assert.Contains("!IsUnsafeSqliteProviderSpecificDeclaredType(normalized)", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("IsWriteBlockingSqliteDeclaredType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("RO", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("SQLite `UUID` declared-type parity", doc, StringComparison.Ordinal);
        Assert.Contains("keyless dynamic", doc, StringComparison.Ordinal);
        Assert.Contains("provider-owned dynamic read-only parity", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server synonyms", doc, StringComparison.Ordinal);
        Assert.Contains("unmodeled defaults", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL `information_schema`", doc, StringComparison.Ordinal);
        Assert.Contains("provider-owned triggers", doc, StringComparison.Ordinal);
        Assert.Contains("write-blocking", doc, StringComparison.Ordinal);
        Assert.Contains("unsafe PostgreSQL provider-specific array columns", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite declared", doc, StringComparison.Ordinal);
        Assert.Contains("spatial provider-specific column diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("unsafe MySQL", doc, StringComparison.Ordinal);
        Assert.Contains("`set(...)` declarations", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific column types", doc, StringComparison.Ordinal);
        Assert.Contains("avoid the enclosing entity type name", doc, StringComparison.Ordinal);
        Assert.Contains("composite primary-key ordinal parity", doc, StringComparison.Ordinal);
        Assert.Contains("normalized dynamic `MaxLength` metadata", doc, StringComparison.Ordinal);
        Assert.Contains("dynamic decimal precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("Static and runtime dynamic scaffolding emit", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL array/domain-column schema probes", doc, StringComparison.Ordinal);
        Assert.Contains("safe-domain diagnostics do not make the generated entity `[ReadOnlyEntity]`", doc, StringComparison.Ordinal);
        Assert.Contains("GetPrimaryKeyOrdinals", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("ReadOnlyEntityAttribute", dynamicSource, StringComparison.Ordinal);
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
    public void Doc_and_cli_pin_repeatable_table_filter_for_literal_commas()
    {
        var doc = ReadDoc();
        var cliSource = ReadRepoFile("src", "dotnet-norm", "Program.cs");
        var scaffolderSource = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        var tableFilterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.cs");
        var contextWriterSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs");
        var cliReadme = ReadRepoFile("src", "dotnet-norm", "README.md");
        var rootReadme = ReadRepoFile("README.md");

        Assert.Contains("repeatable CLI `--table`", doc, StringComparison.Ordinal);
        Assert.Contains("repeatable CLI `--schema`", doc, StringComparison.Ordinal);
        Assert.Contains("multi-value `--table First Second`", doc, StringComparison.Ordinal);
        Assert.Contains("multi-value `--schema Accounting Sales`", doc, StringComparison.Ordinal);
        Assert.Contains("`schema.table`", doc, StringComparison.Ordinal);
        Assert.Contains("`schema.view`", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.Schemas", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.PluralizeQueryProperties", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.UseDatabaseNames", doc, StringComparison.Ordinal);
        Assert.Contains("routine result-column names", doc, StringComparison.Ordinal);
        Assert.Contains("Synthetic navigation", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style CLI aliases", doc, StringComparison.Ordinal);
        Assert.Contains("`--data-annotations`/`-d`", doc, StringComparison.Ordinal);
        Assert.Contains("`--force`/`-f`", doc, StringComparison.Ordinal);
        Assert.Contains("CLI refuses to overwrite existing generated files", doc, StringComparison.Ordinal);
        Assert.Contains("Passing both `--force` and `--no-overwrite` is rejected", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style positional CLI arguments", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style namespace-qualified context names", doc, StringComparison.Ordinal);
        Assert.Contains("validated as C# namespaces before generation", doc, StringComparison.Ordinal);
        Assert.Contains("class-name segments are validated as C# type", doc, StringComparison.Ordinal);
        Assert.Contains("Blank explicit CLI string values", doc, StringComparison.Ordinal);
        Assert.Contains("When CLI `--context` is omitted", doc, StringComparison.Ordinal);
        Assert.Contains("norm scaffold <connection> <provider>", doc, StringComparison.Ordinal);
        Assert.Contains("norm dbcontext scaffold <connection> <provider>", doc, StringComparison.Ordinal);
        Assert.Contains("advertises `dbcontext` as an EF-style alias group", doc, StringComparison.Ordinal);
        Assert.Contains("positional value after `--connection`", doc, StringComparison.Ordinal);
        Assert.Contains("EF Core provider package names", doc, StringComparison.Ordinal);
        Assert.Contains("positional scaffold provider", doc, StringComparison.Ordinal);
        Assert.Contains("explicit `--provider` option", doc, StringComparison.Ordinal);
        Assert.Contains("Microsoft.EntityFrameworkCore.Sqlite", doc, StringComparison.Ordinal);
        Assert.Contains("Npgsql.EntityFrameworkCore.PostgreSQL", doc, StringComparison.Ordinal);
        Assert.Contains("Name=ConnectionStrings:AppDb", doc, StringComparison.Ordinal);
        Assert.Contains("ConnectionStrings__AppDb", doc, StringComparison.Ordinal);
        Assert.Contains("environment variables first", doc, StringComparison.Ordinal);
        Assert.Contains("UserSecretsId", doc, StringComparison.Ordinal);
        Assert.Contains("user secrets", doc, StringComparison.Ordinal);
        Assert.Contains("does not execute startup code", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style project targeting", doc, StringComparison.Ordinal);
        Assert.Contains("paths are resolved below that project directory", doc, StringComparison.Ordinal);
        Assert.Contains("plus sanitized output directory", doc, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", doc, StringComparison.Ordinal);
        Assert.Contains("project-relative placement", doc, StringComparison.Ordinal);
        Assert.Contains("project's root namespace plus", doc, StringComparison.Ordinal);
        Assert.Contains("nullable-reference defaults", doc, StringComparison.Ordinal);
        Assert.Contains("`Directory.Build.props`", doc, StringComparison.Ordinal);
        Assert.Contains("`--startup-project`/`-s`, `--framework`", doc, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", doc, StringComparison.Ordinal);
        Assert.Contains("invoke MSBuild", doc, StringComparison.Ordinal);
        Assert.Contains("application arguments after `--`", doc, StringComparison.Ordinal);
        Assert.Contains("`-- --environment Production`", doc, StringComparison.Ordinal);
        Assert.Contains("`appsettings.Production.json`", doc, StringComparison.Ordinal);
        Assert.Contains("`ASPNETCORE_ENVIRONMENT`", doc, StringComparison.Ordinal);
        Assert.Contains("`DOTNET_ENVIRONMENT`", doc, StringComparison.Ordinal);
        Assert.Contains("typos are not swallowed", doc, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", doc, StringComparison.Ordinal);
        Assert.Contains("`framework`, `configuration`, `runtime`", doc, StringComparison.Ordinal);
        Assert.Contains("`verbose`, `noColor`, and `prefixOutput`", doc, StringComparison.Ordinal);
        Assert.Contains("explicit CLI options take", doc, StringComparison.Ordinal);
        Assert.Contains("EF common output switches", doc, StringComparison.Ordinal);
        Assert.Contains("`--json` emits a machine-readable", doc, StringComparison.Ordinal);
        Assert.Contains("`--no-onconfiguring` is accepted", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextDirectory", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextNamespace", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextOutputDirectory", doc, StringComparison.Ordinal);
        Assert.Contains("matching supported query", doc, StringComparison.Ordinal);
        Assert.Contains("scaffolded by default as read-only query artifacts", doc, StringComparison.Ordinal);
        Assert.Contains("explicitly selected by table/schema filters", doc, StringComparison.Ordinal);
        Assert.Contains("ShouldEmitQueryArtifactObject", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("IsDefaultQueryArtifactObject", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("unioned with explicit table filters", doc, StringComparison.Ordinal);
        Assert.Contains("entity class names and", doc, StringComparison.Ordinal);
        Assert.Contains("output-relative API placement", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style project-relative placement", doc, StringComparison.Ordinal);
        Assert.Contains("CLI `--context-dir` rejects absolute", doc, StringComparison.Ordinal);
        Assert.Contains("literal table names that contain commas", doc, StringComparison.Ordinal);
        Assert.Contains("blank CLI filters are rejected", doc, StringComparison.Ordinal);
        Assert.Contains("--table \"Keep,Me\"", doc, StringComparison.Ordinal);
        Assert.Contains("Option<string>(\"--output\", \"-o\", \"--output-dir\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--namespace\", \"-n\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--context\", \"-c\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--project\", \"-p\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--startup-project\", \"-s\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--msbuildprojectextensionspath\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-build\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--json\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--verbose\", \"-v\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-color\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--prefix-output\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("TreatUnmatchedTokensAsErrors = false", cliSource, StringComparison.Ordinal);
        Assert.Contains("ValidateScaffoldUnmatchedTokens", cliSource, StringComparison.Ordinal);
        Assert.Contains("AreEfPassThroughTokens", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetScaffoldPassThroughEnvironment", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetEfPassThroughTokens", cliSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeEfStyleCommandArgs", cliSource, StringComparison.Ordinal);
        Assert.Contains("new Command(\"dbcontext\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("EF-style DbContext command aliases", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsHelpToken", cliSource, StringComparison.Ordinal);
        Assert.Contains("WriteScaffoldResultJson", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadScaffoldWarningSummary", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldConnectionString", cliSource, StringComparison.Ordinal);
        Assert.Contains("TryResolveScaffoldNamedConnection", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadProjectUserSecretsId", cliSource, StringComparison.Ordinal);
        Assert.Contains("BuildScaffoldConfigurationSources", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetUserSecretsFilePath", cliSource, StringComparison.Ordinal);
        Assert.Contains("appsettings.{environment}.json", cliSource, StringComparison.Ordinal);
        Assert.Contains("ASPNETCORE_ENVIRONMENT", cliSource, StringComparison.Ordinal);
        Assert.Contains("DOTNET_ENVIRONMENT", cliSource, StringComparison.Ordinal);
        Assert.Contains("LoadEfToolConfig", cliSource, StringComparison.Ordinal);
        Assert.Contains("FindEfToolConfigPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadEfToolConfigBool", cliSource, StringComparison.Ordinal);
        Assert.Contains("must be a boolean", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveEfToolConfigPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldProject", cliSource, StringComparison.Ordinal);
        Assert.Contains("TryResolveCurrentDirectoryScaffoldProject", cliSource, StringComparison.Ordinal);
        Assert.Contains("CreateScaffoldProjectInfo", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldOutputPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ValidateScaffoldNamespaceName", cliSource, StringComparison.Ordinal);
        Assert.Contains("ValidateScaffoldContextClassName", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsValidScaffoldIdentifier", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsValidScaffoldNamespaceName", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldContextNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldContextOutputDirectory", cliSource, StringComparison.Ordinal);
        Assert.Contains("Scaffold --context-dir must be a relative path", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldContextNameAndNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("SplitScaffoldContextName", cliSource, StringComparison.Ordinal);
        Assert.Contains("InferScaffoldContextName", cliSource, StringComparison.Ordinal);
        Assert.Contains("TryGetScaffoldDatabaseName", cliSource, StringComparison.Ordinal);
        Assert.Contains("AppendNamespacePath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadProjectDefaultNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadProjectUseNullableReferenceTypes", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadNearestDirectoryBuildPropsProperty", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsNullableReferenceTypesEnabled", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--schemas\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string[]>(\"--schema\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string[]>(\"--table\", \"-t\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("schemaOpt.AllowMultipleArgumentsPerToken = true", cliSource, StringComparison.Ordinal);
        Assert.Contains("tableOpt.AllowMultipleArgumentsPerToken = true", cliSource, StringComparison.Ordinal);
        Assert.Contains("Argument<string?>(\"connection\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Argument<string?>(\"provider\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetOptionalNonBlankScaffoldOption", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetRequiredNonBlankScaffoldOption", cliSource, StringComparison.Ordinal);
        Assert.Contains("Scaffold {optionName} must not be blank", cliSource, StringComparison.Ordinal);
        Assert.Contains("providerPosition = connectionPosition;", cliSource, StringComparison.Ordinal);
        Assert.Contains("FirstNonBlank(connectionOption, connectionPosition)", cliSource, StringComparison.Ordinal);
        Assert.Contains("FirstNonBlank(providerOption, providerPosition)", cliSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeProviderName(prov)", cliSource, StringComparison.Ordinal);
        Assert.Contains("\"microsoft.entityframeworkcore.sqlite\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("\"npgsql.entityframeworkcore.postgresql\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("\"pomelo.entityframeworkcore.mysql\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-pluralize\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--use-database-names\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("sequence, routine, column, and routine result-column names as generated CLR names", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-onconfiguring\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--data-annotations\", \"-d\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--force\", \"-f\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("By default, scaffold output conflicts are refused", cliSource, StringComparison.Ordinal);
        Assert.Contains("OverwriteFiles = forceOverwrite", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--context-dir\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--context-namespace\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("PluralizeQueryProperties = !result.GetValue(noPluralizeOpt)", cliSource, StringComparison.Ordinal);
        Assert.Contains("UseDatabaseNames = result.GetValue(useDatabaseNamesOpt)", cliSource, StringComparison.Ordinal);
        Assert.Contains("UseNullableReferenceTypes = projectInfo?.UseNullableReferenceTypes ?? true", cliSource, StringComparison.Ordinal);
        Assert.Contains("options.UseDatabaseNames", scaffolderSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldRoutineStubWriter.AppendRoutineStubs(sb, context.RoutineStubs, queryPropertyNames, context.UseNullableReferenceTypes, context.UseDatabaseNames)", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("AppendSequenceStubs(sb, context.SequenceStubs, queryPropertyNames, context.UseDatabaseNames)", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("ContextOutputDirectory = contextOutputDirectory", cliSource, StringComparison.Ordinal);
        Assert.Contains("ContextNamespace = contextNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseSchemaFilters", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseTableFilters", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseCliCsvList", cliSource, StringComparison.Ordinal);
        Assert.Contains("Use repeatable `--table`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("routine result-column names", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Blank CLI table/schema filters are rejected", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`schema.view` filters", cliReadme, StringComparison.Ordinal);
        Assert.Contains("repeatable `--schema`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("multi-value `--table First Second`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("multi-value", rootReadme, StringComparison.Ordinal);
        Assert.Contains("routine result-column names", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Blank CLI table/schema filters are rejected", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--project`/`-p`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("plus sanitized output directory", cliReadme, StringComparison.Ordinal);
        Assert.Contains("nullable-reference output", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`#nullable disable`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("EF-style placement", cliReadme, StringComparison.Ordinal);
        Assert.Contains("namespace-qualified names such as", cliReadme, StringComparison.Ordinal);
        Assert.Contains("When `--context` is omitted", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--startup-project`/`-s`, `--framework`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("application arguments after `--`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`-- --environment Production`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`prefixOutput`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("matching supported query artifacts", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--json` emits a machine-readable", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--verbose`/`-v`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--no-pluralize`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--use-database-names`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--no-onconfiguring`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--output-dir`/`-o`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--data-annotations`/`-d`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Unfiltered ordinary views", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--force`/`-f`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("refuses output conflicts by default", cliReadme, StringComparison.Ordinal);
        Assert.Contains("norm scaffold <connection> <provider>", cliReadme, StringComparison.Ordinal);
        Assert.Contains("norm dbcontext scaffold <connection> <provider>", cliReadme, StringComparison.Ordinal);
        Assert.Contains("advertises `dbcontext` as an EF-style alias group", cliReadme, StringComparison.Ordinal);
        Assert.Contains("positional value after `--connection`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Microsoft.EntityFrameworkCore.Sqlite", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Npgsql.EntityFrameworkCore.PostgreSQL", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Name=ConnectionStrings:AppDb", cliReadme, StringComparison.Ordinal);
        Assert.Contains("UserSecretsId", cliReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project and target-project user secrets", cliReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project", cliReadme, StringComparison.Ordinal);
        Assert.Contains("environment files searched before target-project environment files", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--context-dir`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--context-namespace`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("norm scaffold <connection> <provider>", rootReadme, StringComparison.Ordinal);
        Assert.Contains("norm dbcontext scaffold <connection> <provider>", rootReadme, StringComparison.Ordinal);
        Assert.Contains("advertises `dbcontext` as an EF-style alias group", rootReadme, StringComparison.Ordinal);
        Assert.Contains("refuses existing output conflicts by default", rootReadme, StringComparison.Ordinal);
        Assert.Contains("positional value after `--connection`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Microsoft.EntityFrameworkCore.Sqlite", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Npgsql.EntityFrameworkCore.PostgreSQL", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Name=ConnectionStrings:AppDb", rootReadme, StringComparison.Ordinal);
        Assert.Contains("UserSecretsId", rootReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project and target-project user secrets", rootReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project", rootReadme, StringComparison.Ordinal);
        Assert.Contains("environment files searched before target-project environment files", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--project`/`-p` targets", rootReadme, StringComparison.Ordinal);
        Assert.Contains("plus sanitized", rootReadme, StringComparison.Ordinal);
        Assert.Contains("project-aware nullable output", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`Directory.Build.props`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("project-relative paths", rootReadme, StringComparison.Ordinal);
        Assert.Contains("MyApp.Data.AppDbContext", rootReadme, StringComparison.Ordinal);
        Assert.Contains("When `--context` is omitted", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--startup-project`/`-s`, `--framework`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("application arguments after `--`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`-- --environment Production`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`prefixOutput`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("matching supported query artifacts", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Unfiltered ordinary views", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--json` emits a machine-readable", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--verbose`/`-v`", rootReadme, StringComparison.Ordinal);
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
        Assert.Contains("ScaffoldOptions.PluralizeQueryProperties", optionsDoc, StringComparison.Ordinal);
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
