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
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
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
        Assert.Contains("exact ordered unfiltered non-null unique index", doc, StringComparison.Ordinal);
        Assert.Contains("HasNonNullableColumns", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesScaffoldablePrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesUniqueIndex", source, StringComparison.Ordinal);
        Assert.Contains("GetIdentityColumnNamesAsync", source, StringComparison.Ordinal);
        Assert.Contains("auto_increment", source, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nextval(%", source, StringComparison.Ordinal);
        Assert.Contains("dependency.deptype IN ('a', 'i')", source, StringComparison.Ordinal);
        Assert.Contains("column_default NOT LIKE 'nextval(%'", source, StringComparison.Ordinal);
        Assert.Contains("temporal_type <> 0", source, StringComparison.Ordinal);
        Assert.Contains("skippedDatabaseObjects", source, StringComparison.Ordinal);
        Assert.Contains("MissingPrimaryKey", source, StringComparison.Ordinal);
        Assert.Contains("Collation", source, StringComparison.Ordinal);
        Assert.Contains("ProviderSpecificColumnType", source, StringComparison.Ordinal);
        Assert.Contains("PrecisionScale", source, StringComparison.Ordinal);
        Assert.Contains("RowVersion", source, StringComparison.Ordinal);
        Assert.Contains("IdentityStrategy", source, StringComparison.Ordinal);
        Assert.Contains("TryTrimTrailingComputedStorageToken", source, StringComparison.Ordinal);
        Assert.Contains("quote == '[' ? ']'", source, StringComparison.Ordinal);
        Assert.Contains("TryConsumeSqlKeyword(candidate, ref keywordIndex, \"CHECK\")", source, StringComparison.Ordinal);
        Assert.Contains("TryConsumeSqlKeyword(detail, ref keywordIndex, \"IDENTITY\")", source, StringComparison.Ordinal);
        Assert.Contains("cc.is_persisted", source, StringComparison.Ordinal);
        Assert.Contains("generation_expression || ' STORED'", source, StringComparison.Ordinal);
        Assert.Contains("stored generated", source, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("virtual generated", source, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ReferentialAction", source, StringComparison.Ordinal);
        Assert.Contains("RelationshipPrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("'Routine'", source, StringComparison.Ordinal);
        Assert.Contains("outputParameters", source, StringComparison.Ordinal);
        Assert.Contains("parameterModes", source, StringComparison.Ordinal);
        Assert.Contains("pa.is_output = 1 THEN 'OUT'", source, StringComparison.Ordinal);
        Assert.Contains("decimal precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("exclude provider metadata return rows from callable input counts", doc, StringComparison.Ordinal);
        Assert.Contains("p.parameter_mode IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("WITHIN GROUP (ORDER BY pa.parameter_id)", source, StringComparison.Ordinal);
        Assert.Contains("ty.name", source, StringComparison.Ordinal);
        Assert.Contains("base_ty.name", source, StringComparison.Ordinal);
        Assert.Contains("p.data_type", source, StringComparison.Ordinal);
        Assert.Contains("domain_name", source, StringComparison.Ordinal);
        Assert.Contains("domain_schema", source, StringComparison.Ordinal);
        Assert.Contains("column_type", source, StringComparison.Ordinal);
        Assert.Contains("unsigned", source, StringComparison.Ordinal);
        Assert.Contains("legacy display widths", doc, StringComparison.Ordinal);
        Assert.Contains("NormalizeMySqlUnsignedTypeDetail", source, StringComparison.Ordinal);
        Assert.Contains("BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildSequenceMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildSynonymMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildEventMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildQueryObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildUnsupportedFeatureMetadata", source, StringComparison.Ordinal);
        Assert.Contains("TryParseRelationshipPrincipalKeyDetail", source, StringComparison.Ordinal);
        Assert.Contains("TryParseReferentialActionDetail", source, StringComparison.Ordinal);
        Assert.Contains("InferSqliteVirtualTableShadowOwner", source, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineSemicolonValues", source, StringComparison.Ordinal);
        Assert.Contains("ParseSemicolonValues", source, StringComparison.Ordinal);
        Assert.Contains("SelectBestOrderedSemicolonValueMarkers", source, StringComparison.Ordinal);
        Assert.Contains("SelectOrderedSemicolonValueMarkers", source, StringComparison.Ordinal);
        Assert.Contains("GetRoutineSemicolonValueKeyOrders", source, StringComparison.Ordinal);
        Assert.Contains("IsKnownSemicolonValueKey", source, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineParameters", source, StringComparison.Ordinal);
        Assert.Contains("TryParseRoutineParameterMode", source, StringComparison.Ordinal);
        Assert.Contains("trimmed.Length - 1", source, StringComparison.Ordinal);
        Assert.Contains("SplitRoutineResultColumns", source, StringComparison.Ordinal);
        Assert.Contains("TryParseRoutineResultColumnParts", source, StringComparison.Ordinal);
        Assert.Contains("information_schema.parameters", source, StringComparison.Ordinal);
        Assert.Contains("specific_name", source, StringComparison.Ordinal);
        Assert.Contains("'Sequence'", source, StringComparison.Ordinal);
        Assert.Contains("'VirtualTable'", source, StringComparison.Ordinal);
        Assert.Contains("'VirtualTableShadow'", source, StringComparison.Ordinal);
        Assert.Contains("'Synonym'", source, StringComparison.Ordinal);
        Assert.Contains("'MaterializedView'", source, StringComparison.Ordinal);
        Assert.Contains("'Event'", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Source_pins_explicit_system_usings_for_generated_scaffold_artifacts()
    {
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        var entityStart = source.IndexOf("private static async Task<string> ScaffoldEntityAsync", StringComparison.Ordinal);
        var entityEnd = source.IndexOf("private static string BuildSchemaProbeSql", entityStart, StringComparison.Ordinal);
        var contextStart = source.IndexOf("private static string ScaffoldContextWithRelationships", StringComparison.Ordinal);

        Assert.True(entityStart >= 0);
        Assert.True(entityEnd > entityStart);
        Assert.True(contextStart >= 0);

        var entitySource = source[entityStart..entityEnd];
        var contextSource = source[contextStart..];
        Assert.Contains("sb.AppendLine(\"using System;\");", entitySource, StringComparison.Ordinal);
        Assert.Contains("routineStubs?.Count > 0", contextSource, StringComparison.Ordinal);
        Assert.Contains("sb.AppendLine(\"using System;\");", contextSource, StringComparison.Ordinal);
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
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        Assert.Contains("code =", source, StringComparison.Ordinal);
        Assert.Contains("severity =", source, StringComparison.Ordinal);
        Assert.Contains("category =", source, StringComparison.Ordinal);
        Assert.Contains("metadata = BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("providerObjectKind", source, StringComparison.Ordinal);
        Assert.Contains("proretset", source, StringComparison.Ordinal);
        Assert.Contains("queryArtifactTableKeys", source, StringComparison.Ordinal);
        Assert.Contains("reasons = g.Reasons", source, StringComparison.Ordinal);
        Assert.Contains("BuildPossibleJoinTableReasons", source, StringComparison.Ordinal);
        Assert.Contains("BuildCompositeForeignKeyMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildPossibleJoinTableMetadata", source, StringComparison.Ordinal);
        Assert.Contains("AddIndexFeatureMetadata", source, StringComparison.Ordinal);
        Assert.Contains("AddMetadataBooleanValue", source, StringComparison.Ordinal);
        Assert.Contains("ParsePrefixIndexColumns", source, StringComparison.Ordinal);
        Assert.Contains("Metadata { get; init; }", source, StringComparison.Ordinal);
        Assert.Contains("hasExactBridgePrimaryKey", source, StringComparison.Ordinal);
        Assert.Contains("hasGeneratedSurrogatePrimaryKey", source, StringComparison.Ordinal);
        Assert.Contains("hasExactForeignKeyUniqueIndex", source, StringComparison.Ordinal);
        Assert.Contains("totalWarnings =", source, StringComparison.Ordinal);
        Assert.Contains("sectionCounts = new", source, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", source, StringComparison.Ordinal);
        Assert.Contains("RequireScaffoldedRoutineParameters", source, StringComparison.Ordinal);
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
            "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@tenantId:IN:int,@total:OUT:decimal; resultColumns=Id:int:0|Name:nvarchar(40):1")!;
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
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant:id:IN:int; resultColumns=Order:Id:int:0|Line:Name:nvarchar(40):1")!;
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
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant:IN:name:IN:int; resultColumns=")!;
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
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant;id:IN:int; resultColumns=Order;Id:int:0|Line;Name:nvarchar(40):1")!;
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
            "SQL Server stored procedure; parameters=2; outputParameters=0; parameterModes=@tenant,id:IN:int,@search,text:IN:nvarchar(40); resultColumns=Order|Id:int:0|Line|Name:nvarchar(40):1")!;
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
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant; note=retained:IN:int; resultColumns=Order; note=retained:int:0")!;
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
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=tenant; dataType=retained:IN:integer; dataType=integer")!;
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
            "SQL Server sequence; dataType=bigint")!;
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
            "SQL Server synonym; baseObject=[dbo].[Orders; note=retained]; baseType=U")!;
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
            "MySQL event; eventType=RECURRING; status=ENABLED; intervalValue=1; intervalField=DAY; starts=2026-01-01 00:00:00")!;
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
            "PostgreSQL view")!;
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
            "SQLite virtual table shadow table")!;
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
        Assert.Equal("SET DEFAULT", referentialAction["onDelete"]);
        Assert.Equal("NO ACTION", referentialAction["onUpdate"]);

        var precisionScale = Metadata("PrecisionScale", "Amount", "decimal(18,2)");
        Assert.Equal(18, precisionScale["precision"]);
        Assert.Equal(2, precisionScale["scale"]);

        var computed = Metadata("Computed", "Total", "(Price * Quantity) STORED");
        Assert.Equal("Price * Quantity", computed["computedSql"]);
        Assert.Equal(true, computed["stored"]);

        Assert.Equal(true, Metadata("RowVersion", "Version", "rowversion")["concurrencyToken"]);
        Assert.Equal(true, Metadata("RowVersion", "Version", "rowversion")["databaseGenerated"]);

        var identity = Metadata("IdentityStrategy", "Id", "IDENTITY(100,10)");
        Assert.Equal(100L, identity["seed"]);
        Assert.Equal(10L, identity["increment"]);

        Assert.Equal("geometry", Metadata("ProviderSpecificColumnType", "Shape", "geometry")["providerType"]);
        Assert.Equal("Latin1_General_CI_AS", Metadata("Collation", "Name", "Latin1_General_CI_AS")["collation"]);
        Assert.Equal("now()", Metadata("Default", "CreatedAt", "now()")["defaultSql"]);
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

        Assert.Contains("SQLite declared `UUID`, `JSON`, and `XML`, SQL Server `xml`, PostgreSQL `json`/`jsonb`/`xml`/`uuid` plus safe scalar arrays/simple enums, and MySQL `json`/`year`/simple `enum(...)` plus bounded simple `set(...)` are scaffolded as supported storage", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific declarations such as `GEOMETRY` remain diagnostics", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_provider_specific_index_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

        Assert.Contains("filtered/partial", doc, StringComparison.Ordinal);
        Assert.Contains("filtered/partial predicates for the same", doc, StringComparison.Ordinal);
        Assert.Contains("expression", doc, StringComparison.Ordinal);
        Assert.Contains("included-column", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite attached-schema partial index predicates", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL prefix indexes", doc, StringComparison.Ordinal);
        Assert.Contains("descending", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PartialIndex", source, StringComparison.Ordinal);
        Assert.Contains("ExpressionIndex", source, StringComparison.Ordinal);
        Assert.Contains("IncludedColumnIndex", source, StringComparison.Ordinal);
        Assert.Contains("DescendingIndex", source, StringComparison.Ordinal);
        Assert.Contains("PrefixIndex", source, StringComparison.Ordinal);
        Assert.Contains("ProviderSpecificIndex", source, StringComparison.Ordinal);
        Assert.Contains("ExtractCreateIndexWhereClause(feature.Detail)", source, StringComparison.Ordinal);
        Assert.Contains("IsCreateIndexUnique(feature.Detail)", source, StringComparison.Ordinal);
        Assert.Contains("FindCreateIndexKeyListOpen", source, StringComparison.Ordinal);
        Assert.Contains("FindSqlKeywordOutsideQuotes", source, StringComparison.Ordinal);
        Assert.Contains("GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name)", source, StringComparison.Ordinal);
        Assert.Contains("i.type IN (1, 2)", source, StringComparison.Ordinal);
        Assert.Contains("am.amname = 'btree'", source, StringComparison.Ordinal);
        Assert.Contains("ix.indoption[key.ord - 1] & 1", source, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_get_indexdef(ix.indexrelid) ILIKE '% DESC%'", source, StringComparison.Ordinal);
        Assert.Contains("UPPER(COALESCE(NULLIF(s.index_type, ''), 'BTREE')) = 'BTREE'", source, StringComparison.Ordinal);
        Assert.Contains("bad.sub_part IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("bad.sub_part < bad_col.character_maximum_length", source, StringComparison.Ordinal);
        Assert.Contains("sub_part IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("SHOW INDEX FROM", source, StringComparison.Ordinal);
        Assert.Contains("ReaderHasColumn(reader, \"Expression\")", source, StringComparison.Ordinal);
        Assert.Contains("MySQL expression index", source, StringComparison.Ordinal);
        Assert.Contains("expressionIndexKeys", source, StringComparison.Ordinal);
        Assert.Contains("!string.IsNullOrWhiteSpace(index.FilterSql)", source, StringComparison.Ordinal);
        Assert.Contains("Mixed functional indexes are not partially emitted", doc, StringComparison.Ordinal);
        Assert.Contains("ordinary PostgreSQL B-tree expression indexes", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific access methods or non-default B-tree key options", doc, StringComparison.Ordinal);
        Assert.Contains("expression indexes with included columns or descending expression keys remain provider-owned diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("unrepresentableExpressionIndexes", source, StringComparison.Ordinal);
        Assert.Contains("pg_get_indexdef(ix.indexrelid)::text", source, StringComparison.Ordinal);
        Assert.Contains("provider-specific B-tree key options", doc, StringComparison.Ordinal);
        Assert.Contains("expression indexes with non-default B-tree key options", doc, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_attribute option_att", source, StringComparison.Ordinal);
        Assert.Contains("option_att.attnum IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("option_opclass.opcdefault = false", source, StringComparison.Ordinal);
        Assert.Contains("ix.indcollation[option_key.ord - 1] <> option_att.attcollation", source, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL B-tree indexes with non-default", doc, StringComparison.Ordinal);
        Assert.Contains("indnullsnotdistinct", source, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL `NULLS NOT DISTINCT` uniqueness remain provider-owned diagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("hasNullsNotDistinct", source, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultOperatorClass", source, StringComparison.Ordinal);
        Assert.Contains("hasIndexCollation", source, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultNullOrdering", source, StringComparison.Ordinal);
        Assert.Contains("unrepresentableExpressionIndexes.Contains", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_dynamic_rowversion_metadata()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.cs");

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
        Assert.Contains("'timestamp', 'rowversion'", source, StringComparison.Ordinal);
        Assert.Contains("RV", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_pins_inverse_many_to_many_scaffolding()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
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
        Assert.Contains(".WithMany(p => p.", source, StringComparison.Ordinal);
        Assert.Contains("isSelfJoin", source, StringComparison.Ordinal);
        Assert.Contains("leftCollectionBase += \"By\"", source, StringComparison.Ordinal);
        Assert.Contains("IF SCHEMA_ID", sqlServerMigration, StringComparison.Ordinal);
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS", postgresMigration, StringComparison.Ordinal);
        Assert.Contains("EscTableNameOnly(fk.PrincipalTable)", sqliteMigration, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_deterministic_scaffold_output()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

        Assert.Contains("generated output are ordered deterministically", doc, StringComparison.Ordinal);
        Assert.Contains("Relationship navigations and fluent relationship", doc, StringComparison.Ordinal);
        Assert.Contains("provider-reported foreign key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("ConstraintName", source, StringComparison.Ordinal);
        Assert.Contains("ThenBy(r => r.CollectionNavigationName", source, StringComparison.Ordinal);
        Assert.Contains("ThenBy(r => r.ReferenceNavigationName", source, StringComparison.Ordinal);
        Assert.Contains("ThenBy(j => j.LeftCollectionNavigationName", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_sqlite_rowid_key_normalization()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

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
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        var dynamicSource = ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.cs");

        Assert.Contains("Repeated scaffolds of the same schema", cliReadme, StringComparison.Ordinal);
        Assert.Contains("ordered deterministically", cliReadme, StringComparison.Ordinal);
        Assert.Contains("stale `nORM.ScaffoldWarnings.*`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("role-based self-referencing FK and self-join navigations", cliReadme, StringComparison.Ordinal);
        Assert.Contains("SQLite rowid integer primary keys are generated as non-null `long`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("stale warning reports", rootReadme, StringComparison.Ordinal);
        Assert.Contains("SQLite rowid integer primary keys are normalized to non-null `long`", doc, StringComparison.Ordinal);
        Assert.Contains("reference-column `[Required]` parity", doc, StringComparison.Ordinal);
        Assert.Contains("Self-referencing pure join tables", doc, StringComparison.Ordinal);

        Assert.Contains("Scaffold_sqlite_output_builds_as_consumer_project", releaseGates, StringComparison.Ordinal);
        Assert.Contains("Scaffold_clean_run_removes_stale_warning_reports_without_printing_summary", releaseGates, StringComparison.Ordinal);
        Assert.Contains("LiveProviderScaffoldingParityTests", releaseGates, StringComparison.Ordinal);
        Assert.Contains("LiveProviderScaffoldCliParityTests", releaseGates, StringComparison.Ordinal);
        Assert.Contains("already-built `nORM.dll`", releaseGates, StringComparison.Ordinal);

        Assert.Contains("NormalizeScaffoldClrType", source, StringComparison.Ordinal);
        Assert.Contains("effectiveAllowNull", source, StringComparison.Ordinal);
        Assert.Contains("isSelfRelationship", source, StringComparison.Ordinal);
        Assert.Contains("isSelfJoin", source, StringComparison.Ordinal);
        Assert.Contains("LeftCollectionNavigationName", source, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("NormalizeScaffoldClrType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("effectiveAllowNull", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("existingPropertyNames.Add(EscapeCSharpIdentifier(ToPascalCase(tableName)))", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetScaffoldMaxLength(normalizedClrType, row)", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldDecimalPrecision", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetDecimalPrecisions", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("QueryDecimalPrecisionMap", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("nameof(ColumnAttribute.TypeName)", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("column.DecimalPrecision", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("RequiredAttribute", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("BuildSchemaDescriptor", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetSqliteDeclaredColumnTypes", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("GetPostgresDomainColumnCastTypes", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeMySqlUnsignedTypeDetail", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("SQLite `UUID` declared-type parity", doc, StringComparison.Ordinal);
        Assert.Contains("keyless dynamic", doc, StringComparison.Ordinal);
        Assert.Contains("avoid the enclosing entity type name", doc, StringComparison.Ordinal);
        Assert.Contains("composite primary-key ordinal parity", doc, StringComparison.Ordinal);
        Assert.Contains("normalized dynamic `MaxLength` metadata", doc, StringComparison.Ordinal);
        Assert.Contains("dynamic decimal precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("Static and runtime dynamic scaffolding emit", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL domain-column schema probes", doc, StringComparison.Ordinal);
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
        var cliReadme = ReadRepoFile("src", "dotnet-norm", "README.md");

        Assert.Contains("repeatable CLI `--table`", doc, StringComparison.Ordinal);
        Assert.Contains("literal table names that contain commas", doc, StringComparison.Ordinal);
        Assert.Contains("--table \"Keep,Me\"", doc, StringComparison.Ordinal);
        Assert.Contains("Option<string[]>(\"--table\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseTableFilters", cliSource, StringComparison.Ordinal);
        Assert.Contains("Use repeatable `--table`", cliReadme, StringComparison.Ordinal);
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
        Assert.Contains("Null or blank filters are", optionsDoc, StringComparison.Ordinal);
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
