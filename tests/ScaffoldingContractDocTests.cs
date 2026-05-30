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
        Assert.Contains("rowversion/timestamp", doc, StringComparison.Ordinal);
        Assert.Contains("identity seed/increment", doc, StringComparison.Ordinal);
        Assert.Contains("unrecognized FK referential actions", doc, StringComparison.Ordinal);
        Assert.Contains("relationships that do not target the generated principal primary key or an", doc, StringComparison.Ordinal);
        Assert.Contains("exact ordered unique index", doc, StringComparison.Ordinal);
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
        Assert.Contains("ReferentialAction", source, StringComparison.Ordinal);
        Assert.Contains("RelationshipPrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("'Routine'", source, StringComparison.Ordinal);
        Assert.Contains("outputParameters", source, StringComparison.Ordinal);
        Assert.Contains("parameterModes", source, StringComparison.Ordinal);
        Assert.Contains("WITHIN GROUP (ORDER BY pa.parameter_id)", source, StringComparison.Ordinal);
        Assert.Contains("ty.name", source, StringComparison.Ordinal);
        Assert.Contains("base_ty.name", source, StringComparison.Ordinal);
        Assert.Contains("p.data_type", source, StringComparison.Ordinal);
        Assert.Contains("domain_name", source, StringComparison.Ordinal);
        Assert.Contains("domain_schema", source, StringComparison.Ordinal);
        Assert.Contains("BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineParameters", source, StringComparison.Ordinal);
        Assert.Contains("information_schema.parameters", source, StringComparison.Ordinal);
        Assert.Contains("specific_name", source, StringComparison.Ordinal);
        Assert.Contains("column_type", source, StringComparison.Ordinal);
        Assert.Contains("'Sequence'", source, StringComparison.Ordinal);
        Assert.Contains("'VirtualTable'", source, StringComparison.Ordinal);
        Assert.Contains("'VirtualTableShadow'", source, StringComparison.Ordinal);
        Assert.Contains("'Synonym'", source, StringComparison.Ordinal);
        Assert.Contains("'MaterializedView'", source, StringComparison.Ordinal);
        Assert.Contains("'Event'", source, StringComparison.Ordinal);
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
        Assert.Contains("parameterCount", doc, StringComparison.Ordinal);
        Assert.Contains("outputParameterCount", doc, StringComparison.Ordinal);
        Assert.Contains("routineType", doc, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", doc, StringComparison.Ordinal);
        Assert.Contains("stale `nORM.ScaffoldWarnings.*` files", doc, StringComparison.Ordinal);
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        Assert.Contains("code =", source, StringComparison.Ordinal);
        Assert.Contains("severity =", source, StringComparison.Ordinal);
        Assert.Contains("category =", source, StringComparison.Ordinal);
        Assert.Contains("metadata = BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("reasons = g.Reasons", source, StringComparison.Ordinal);
        Assert.Contains("BuildPossibleJoinTableReasons", source, StringComparison.Ordinal);
        Assert.Contains("totalWarnings =", source, StringComparison.Ordinal);
        Assert.Contains("sectionCounts = new", source, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", source, StringComparison.Ordinal);
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
            "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@tenantId:IN:int,@total:OUT:decimal")!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;

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
            "SCF116", "SCF199",
            "SCF200", "SCF201", "SCF202", "SCF203", "SCF204", "SCF205", "SCF206", "SCF207",
            "SCF299"
        };

        foreach (var code in expectedCodes)
            Assert.Contains($"`{code}`", doc, StringComparison.Ordinal);

        Assert.Contains("SQLite declared `UUID`, `JSON`, and `XML`, SQL Server `xml`, PostgreSQL `json`/`jsonb`/`xml`/`uuid`, and MySQL `json`/`year` are scaffolded as supported scalar storage", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific declarations such as `GEOMETRY` remain diagnostics", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_provider_specific_index_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

        Assert.Contains("filtered/partial", doc, StringComparison.Ordinal);
        Assert.Contains("expression", doc, StringComparison.Ordinal);
        Assert.Contains("included-column", doc, StringComparison.Ordinal);
        Assert.Contains("descending", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PartialIndex", source, StringComparison.Ordinal);
        Assert.Contains("ExpressionIndex", source, StringComparison.Ordinal);
        Assert.Contains("IncludedColumnIndex", source, StringComparison.Ordinal);
        Assert.Contains("DescendingIndex", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_dynamic_rowversion_metadata()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.cs");

        Assert.Contains("computed/identity/rowversion metadata", doc, StringComparison.Ordinal);
        Assert.Contains("non-null reference-column `[Required]` parity", doc, StringComparison.Ordinal);
        Assert.Contains("GetComputedColumns", source, StringComparison.Ordinal);
        Assert.Contains("sys.computed_columns", source, StringComparison.Ordinal);
        Assert.Contains("is_generated <> 'NEVER'", source, StringComparison.Ordinal);
        Assert.Contains("generation_expression IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("IsRowVersion", source, StringComparison.Ordinal);
        Assert.Contains("GetRowVersionColumns", source, StringComparison.Ordinal);
        Assert.Contains("TimestampAttribute", source, StringComparison.Ordinal);
        Assert.Contains("RequiredAttribute", source, StringComparison.Ordinal);
        Assert.Contains("NormalizeScaffoldClrType", source, StringComparison.Ordinal);
        Assert.Contains("'timestamp', 'rowversion'", source, StringComparison.Ordinal);
        Assert.Contains("RV", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_pins_inverse_many_to_many_scaffolding()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

        Assert.Contains("Both entity sides receive collection navigations", doc, StringComparison.Ordinal);
        Assert.Contains("non-null foreign-key constraints", doc, StringComparison.Ordinal);
        Assert.Contains("Single-column,", doc, StringComparison.Ordinal);
        Assert.Contains("alternate-key pure junction", doc, StringComparison.Ordinal);
        Assert.Contains("key-selector `UsingTable` overload", doc, StringComparison.Ordinal);
        Assert.Contains("WithMany(inverse)", doc, StringComparison.Ordinal);
        Assert.Contains("schema-aware `UsingTable`", doc, StringComparison.Ordinal);
        Assert.Contains("Self-referencing pure join tables receive distinct", doc, StringComparison.Ordinal);
        Assert.Contains("navigations from the join FK columns", doc, StringComparison.Ordinal);
        Assert.Contains("JoinTableSchema", source, StringComparison.Ordinal);
        Assert.Contains(".WithMany(p => p.", source, StringComparison.Ordinal);
        Assert.Contains("isSelfJoin", source, StringComparison.Ordinal);
        Assert.Contains("leftCollectionBase += \"By\"", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_deterministic_scaffold_output()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

        Assert.Contains("generated output are ordered deterministically", doc, StringComparison.Ordinal);
        Assert.Contains("Relationship navigations and fluent relationship", doc, StringComparison.Ordinal);
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
        Assert.Contains("non-null reference-column `[Required]` parity", doc, StringComparison.Ordinal);
        Assert.Contains("Self-referencing pure join tables", doc, StringComparison.Ordinal);

        Assert.Contains("Scaffold_sqlite_output_builds_as_consumer_project", releaseGates, StringComparison.Ordinal);
        Assert.Contains("Scaffold_clean_run_removes_stale_warning_reports_without_printing_summary", releaseGates, StringComparison.Ordinal);
        Assert.Contains("LiveProviderScaffoldingParityTests", releaseGates, StringComparison.Ordinal);
        Assert.Contains("already-built `nORM.dll`", releaseGates, StringComparison.Ordinal);

        Assert.Contains("NormalizeScaffoldClrType", source, StringComparison.Ordinal);
        Assert.Contains("effectiveAllowNull", source, StringComparison.Ordinal);
        Assert.Contains("isSelfRelationship", source, StringComparison.Ordinal);
        Assert.Contains("isSelfJoin", source, StringComparison.Ordinal);
        Assert.Contains("LeftCollectionNavigationName", source, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("NormalizeScaffoldClrType", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("effectiveAllowNull", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("RequiredAttribute", dynamicSource, StringComparison.Ordinal);
        Assert.Contains("BuildSchemaDescriptor", dynamicSource, StringComparison.Ordinal);
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
