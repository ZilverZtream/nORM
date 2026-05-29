using System;
using System.IO;
using System.Linq;
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
        Assert.Contains("provider-specific collations", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific column types", doc, StringComparison.Ordinal);
        Assert.Contains("decimal precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("provider metadata-backed identity", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL identity/serial", doc, StringComparison.Ordinal);
        Assert.Contains("owned sequences are treated as", doc, StringComparison.Ordinal);
        Assert.Contains("rowversion/timestamp", doc, StringComparison.Ordinal);
        Assert.Contains("identity seed/increment", doc, StringComparison.Ordinal);
        Assert.Contains("non-default FK referential actions", doc, StringComparison.Ordinal);
        Assert.Contains("relationships that do not target the generated principal primary key", doc, StringComparison.Ordinal);
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
        Assert.Contains("suggestedAction", doc, StringComparison.Ordinal);
        Assert.Contains("stale `nORM.ScaffoldWarnings.*` files", doc, StringComparison.Ordinal);
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");
        Assert.Contains("code =", source, StringComparison.Ordinal);
        Assert.Contains("severity =", source, StringComparison.Ordinal);
        Assert.Contains("category =", source, StringComparison.Ordinal);
        Assert.Contains("totalWarnings =", source, StringComparison.Ordinal);
        Assert.Contains("sectionCounts = new", source, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", source, StringComparison.Ordinal);
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
        Assert.Contains("GetComputedColumns", source, StringComparison.Ordinal);
        Assert.Contains("sys.computed_columns", source, StringComparison.Ordinal);
        Assert.Contains("is_generated <> 'NEVER'", source, StringComparison.Ordinal);
        Assert.Contains("generation_expression IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("IsRowVersion", source, StringComparison.Ordinal);
        Assert.Contains("GetRowVersionColumns", source, StringComparison.Ordinal);
        Assert.Contains("TimestampAttribute", source, StringComparison.Ordinal);
        Assert.Contains("'timestamp', 'rowversion'", source, StringComparison.Ordinal);
        Assert.Contains("RV", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_pins_inverse_many_to_many_scaffolding()
    {
        var doc = ReadDoc();
        var source = ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs");

        Assert.Contains("Both entity sides receive collection navigations", doc, StringComparison.Ordinal);
        Assert.Contains("WithMany(inverse)", doc, StringComparison.Ordinal);
        Assert.Contains("schema-aware `UsingTable`", doc, StringComparison.Ordinal);
        Assert.Contains("JoinTableSchema", source, StringComparison.Ordinal);
        Assert.Contains(".WithMany(p => p.", source, StringComparison.Ordinal);
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
