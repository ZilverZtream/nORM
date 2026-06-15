using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public partial class ScaffoldingContractDocTests
{
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
        var skippedDiscoverySource = ReadSkippedObjectDiscoverySource();
        var diagnosticsWriterSource = ReadDiagnosticsWriterSource();
        var routineWriterSource = ReadRoutineStubWriterSource();
        var unsupportedMetadataSource = ReadUnsupportedFeatureMetadataSource();
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
        Assert.Contains("foreignKeyUniqueIndexCandidates", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("totalWarnings =", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("sectionCounts = new", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("EnsureNoStaleScaffoldWarningReports", source, StringComparison.Ordinal);
        Assert.Contains("suggestedAction", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("RequireScaffoldedRoutineParameters", routineWriterSource, StringComparison.Ordinal);
        Assert.Contains("QueryUnchangedStreamAsync", routineWriterSource, StringComparison.Ordinal);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync", routineWriterSource, StringComparison.Ordinal);
        Assert.Contains("OutputParameter[]", routineWriterSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Referential_action_diagnostic_metadata_includes_relationship_shape()
    {
        var features = new List<ScaffoldUnsupportedFeature>();
        var foreignKeys = new[]
        {
            new ScaffoldForeignKey(
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
                false),
            new ScaffoldForeignKey(
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
                false)
        };

        ScaffoldUnsupportedDiagnosticAdapter.AddReferentialActionDiagnostics(features, foreignKeys);

        var feature = Assert.Single(features);
        var metadata = BuildUnsupportedFeatureMetadata(
            feature.TableKey,
            feature.Kind,
            feature.Name,
            feature.Detail,
            feature.Metadata);

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
            "SCF100", "SCF101", "SCF102", "SCF103", "SCF104", "SCF105", "SCF106", "SCF107",
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
    public void Provider_emitted_unsupported_feature_kinds_have_stable_diagnostics()
    {
        var source = string.Concat(
            ReadSqliteUnsupportedFeatureSource(),
            ReadSqlServerUnsupportedFeatureSource(),
            ReadPostgresUnsupportedFeatureSource(),
            ReadMySqlUnsupportedFeatureSource(),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipDiagnosticBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedDiagnosticAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedDiagnosticAdapter.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedDiagnosticAdapter.Keys.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedDiagnosticAdapter.ReferentialActions.cs"));
        var codeSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.Codes.cs");
        var actionSource = ReadSuggestedActionSource();

        var emittedKinds = Regex.Matches(
                source,
                "new\\s+ScaffoldUnsupportedFeatureInfo\\([^;]*?\"(?<kind>[A-Za-z][A-Za-z0-9]*)\"",
                RegexOptions.Singleline)
            .Select(match => match.Groups["kind"].Value)
            .Concat(Regex.Matches(
                    source,
                    "'(?<kind>[A-Za-z][A-Za-z0-9]*)'\\s+(?:AS\\s+)?Kind\\b",
                    RegexOptions.IgnoreCase)
                .Select(match => match.Groups["kind"].Value))
            .Where(kind => !string.Equals(kind, "Kind", StringComparison.OrdinalIgnoreCase))
            .Where(kind => kind is not ("View" or "Routine" or "Sequence" or "Synonym" or "MaterializedView" or "Event" or "VirtualTable" or "VirtualTableShadow"))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(kind => kind, StringComparer.Ordinal)
            .ToArray();

        Assert.Contains("PrecisionScale", emittedKinds);
        foreach (var kind in emittedKinds)
        {
            Assert.Contains($"\"{kind}\" => \"SCF", codeSource, StringComparison.Ordinal);
            Assert.DoesNotContain($"\"{kind}\" => \"SCF199\"", codeSource, StringComparison.Ordinal);
            Assert.Contains($"\"{kind}\" =>", actionSource, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Provider_emitted_skipped_object_kinds_have_stable_diagnostics()
    {
        var source = ReadSkippedObjectDiscoverySource();
        var codeSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.Codes.cs");
        var actionSource = ReadSuggestedActionSource();
        var emittedKinds = source
            .Split(new[] { "\r\n", "\n" }, StringSplitOptions.None)
            .SelectMany(line => new[]
            {
                Regex.Match(line, "'(?<kind>[A-Za-z][A-Za-z0-9]*)'\\s+AS\\s+Kind", RegexOptions.IgnoreCase),
                Regex.Match(line, "SELECT\\b.*?,\\s*.*?,\\s*'(?<kind>[A-Za-z][A-Za-z0-9]*)'\\s*,", RegexOptions.IgnoreCase)
            })
            .Where(match => match.Success)
            .Select(match => match.Groups["kind"].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(kind => kind, StringComparer.Ordinal)
            .ToArray();

        Assert.Contains("View", emittedKinds);
        Assert.Contains("Routine", emittedKinds);
        Assert.Contains("VirtualTable", emittedKinds);
        foreach (var kind in emittedKinds)
        {
            Assert.Contains($"\"{kind}\" => \"SCF", codeSource, StringComparison.Ordinal);
            Assert.DoesNotContain($"\"{kind}\" => \"SCF299\"", codeSource, StringComparison.Ordinal);
            Assert.Contains($"\"{kind}\" =>", actionSource, StringComparison.Ordinal);
        }
    }
}
