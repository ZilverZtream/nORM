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
public partial class ScaffoldingContractDocTests
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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableDiagnosticBuilder.Composite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableDiagnosticBuilder.PossibleJoins.cs"),
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

    private static string ReadForeignKeyDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.Sql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.Helpers.cs"));

    private static string ReadManyToManyDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldManyToManyJoinDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldManyToManyJoinDiscovery.Shape.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldManyToManyJoinDiscovery.Principals.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldManyToManyNavigationNameBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyShape.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldJoinTableShape.cs"));

    private static string ReadProviderSpecificTypeClassifierSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderSpecificTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Arrays.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Domains.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Enums.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerAliasTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlStringLiteralParser.cs"));

    private static string ReadSqlMetadataParserSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.Expressions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.Types.cs"));

    private static string ReadSemicolonParserSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSemicolonParser.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSemicolonParser.Keys.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSemicolonParser.Markers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSemicolonParser.Routines.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSemicolonParser.Selection.cs"));

    private static string ReadColumnDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Nullability.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Identity.cs"));

    private static string ReadPostgresUnsupportedFeatureSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.TableFeatures.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresUnsupportedFeatureDiscovery.Enums.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresProviderSpecificColumnFeatureDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresProviderSpecificIndexFeatureDiscovery.cs"));

    private static string ReadSqlServerUnsupportedFeatureSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.Sql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.CheckConstraints.cs"));

    private static string ReadMySqlUnsupportedFeatureSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlUnsupportedFeatureDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlUnsupportedFeatureDiscovery.Sql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlUnsupportedFeatureDiscovery.ExpressionIndexes.cs"));

    private static string ReadSqliteUnsupportedFeatureSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteUnsupportedFeatureDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteUnsupportedFeatureDiscovery.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteUnsupportedFeatureDiscovery.Triggers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteUnsupportedFeatureDiscovery.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteUnsupportedFeatureDiscovery.Helpers.cs"));

    private static string ReadIndexDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.Sql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.Helpers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteIndexDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexNameNormalizer.cs"));

    private static string ReadKeyDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.Fallback.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.Sql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.Helpers.cs"));

    private static string ReadSkippedObjectDiscoverySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqliteSkippedObjectDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerSkippedObjectDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerSkippedObjectDiscovery.Sql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresSkippedObjectDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlSkippedObjectDiscovery.cs"));

    private static string ReadScaffoldProviderDispatchSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Discovery.Selection.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Identity.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Nullability.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnPropertyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCommentDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSchemaDiscoveryAdapter.UnsupportedFeatures.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectQuery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Requests.cs"));

    private static string ReadUnsupportedFeatureMetadataSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.ProviderObjects.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.Relationships.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexFeatureMetadataBuilder.cs"));

    private static string ReadFeatureConfigurationBuilderSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.GeneratedDiagnostics.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.GeneratedDiagnostics.Checks.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.GeneratedDiagnostics.GeneratedColumns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.GeneratedDiagnostics.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.GeneratedDiagnostics.Removal.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationBuilder.GeneratedDiagnostics.Scalar.cs"));

    private static string ReadDiagnosticsWriterSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.Categories.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.Codes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.SuggestedActions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.Markdown.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsWriter.Json.cs"));

    private static string ReadEntityWriterSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityWriter.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityWriter.Formatting.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityWriter.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityWriter.Navigations.cs"));

    private static string ReadRoutineStubWriterSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.GeneratedMembers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.Documentation.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.Functions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.Guards.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.Metadata.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.Types.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFunctionRoutineStubWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFunctionRoutineStubWriter.Buffered.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFunctionRoutineStubWriter.Scalar.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFunctionRoutineStubWriter.Streaming.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoredProcedureRoutineStubWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoredProcedureOutputRoutineStubWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoredProcedureOutputRoutineStubWriter.Parameters.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoredProcedureOutputRoutineStubWriter.Scaffolded.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineTypeMapper.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineTypeMapper.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineTypeMapper.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineTypeMapper.Results.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineTypeMapper.Types.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataReader.Parameters.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataReader.Results.cs"));

    private static string ReadContextWriterSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.Shell.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.ModelConfiguration.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.ModelConfiguration.Constraints.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.ModelConfiguration.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.ModelConfiguration.Keys.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.ModelConfiguration.Properties.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.ManyToMany.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextWriter.Relationships.cs"));

    private static string ReadLiveProviderScaffoldCliParitySource()
        => string.Concat(
            ReadRepoFile("tests", "LiveProviderScaffoldCliParityTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliCurrentDirectoryConfigurationTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliNamedConnectionConfigurationTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliEnvironmentConfigurationTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliStartupConfigurationTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliManyToManyTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliProjectConfigurationTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliRelationshipTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliOutputFilterTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliDiagnosticsTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliWarningDiagnosticsTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliCoreShapeTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliRoutineSequenceTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliQueryArtifactTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliReferentialActionTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliCompatibilityTests.cs"));

    private static string ReadLiveProviderScaffoldingParitySource()
        => string.Concat(
            ReadRepoFile("tests", "LiveProviderScaffoldingParityTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingManyToManyTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingRelationshipTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderTypeTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingIndexTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingRoutineTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingRoutineOutputTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderObjectTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingQueryArtifactTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingDynamicTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingDiagnosticsTests.cs"));

    private static string ReadDatabaseScaffolderSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Models.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Names.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.TypeMapping.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Context.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Discovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Discovery.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Discovery.Selection.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Diagnostics.JoinTables.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Diagnostics.Metadata.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Diagnostics.Reports.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Diagnostics.UnsupportedFeatures.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.FeatureDiagnostics.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Relationships.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.FeatureConfigurations.Checks.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.FeatureConfigurations.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.FeatureConfigurations.Maps.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.FeatureConfigurations.Scalar.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.FeatureConfigurations.SqlMetadata.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.EntityFiles.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.SchemaMaps.cs"));

    private static string ReadStaticEntityScaffoldSource()
        => string.Concat(
            ReadDatabaseScaffolderSource(),
            ReadEntityWriterSource(),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceInfo.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelMetadataDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelMetadataDiscovery.Filters.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelMetadataDiscovery.Result.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelMetadataDiscovery.UnsupportedFeatures.cs"),
            ReadKeyDiscoverySource(),
            ReadForeignKeyDiscoverySource(),
            ReadIndexDiscoverySource(),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelCompositionBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldObjectSelectionBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Requests.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.QueryArtifacts.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityNameBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnPropertyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPrimaryKeyConfigurationBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticReportBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationAdapter.Scalar.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationAdapter.Relational.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationAdapter.Maps.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldFeatureConfigurationAdapter.Conversions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextInfoFactory.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextInfoFactory.Features.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextInfoFactory.ProviderObjects.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldContextInfoFactory.Relationships.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSchemaDiscoveryAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSchemaDiscoveryAdapter.Conversions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSchemaDiscoveryAdapter.UnsupportedFeatures.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSchemaDiscoveryAdapter.Helpers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedDiagnosticAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureDiscoveryReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipAdapter.Conversions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipAdapter.Keys.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityFileAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityFileAdapter.Conversions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntityFileAdapter.ReadOnly.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsAdapter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsAdapter.JoinTables.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDiagnosticsAdapter.Reports.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDataReaderHelper.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyShape.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRelationshipDiagnosticBuilder.cs"));

    private static string ReadDynamicEntitySource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.Generation.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.Models.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.Names.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.ReadOnly.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.Schema.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.SchemaSignature.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeGenerator.SqlParsing.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaDescriptorBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Metadata.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Names.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.PropertyAttributes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.Properties.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Keys.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataQuery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityConnectionKind.cs"),
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
        var scaffolder = ReadDatabaseScaffolderSource();
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
        var postgresUnsupportedSource = ReadPostgresUnsupportedFeatureSource();
        var sqlServerUnsupportedSource = ReadSqlServerUnsupportedFeatureSource();
        var mySqlUnsupportedSource = ReadMySqlUnsupportedFeatureSource();
        var skippedDiscoverySource = ReadSkippedObjectDiscoverySource();
        var skippedMetadataSource = string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Events.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.QueryObjects.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Sequences.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Synonyms.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Values.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataBuilder.Parameters.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataBuilder.Results.cs"));
        var columnDiscoverySource = ReadColumnDiscoverySource();
        var diagnosticsWriterSource = ReadDiagnosticsWriterSource();
        var parserSource = ReadSemicolonParserSource();
        var sqlMetadataParserSource = ReadSqlMetadataParserSource();
        var unsupportedMetadataSource = ReadUnsupportedFeatureMetadataSource();
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
        Assert.Contains("HasExactUniqueColumnSet(indexes, tableKey, foreignKeyColumnSet)", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("HasExactOrderedUniqueIndex(indexes, principalKey, principalColumns)", joinDiagnosticBuilderSource, StringComparison.Ordinal);
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
    public void Source_pins_scaffold_provider_dispatch_helper()
    {
        var helper = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderKind.cs");
        var dispatchSource = ReadScaffoldProviderDispatchSource();

        Assert.Contains("internal static class ScaffoldProviderKind", helper, StringComparison.Ordinal);
        Assert.Contains("IsSqlite(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("IsSqlServer(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("IsPostgres(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("IsMySql(DatabaseProvider provider)", helper, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsSqlite(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsSqlServer(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsPostgres(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldProviderKind.IsMySql(provider)", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("provider.GetType().Name", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("providerName", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("provider is SqliteProvider", dispatchSource, StringComparison.Ordinal);
        Assert.DoesNotContain("IsMySqlProvider", dispatchSource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_provider_specific_index_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadStaticEntityScaffoldSource();
        var diagnosticsWriterSource = ReadDiagnosticsWriterSource();
        var featureConfigurationBuilderSource = ReadFeatureConfigurationBuilderSource();
        var expressionIndexConfigurationBuilderSource = ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldExpressionIndexConfigurationBuilder.cs");
        var indexDiscoverySource = ReadIndexDiscoverySource();
        var postgresUnsupportedSource = ReadPostgresUnsupportedFeatureSource();
        var mySqlUnsupportedSource = ReadMySqlUnsupportedFeatureSource();
        var sqliteUnsupportedSource = ReadSqliteUnsupportedFeatureSource();
        var unsupportedMetadataSource = ReadUnsupportedFeatureMetadataSource();

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
        Assert.Contains("Ordinary SQL Server/PostgreSQL/SQLite filtered and partial column indexes are emitted with IndexAttribute.FilterSql", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("supported expression-index predicates are emitted with HasExpressionIndex filter metadata", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("SQLite, ordinary PostgreSQL B-tree, and MySQL expression indexes are emitted with HasExpressionIndex", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("including supported filters, included columns, PostgreSQL null ordering, and NULLS NOT DISTINCT uniqueness", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits only key-column index metadata", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits only provider-neutral column indexes", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits portable B-tree/rowstore column-index metadata only", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("non-default FK referential actions", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("ExtractCreateIndexWhereClause(feature.Detail)", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("IsCreateIndexUnique(feature.Detail)", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("FindCreateIndexKeyListOpen", source, StringComparison.Ordinal);
        Assert.Contains("FindSqlKeywordOutsideQuotes", source, StringComparison.Ordinal);
        Assert.Contains("GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("index_xinfo", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("\"PartialIndex\"", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("\"ExpressionIndex\"", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("\"DescendingIndex\"", sqliteUnsupportedSource, StringComparison.Ordinal);
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
        var builderSource = ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.cs");
        var configurationSource = ReadRepoFile("src", "nORM", "Configuration", "IEntityTypeConfiguration.cs");
        var snapshotSource = ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.cs");

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
        Assert.Contains("SQL Server and PostgreSQL primary-key constraint names", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server system-generated names", doc, StringComparison.Ordinal);
        Assert.Contains("ConstraintName", source, StringComparison.Ordinal);
        Assert.Contains("fk.is_system_named AS IsSyntheticConstraintName", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("dep.relname || '_'", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("CHAR_LENGTH(kcu.table_name) + 6", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("REGEXP '^[0-9]+$'", foreignKeyDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("foreignKey.IsSyntheticConstraintName ? null", relationshipDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("MarkSystemNamedCheckConstraintFeaturesAsync", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MarkDefaultNamedCheckConstraintFeaturesAsync", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MarkDefaultNamedCheckConstraintFeaturesAsync", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldSyntheticFeatureNameMarker", syntheticFeatureNameMarkerSource, StringComparison.Ordinal);
        Assert.Contains("isSyntheticName", syntheticFeatureNameMarkerSource, StringComparison.Ordinal);
        Assert.Contains("BuildGeneratedCheckConstraintName", checkFeatureConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeSyntheticIndexNames", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("RequiresExplicitManyToManyReferentialActions", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", builderSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", configurationSource, StringComparison.Ordinal);
        Assert.Contains("LeftOnDelete", snapshotSource, StringComparison.Ordinal);
        Assert.Contains("sqlite_autoindex_", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("kc.unique_index_id = i.index_id", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("unique_constraint.conname", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("FIRST_VALUE(s.column_name)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("_UNIQUE", indexDiscoverySource, StringComparison.Ordinal);
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
