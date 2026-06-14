using System;
using System.IO;
using System.Linq;
using Xunit;

namespace nORM.Tests;

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

    private static string ReadEntityTypeBuilderSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.cs"),
            ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.MappingConfiguration.cs"),
            ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.PropertyBuilders.cs"),
            ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.ReferenceBuilders.cs"),
            ReadRepoFile("src", "nORM", "Configuration", "EntityTypeBuilder.CollectionBuilders.cs"));

    private static string ReadSchemaSnapshotSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.cs"),
            ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.Builder.cs"),
            ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.Builder.ColumnFacets.cs"),
            ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.Diff.cs"),
            ReadRepoFile("src", "nORM", "Migration", "SchemaSnapshot.Differ.cs"));

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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderSpecificTypeClassifier.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderSpecificTypeClassifier.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldProviderSpecificTypeClassifier.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Arrays.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Domains.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.DomainCastTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Parameterized.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldPostgresTypeClassifier.Enums.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlTypeClassifier.QuotedValues.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldMySqlTypeClassifier.Unsigned.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerAliasTypeClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlStringLiteralParser.cs"));

    private static string ReadSqlMetadataParserSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.IndexScanning.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.Expressions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlMetadataParser.Scanning.cs"),
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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Nullability.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Identity.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Queries.cs"));

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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSqlServerUnsupportedFeatureDiscovery.DefaultConstraints.cs"),
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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldObjectSelectionBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Facets.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Identity.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.Nullability.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnPropertyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCommentDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCommentDiscovery.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCommentDiscovery.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCommentDiscovery.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldCommentDiscovery.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldForeignKeyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldIndexDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldKeyDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldModelDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSchemaDiscoveryAdapter.UnsupportedFeatures.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectQuery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectQuery.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectQuery.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectQuery.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableDiscovery.Fallback.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Requests.cs"));

    private static string ReadUnsupportedFeatureMetadataSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.Features.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldUnsupportedFeatureMetadataBuilder.Indexes.cs"),
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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineStubWriter.Plan.cs"),
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
            ReadRepoFile("tests", "LiveProviderScaffoldCliEfConfigDefaultsTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliRelationshipTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliOneToOneTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliOutputFilterTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliDiagnosticsTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliWarningDiagnosticsTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliCoreShapeTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliNamingTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliPluralizationTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliRoutineSequenceTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliQueryArtifactTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliReferentialActionTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldCliCompatibilityTests.cs"));

    private static string ReadLiveProviderScaffoldingParitySource()
        => string.Concat(
            ReadRepoFile("tests", "LiveProviderScaffoldingParityTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingManyToManyTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingReferentialActionTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingRelationshipTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingOneToOneTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderTypeTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderTypePostgresTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderTypeMySqlTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderTypeSqlServerTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingIndexTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingRoutineTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingRoutineOutputTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingProviderObjectTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingQueryArtifactTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingDefaultQueryArtifactTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingDynamicTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingCommentTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingSyntheticNameTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingFilterTests.cs"),
            ReadRepoFile("tests", "LiveProviderScaffoldingDiagnosticsTests.cs"));

    private static string ReadDatabaseScaffolderSource()
        => string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputManager.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DatabaseScaffolder.Models.cs"));

    private static string ReadStaticEntityScaffoldSource()
        => string.Concat(
            ReadDatabaseScaffolderSource(),
            ReadEntityWriterSource(),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.ProviderTypes.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Indexes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceBuilder.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldEntitySourceInfo.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.Helpers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldColumnDiscovery.StoreTypes.Queries.cs"),
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
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanBuilder.Context.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanBuilder.Diagnostics.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanRequest.cs"),
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
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Models.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Probes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Metadata.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.ProviderTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.ProviderTypes.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.ProviderTypes.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.ProviderTypes.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.ProviderTypes.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTableSchemaReader.Names.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.Columns.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.PropertyAttributes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityTypeBuilder.Properties.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityComputedColumnReader.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.ProviderObjects.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityReadOnlyClassifier.Defaults.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityWriteBlockingClassifier.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Facets.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Facets.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Facets.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Facets.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Facets.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Keys.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Keys.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Keys.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Keys.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.Keys.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataReader.StoreTypes.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityProviderTypeMetadataReader.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaMetadataQuery.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntityConnectionKind.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaResolver.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaResolver.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaResolver.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaResolver.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "DynamicEntitySchemaResolver.Queries.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.SqlServer.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.Postgres.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.MySql.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.Sqlite.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldStoreTypeClrMapper.Helpers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldDataReaderHelper.cs"));
}
