#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldModelCompositionBuilder
    {
        public static ScaffoldModelComposition Build(ScaffoldModelDiscoveryResult discovery)
        {
            var featureConfigurations = discovery.FeatureConfigurations;
            var manyToManyJoins = ScaffoldRelationshipAdapter.BuildManyToManyJoins(
                discovery.ForeignKeys,
                discovery.Tables,
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.IdentityColumnsByTable,
                featureConfigurations.ComputedColumnsByTable,
                discovery.Indexes,
                discovery.NonNullableColumnsByTable,
                featureConfigurations.ProviderOwnedWriteBlockedTableKeys,
                discovery.MemberNamesByTable);
            var manyToManyJoinTableKeys = manyToManyJoins
                .Select(static join => join.JoinTableKey)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var relationships = ScaffoldRelationshipAdapter.BuildRelationships(
                discovery.ForeignKeys
                    .Where(fk => !manyToManyJoinTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable)))
                    .ToArray(),
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.Indexes,
                discovery.NonNullableColumnsByTable,
                discovery.MemberNamesByTable);
            var compositePrimaryKeys = ScaffoldRelationshipAdapter.BuildPrimaryKeyConfigurations(
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.PrimaryKeyConstraintNamesByTable,
                manyToManyJoinTableKeys);
            var defaultValueConfigurations = ScaffoldFeatureConfigurationAdapter.BuildDefaultValueConfigurations(
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                featureConfigurations.DefaultValuesByTable);

            RestoreGeneratedManyToManyUnsupportedFeatures(
                discovery.UnsupportedFeatures,
                featureConfigurations.GeneratedModelFeatureDiagnostics,
                manyToManyJoinTableKeys);

            return new ScaffoldModelComposition(
                manyToManyJoins,
                manyToManyJoinTableKeys,
                relationships,
                compositePrimaryKeys,
                defaultValueConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.CheckConstraints
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.ComputedColumnConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.ExpressionIndexConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.CollationConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.IdentityOptionConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.PrecisionConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray(),
                featureConfigurations.ColumnFacetConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray());
        }

        public static void RestoreGeneratedManyToManyUnsupportedFeatures(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> unsupportedFeatures,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics,
            IReadOnlySet<string> manyToManyJoinTableKeys)
        {
            foreach (var feature in generatedModelFeatureDiagnostics)
            {
                if (!ShouldRestoreGeneratedManyToManyUnsupportedFeature(feature.Kind)
                    || !manyToManyJoinTableKeys.Contains(feature.TableKey)
                    || unsupportedFeatures.Contains(feature))
                {
                    continue;
                }

                unsupportedFeatures.Add(feature);
            }
        }

        public static bool ShouldRestoreGeneratedManyToManyUnsupportedFeature(string kind)
            => !string.Equals(kind, "Computed", StringComparison.OrdinalIgnoreCase);

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }

    internal sealed record ScaffoldModelComposition(
        IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> ManyToManyJoins,
        IReadOnlySet<string> ManyToManyJoinTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> Relationships,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> CompositePrimaryKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> DefaultValueConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> CheckConstraints,
        IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> CollationConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> PrecisionConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations);
}
