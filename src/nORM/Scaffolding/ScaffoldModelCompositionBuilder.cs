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
            var manyToManyJoins = BuildManyToManyJoins(discovery, featureConfigurations);
            var manyToManyJoinTableKeys = BuildManyToManyJoinTableKeys(manyToManyJoins);
            var relationships = BuildNonJoinRelationships(discovery, manyToManyJoinTableKeys);
            var compositePrimaryKeys = ScaffoldRelationshipAdapter.BuildPrimaryKeyConfigurations(
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.PrimaryKeyConstraintNamesByTable,
                manyToManyJoinTableKeys);
            var defaultValueConfigurations = BuildDefaultValueConfigurations(discovery, featureConfigurations);

            RestoreGeneratedManyToManyUnsupportedFeatures(
                discovery.UnsupportedFeatures,
                featureConfigurations.GeneratedModelFeatureDiagnostics,
                manyToManyJoinTableKeys);

            return new ScaffoldModelComposition(
                manyToManyJoins,
                manyToManyJoinTableKeys,
                relationships,
                compositePrimaryKeys,
                ExcludeJoinTableConfigurations(
                    defaultValueConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.CheckConstraints,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.ComputedColumnConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.ExpressionIndexConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.CollationConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.IdentityOptionConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.PrecisionConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey),
                ExcludeJoinTableConfigurations(
                    featureConfigurations.ColumnFacetConfigurations,
                    manyToManyJoinTableKeys,
                    static config => config.TableKey));
        }

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> BuildManyToManyJoins(
            ScaffoldModelDiscoveryResult discovery,
            DatabaseScaffolder.ScaffoldFeatureConfigurations featureConfigurations)
            => ScaffoldRelationshipAdapter.BuildManyToManyJoins(
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

        private static IReadOnlySet<string> BuildManyToManyJoinTableKeys(
            IEnumerable<DatabaseScaffolder.ScaffoldManyToManyJoin> manyToManyJoins)
            => manyToManyJoins
                .Select(static join => join.JoinTableKey)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> BuildNonJoinRelationships(
            ScaffoldModelDiscoveryResult discovery,
            IReadOnlySet<string> manyToManyJoinTableKeys)
            => ScaffoldRelationshipAdapter.BuildRelationships(
                discovery.ForeignKeys
                    .Where(fk => !manyToManyJoinTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable)))
                    .ToArray(),
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.Indexes,
                discovery.NonNullableColumnsByTable,
                discovery.MemberNamesByTable);

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            ScaffoldModelDiscoveryResult discovery,
            DatabaseScaffolder.ScaffoldFeatureConfigurations featureConfigurations)
            => ScaffoldFeatureConfigurationAdapter.BuildDefaultValueConfigurations(
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                featureConfigurations.DefaultValuesByTable,
                featureConfigurations.DefaultConstraintNamesByTable);

        private static T[] ExcludeJoinTableConfigurations<T>(
            IEnumerable<T> configurations,
            IReadOnlySet<string> manyToManyJoinTableKeys,
            Func<T, string> getTableKey)
            => configurations
                .Where(config => !manyToManyJoinTableKeys.Contains(getTableKey(config)))
                .ToArray();

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
