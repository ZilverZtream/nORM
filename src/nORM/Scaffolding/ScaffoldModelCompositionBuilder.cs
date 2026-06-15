#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelCompositionBuilder
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
    }
}
