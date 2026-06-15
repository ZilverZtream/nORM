#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> ConvertDefaultValueConfigurations(
            IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> defaultValues)
            => defaultValues
                .Select(static value => new ScaffoldDefaultValueConfiguration(value.TableKey, value.EntityName, value.ColumnName, value.PropertyName, value.DefaultValueSql, value.ConstraintName))
                .ToArray();

        private static IReadOnlyList<ScaffoldCollationConfiguration> ConvertCollationConfigurations(
            IReadOnlyList<ScaffoldCollationConfigurationInfo> collations)
            => collations
                .Select(static collation => new ScaffoldCollationConfiguration(collation.TableKey, collation.EntityName, collation.ColumnName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static IReadOnlyList<ScaffoldComputedColumnConfiguration> ConvertComputedColumnConfigurations(
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> computedColumns)
            => computedColumns
                .Select(static computed => new ScaffoldComputedColumnConfiguration(computed.TableKey, computed.EntityName, computed.ColumnName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> ConvertDecimalPrecisionMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, ScaffoldDecimalPrecision>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new ScaffoldDecimalPrecision(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> ConvertDecimalPrecisionInfoMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new ScaffoldDecimalPrecisionInfo(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<ScaffoldPrecisionConfiguration> ConvertPrecisionConfigurations(
            IReadOnlyList<ScaffoldPrecisionConfigurationInfo> precisionConfigurations)
            => precisionConfigurations
                .Select(static precision => new ScaffoldPrecisionConfiguration(precision.TableKey, precision.EntityName, precision.ColumnName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static IReadOnlyList<ScaffoldColumnFacetConfiguration> ConvertColumnFacetConfigurations(
            IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(static facet => new ScaffoldColumnFacetConfiguration(facet.TableKey, facet.EntityName, facet.ColumnName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> ConvertIdentityOptionConfigurations(
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> identityOptions)
            => identityOptions
                .Select(static identity => new ScaffoldIdentityOptionConfiguration(identity.TableKey, identity.EntityName, identity.ColumnName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();
    }
}
