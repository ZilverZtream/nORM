#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        private static IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> ConvertDefaultValueConfigurations(
            IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> defaultValues)
            => defaultValues
                .Select(static value => new DatabaseScaffolder.ScaffoldDefaultValueConfiguration(value.TableKey, value.EntityName, value.ColumnName, value.PropertyName, value.DefaultValueSql, value.ConstraintName))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> ConvertCollationConfigurations(
            IReadOnlyList<ScaffoldCollationConfigurationInfo> collations)
            => collations
                .Select(static collation => new DatabaseScaffolder.ScaffoldCollationConfiguration(collation.TableKey, collation.EntityName, collation.ColumnName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> ConvertComputedColumnConfigurations(
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> computedColumns)
            => computedColumns
                .Select(static computed => new DatabaseScaffolder.ScaffoldComputedColumnConfiguration(computed.TableKey, computed.EntityName, computed.ColumnName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> ConvertDecimalPrecisionMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new DatabaseScaffolder.ScaffoldDecimalPrecision(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> ConvertDecimalPrecisionInfoMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new ScaffoldDecimalPrecisionInfo(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> ConvertPrecisionConfigurations(
            IReadOnlyList<ScaffoldPrecisionConfigurationInfo> precisionConfigurations)
            => precisionConfigurations
                .Select(static precision => new DatabaseScaffolder.ScaffoldPrecisionConfiguration(precision.TableKey, precision.EntityName, precision.ColumnName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> ConvertColumnFacetConfigurations(
            IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(static facet => new DatabaseScaffolder.ScaffoldColumnFacetConfiguration(facet.TableKey, facet.EntityName, facet.ColumnName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> ConvertIdentityOptionConfigurations(
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> identityOptions)
            => identityOptions
                .Select(static identity => new DatabaseScaffolder.ScaffoldIdentityOptionConfiguration(identity.TableKey, identity.EntityName, identity.ColumnName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();
    }
}
