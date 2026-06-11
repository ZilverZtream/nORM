#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static readonly IReadOnlyDictionary<string, object?> EmptyMetadata =
            new Dictionary<string, object?>(0, StringComparer.Ordinal);

        private static CompositeForeignKeyJsonItem[] BuildCompositeForeignKeyJsonItems(
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys)
            => compositeForeignKeys
                .Select(foreignKey => new CompositeForeignKeyJsonItem
                {
                    code = CodeForCompositeForeignKey(),
                    severity = Severity(),
                    category = CategoryForCompositeForeignKey(),
                    constraint = foreignKey.ConstraintName,
                    dependentTable = foreignKey.DependentTable,
                    dependentColumns = foreignKey.DependentColumns,
                    principalTable = foreignKey.PrincipalTable,
                    principalColumns = foreignKey.PrincipalColumns,
                    metadata = foreignKey.Metadata,
                    suggestedAction = SuggestedActionForCompositeForeignKey()
                })
                .ToArray();

        private static PossibleJoinTableJsonItem[] BuildPossibleJoinTableJsonItems(
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables)
            => possibleJoinTables
                .Select(table => new PossibleJoinTableJsonItem
                {
                    code = CodeForPossibleJoinTable(),
                    severity = Severity(),
                    category = CategoryForPossibleJoinTable(),
                    table = table.TableKey,
                    principalTables = table.PrincipalTables,
                    constraints = table.ConstraintNames,
                    reasons = table.Reasons,
                    metadata = table.Metadata,
                    suggestedAction = SuggestedActionForPossibleJoinTable()
                })
                .ToArray();

        private static ProviderOwnedSchemaFeatureJsonItem[] BuildProviderOwnedSchemaFeatureJsonItems(
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures)
            => unsupportedFeatures
                .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                .ThenBy(f => f.Kind, StringComparer.Ordinal)
                .ThenBy(f => f.Name, StringComparer.Ordinal)
                .Select(feature => new ProviderOwnedSchemaFeatureJsonItem
                {
                    code = CodeForUnsupportedFeature(feature.Kind),
                    severity = Severity(),
                    category = CategoryForUnsupportedFeature(feature.Kind),
                    kind = feature.Kind,
                    table = feature.TableKey,
                    name = feature.Name,
                    detail = feature.Detail,
                    metadata = feature.Metadata ?? new Dictionary<string, object?>(0, StringComparer.Ordinal),
                    suggestedAction = SuggestedActionForUnsupportedFeature(feature.Kind)
                })
                .ToArray();

        private static SkippedDatabaseObjectJsonItem[] BuildSkippedDatabaseObjectJsonItems(
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
            => skippedObjects
                .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                .ThenBy(o => o.Kind, StringComparer.Ordinal)
                .Select(obj => new SkippedDatabaseObjectJsonItem
                {
                    code = CodeForSkippedObject(obj.Kind),
                    severity = Severity(),
                    category = CategoryForSkippedObject(obj.Kind),
                    kind = obj.Kind,
                    name = TableKey(obj.Schema, obj.Name),
                    detail = obj.Detail,
                    metadata = obj.Metadata ?? new Dictionary<string, object?>(0, StringComparer.Ordinal),
                    suggestedAction = SuggestedActionForSkippedObject(obj.Kind)
                })
                .ToArray();

        private static Dictionary<string, int> CountByOrdinalValue(this IEnumerable<string> values)
            => values
                .GroupBy(value => value, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

        private sealed class CompositeForeignKeyJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string constraint { get; init; } = string.Empty;
            public string dependentTable { get; init; } = string.Empty;
            public IReadOnlyList<string> dependentColumns { get; init; } = Array.Empty<string>();
            public string principalTable { get; init; } = string.Empty;
            public IReadOnlyList<string> principalColumns { get; init; } = Array.Empty<string>();
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }

        private sealed class PossibleJoinTableJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string table { get; init; } = string.Empty;
            public IReadOnlyList<string> principalTables { get; init; } = Array.Empty<string>();
            public IReadOnlyList<string> constraints { get; init; } = Array.Empty<string>();
            public IReadOnlyList<string> reasons { get; init; } = Array.Empty<string>();
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }

        private sealed class ProviderOwnedSchemaFeatureJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string kind { get; init; } = string.Empty;
            public string table { get; init; } = string.Empty;
            public string name { get; init; } = string.Empty;
            public string detail { get; init; } = string.Empty;
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }

        private sealed class SkippedDatabaseObjectJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string kind { get; init; } = string.Empty;
            public string name { get; init; } = string.Empty;
            public string detail { get; init; } = string.Empty;
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }
    }
}
