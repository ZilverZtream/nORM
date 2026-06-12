using System;
using System.Collections.Generic;
using nORM.Migration;

namespace nORM.Cli;

public static partial class ProviderMobilitySchemaInspector
{
    private static void InspectColumns(TableSchema table, List<ProviderMobilityFinding> findings)
    {
        if (table.Columns is null)
        {
            findings.Add(Finding($"schema:{table.Name}", 0, "schema-invalid", "Error",
                $"Table '{table.Name}' has null Columns.",
                "Regenerate the snapshot through nORM migrations or a design-time DbContext."));
            return;
        }

        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pkCount = 0;
        foreach (var column in table.Columns)
        {
            var location = $"schema:{table.Name}.{column.Name}";
            if (string.IsNullOrWhiteSpace(column.Name))
            {
                findings.Add(Finding($"schema:{table.Name}", 0, "schema-invalid-identifier", "Error",
                    $"Table '{table.Name}' has a column with no name.",
                    "Give every mapped property a non-empty column name."));
                continue;
            }

            if (!columnNames.Add(column.Name))
            {
                findings.Add(Finding(location, 0, "schema-duplicate-column", "Error",
                    $"Table '{table.Name}' contains duplicate column '{column.Name}'.",
                    "Use distinct column names before certifying provider mobility."));
            }

            InspectColumnType(table, column, location, findings);
            if (column.IsPrimaryKey)
                pkCount++;

            if (column.IsIdentity && !IsPortableIdentityType(column.ClrType))
            {
                findings.Add(Finding(location, 0, "schema-nonportable-identity", "Error",
                    $"Column '{table.Name}.{column.Name}' is marked identity but uses '{column.ClrType}'.",
                    "Use an integral identity column type for provider-mobile generated migrations."));
            }

            InspectDefaultValue(table, column, findings);
        }

        if (pkCount == 0)
        {
            findings.Add(Finding($"schema:{table.Name}", 0, "schema-missing-primary-key", "Warning",
                $"Table '{table.Name}' has no primary key in the migration snapshot.",
                "Add a primary key for full generated write, include, tenant, and temporal behavior. Keyless/read-only shapes should be explicitly documented."));
        }
    }

    private static void InspectColumnType(
        TableSchema table,
        ColumnSchema column,
        string location,
        List<ProviderMobilityFinding> findings)
    {
        if (string.IsNullOrWhiteSpace(column.ClrType))
        {
            findings.Add(Finding(location, 0, "schema-missing-clr-type", "Error",
                $"Column '{table.Name}.{column.Name}' has no CLR type metadata.",
                "Regenerate the schema snapshot from nORM mapping metadata so migration type mapping is deterministic."));
        }
        else if (!IsPortableClrType(column.ClrType, out var unresolvedCustomType))
        {
            findings.Add(Finding(location, 0, "schema-unsupported-clr-type", unresolvedCustomType ? "Warning" : "Error",
                $"Column '{table.Name}.{column.Name}' uses CLR type '{column.ClrType}', which is not in the provider-mobile scalar type set.",
                unresolvedCustomType
                    ? "Run certification with the application assembly loaded so enum types can be resolved, or convert the property to a supported scalar provider type."
                    : "Use a supported scalar CLR type or a value converter whose provider type is in nORM's provider-mobile type map."));
        }
    }
}
