using System;
using System.Collections.Generic;
using nORM.Migration;

namespace nORM.Cli;

public static partial class ProviderMobilitySchemaInspector
{
    public static IReadOnlyList<ProviderMobilityFinding> Inspect(SchemaSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var findings = new List<ProviderMobilityFinding>();
        if (snapshot.Tables is null)
        {
            findings.Add(Finding("schema", 0, "schema-invalid", "Error",
                "SchemaSnapshot.Tables is null.",
                "Regenerate the snapshot through nORM migrations or a design-time DbContext."));
            return findings;
        }

        var tableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var table in snapshot.Tables)
        {
            if (string.IsNullOrWhiteSpace(table.Name))
            {
                findings.Add(Finding("schema", 0, "schema-invalid-identifier", "Error",
                    "A schema table has no name.",
                    "Give every mapped entity a non-empty table name."));
                continue;
            }

            if (!tableNames.Add(table.Name))
            {
                findings.Add(Finding($"schema:{table.Name}", 0, "schema-duplicate-table", "Error",
                    $"The schema contains duplicate table name '{table.Name}'.",
                    "Use distinct table names before certifying provider mobility."));
            }

            InspectColumns(table, findings);
            InspectForeignKeys(table, findings);
        }

        ValidateProviderGeneration(snapshot, findings);
        return findings;
    }

    private static ProviderMobilityFinding Finding(
        string path,
        int line,
        string kind,
        string severity,
        string reason,
        string suggestedFix)
        => new(path, line, kind, severity, reason, suggestedFix);
}
