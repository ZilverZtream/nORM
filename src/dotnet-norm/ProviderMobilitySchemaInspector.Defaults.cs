using System;
using System.Collections.Generic;
using nORM.Migration;

namespace nORM.Cli;

public static partial class ProviderMobilitySchemaInspector
{
    private static readonly string[] ProviderSpecificDefaultTokens =
    {
        "GETDATE(",
        "GETUTCDATE(",
        "SYSDATETIME(",
        "NEWID(",
        "NEWSEQUENTIALID(",
        "DATEADD(",
        "FOR JSON",
        "UUID(",
        "UUID_TO_BIN(",
        "GEN_RANDOM_UUID(",
        "NOW(",
        "SYSDATE(",
        "CLOCK_TIMESTAMP(",
        "LAST_INSERT_ID(",
        "NEXTVAL",
        "timezone(",
        "::",
        "N'"
    };

    private static readonly string[] DefaultReviewTokens =
    {
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "CURRENT_USER"
    };

    private static void InspectDefaultValue(TableSchema table, ColumnSchema column, List<ProviderMobilityFinding> findings)
    {
        if (string.IsNullOrWhiteSpace(column.DefaultValue))
            return;

        var defaultValue = column.DefaultValue!;
        foreach (var token in ProviderSpecificDefaultTokens)
        {
            if (!defaultValue.Contains(token, StringComparison.OrdinalIgnoreCase))
                continue;

            findings.Add(Finding($"schema:{table.Name}.{column.Name}", 0, "schema-provider-specific-default", "Error",
                $"Column '{table.Name}.{column.Name}' default value '{defaultValue}' contains provider-specific token '{token}'.",
                "Use an application-stamped value, a simple literal default, or an explicit provider-specific migration outside strict certification."));
            return;
        }

        foreach (var token in DefaultReviewTokens)
        {
            if (!defaultValue.Equals(token, StringComparison.OrdinalIgnoreCase))
                continue;

            findings.Add(Finding($"schema:{table.Name}.{column.Name}", 0, "schema-default-needs-review", "Warning",
                $"Column '{table.Name}.{column.Name}' default value '{defaultValue}' uses database-evaluated token '{token}'.",
                "Verify timezone, precision, and user semantics on every target provider or replace it with an application-stamped value before strict certification."));
            return;
        }

        if (defaultValue.Contains('(') && !defaultValue.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            findings.Add(Finding($"schema:{table.Name}.{column.Name}", 0, "schema-default-needs-review", "Warning",
                $"Column '{table.Name}.{column.Name}' default value '{defaultValue}' looks function-based.",
                "Verify this default on every target provider or replace it with an application-stamped value before strict certification."));
        }
    }
}
