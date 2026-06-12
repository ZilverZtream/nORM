using System;
using System.Collections.Generic;
using nORM.Migration;

namespace nORM.Cli;

public static partial class ProviderMobilitySchemaInspector
{
    private static void InspectForeignKeys(TableSchema table, List<ProviderMobilityFinding> findings)
    {
        if (table.ForeignKeys is null)
        {
            findings.Add(Finding($"schema:{table.Name}", 0, "schema-invalid", "Error",
                $"Table '{table.Name}' has null ForeignKeys.",
                "Regenerate the snapshot through nORM migrations or a design-time DbContext."));
            return;
        }

        var fkNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var fk in table.ForeignKeys)
        {
            var location = $"schema:{table.Name}.{fk.ConstraintName}";
            if (string.IsNullOrWhiteSpace(fk.ConstraintName) || !fkNames.Add(fk.ConstraintName))
            {
                findings.Add(Finding(location, 0, "schema-invalid-foreign-key", "Error",
                    $"Table '{table.Name}' has an empty or duplicate foreign key constraint name.",
                    "Give each foreign key a stable, unique constraint name."));
            }

            if (fk.DependentColumns is null || fk.DependentColumns.Length == 0 ||
                fk.PrincipalColumns is null || fk.PrincipalColumns.Length == 0 ||
                string.IsNullOrWhiteSpace(fk.PrincipalTable))
            {
                findings.Add(Finding(location, 0, "schema-invalid-foreign-key", "Error",
                    $"Foreign key '{fk.ConstraintName}' on '{table.Name}' is missing dependent columns, principal columns, or principal table.",
                    "Regenerate the schema snapshot from fluent relation metadata."));
            }
        }
    }
}
