using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using nORM.Migration;

namespace nORM.Cli;

public static partial class ProviderMobilityCertificationRunner
{
    private static (SchemaSnapshot? Snapshot, IReadOnlyList<ProviderMobilityFinding> Findings) ReadSchemaSnapshot(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return (null, Array.Empty<ProviderMobilityFinding>());

        var fullPath = Path.GetFullPath(path);
        if (!File.Exists(fullPath))
        {
            return (null, new[]
            {
                new ProviderMobilityFinding(
                    fullPath,
                    0,
                    "schema-snapshot-missing",
                    "Error",
                    "The requested nORM schema snapshot path does not exist.",
                    "Pass a valid schema.snapshot.json path or use --assembly so the CLI can build the snapshot from the design-time DbContext.")
            });
        }

        try
        {
            return (JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(fullPath))
                ?? new SchemaSnapshot(), Array.Empty<ProviderMobilityFinding>());
        }
        catch (JsonException ex)
        {
            return (null, new[]
            {
                new ProviderMobilityFinding(
                    fullPath,
                    0,
                    "schema-snapshot-invalid-json",
                    "Error",
                    "The requested nORM schema snapshot is not valid JSON: " + ex.Message,
                    "Regenerate the snapshot through nORM migrations or pass the application assembly with --assembly.")
            });
        }
    }
}
