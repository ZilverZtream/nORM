using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;

namespace nORM.Cli;

public static partial class ProviderMobilitySchemaInspector
{
    private static readonly IMigrationSqlGenerator[] Generators =
    {
        new SqliteMigrationSqlGenerator(),
        new SqlServerMigrationSqlGenerator(),
        new PostgresMigrationSqlGenerator(),
        new MySqlMigrationSqlGenerator()
    };

    private static void ValidateProviderGeneration(SchemaSnapshot snapshot, List<ProviderMobilityFinding> findings)
    {
        if (findings.Any(static finding => finding.Severity.Equals("Error", StringComparison.OrdinalIgnoreCase)))
            return;

        var diff = new SchemaDiff();
        foreach (var table in snapshot.Tables)
            diff.AddedTables.Add(table);

        foreach (var generator in Generators)
        {
            try
            {
                _ = generator.GenerateSql(diff);
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or NotSupportedException)
            {
                findings.Add(Finding("schema", 0, "schema-provider-generation-failed", "Error",
                    $"{generator.GetType().Name} could not generate provider DDL from the snapshot: {ex.Message}",
                    "Fix schema metadata so all target provider migration generators can emit DDL deterministically."));
            }
        }
    }
}
