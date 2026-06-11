using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;

namespace nORM.Cli;

public static class ProviderMobilitySchemaInspector
{
    private static readonly HashSet<string> PortableClrTypes = new(StringComparer.Ordinal)
    {
        typeof(bool).FullName!,
        typeof(byte).FullName!,
        typeof(sbyte).FullName!,
        typeof(short).FullName!,
        typeof(ushort).FullName!,
        typeof(int).FullName!,
        typeof(uint).FullName!,
        typeof(long).FullName!,
        typeof(ulong).FullName!,
        typeof(float).FullName!,
        typeof(double).FullName!,
        typeof(decimal).FullName!,
        typeof(string).FullName!,
        typeof(Guid).FullName!,
        typeof(DateTime).FullName!,
        typeof(DateTimeOffset).FullName!,
        typeof(DateOnly).FullName!,
        typeof(TimeOnly).FullName!,
        typeof(TimeSpan).FullName!,
        typeof(char).FullName!,
        typeof(byte[]).FullName!
    };

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

    private static readonly IMigrationSqlGenerator[] Generators =
    {
        new SqliteMigrationSqlGenerator(),
        new SqlServerMigrationSqlGenerator(),
        new PostgresMigrationSqlGenerator(),
        new MySqlMigrationSqlGenerator()
    };

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

    private static bool IsPortableClrType(string clrType, out bool unresolvedCustomType)
    {
        unresolvedCustomType = false;
        if (PortableClrTypes.Contains(clrType))
            return true;

        var resolved = ResolveType(clrType);
        if (resolved?.IsEnum == true)
            return true;

        unresolvedCustomType = !clrType.StartsWith("System.", StringComparison.Ordinal);
        return false;
    }

    private static bool IsPortableIdentityType(string clrType)
        => clrType == typeof(byte).FullName ||
           clrType == typeof(short).FullName ||
           clrType == typeof(int).FullName ||
           clrType == typeof(long).FullName;

    private static Type? ResolveType(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return null;

        var type = Type.GetType(typeName);
        if (type != null)
            return type;

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            type = assembly.GetType(typeName);
            if (type != null)
                return type;
        }

        return null;
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
