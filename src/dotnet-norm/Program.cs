using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using nORM.Cli;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using nORM.Scaffolding;
using nORM.Security;

var root = new RootCommand("Command line tools for the nORM ORM framework");

root.Add(CreateScaffoldCommand());

var dbcontext = new Command("dbcontext", "EF-style DbContext command aliases. Use 'norm dbcontext scaffold ...' to run bounded nORM scaffolding.");
root.Add(dbcontext);

root.Add(CreateDatabaseCommand());

var portability = new Command("portability", "Provider mobility certification and diagnostics");
var portabilityCertify = new Command("certify", "Scan an application for provider-bound code that cannot pass strict provider mobility certification.");
var portabilityScanPathOpt = new Option<string>("--scan-path") { Description = "Application source directory or file to scan.", Required = true };
var portabilityReportOpt = new Option<string?>("--report") { Description = "Path to write the JSON certification report." };
var portabilityHtmlOpt = new Option<string?>("--html") { Description = "Path to write an HTML certification dashboard." };
var portabilityProfileOpt = new Option<string>("--profile") { Description = "Certification profile label, e.g. all-four, sqlite-postgres, sqlserver-postgres.", DefaultValueFactory = _ => "all-four" };
var portabilityProvidersOpt = new Option<string?>("--providers") { Description = "Comma-separated provider target list for capability profiling, e.g. sqlite,postgres,mysql." };
var portabilitySqliteConnectionOpt = new Option<string?>("--sqlite-connection") { Description = "Optional SQLite target connection string to open and validate for actual provider mobility evidence." };
var portabilitySqlServerConnectionOpt = new Option<string?>("--sqlserver-connection") { Description = "Optional SQL Server target connection string to open and validate for actual provider mobility evidence." };
var portabilityPostgresConnectionOpt = new Option<string?>("--postgres-connection") { Description = "Optional PostgreSQL target connection string to open and validate for actual provider mobility evidence." };
var portabilityMySqlConnectionOpt = new Option<string?>("--mysql-connection") { Description = "Optional MySQL target connection string to open and validate for actual provider mobility evidence." };
var portabilitySchemaSnapshotOpt = new Option<string?>("--schema-snapshot") { Description = "Optional nORM schema.snapshot.json to inspect for provider-mobile schema metadata." };
var portabilityAssemblyOpt = new Option<string?>("--assembly") { Description = "Optional application assembly containing a design-time DbContext used to inspect provider-mobile schema metadata." };
var portabilityAttributeOnlyOpt = new Option<bool>("--attribute-only") { Description = "When --assembly is used, build the schema from attributes only instead of requiring a design-time DbContext." };
portabilityCertify.Add(portabilityScanPathOpt);
portabilityCertify.Add(portabilityReportOpt);
portabilityCertify.Add(portabilityHtmlOpt);
portabilityCertify.Add(portabilityProfileOpt);
portabilityCertify.Add(portabilityProvidersOpt);
portabilityCertify.Add(portabilitySqliteConnectionOpt);
portabilityCertify.Add(portabilitySqlServerConnectionOpt);
portabilityCertify.Add(portabilityPostgresConnectionOpt);
portabilityCertify.Add(portabilityMySqlConnectionOpt);
portabilityCertify.Add(portabilitySchemaSnapshotOpt);
portabilityCertify.Add(portabilityAssemblyOpt);
portabilityCertify.Add(portabilityAttributeOnlyOpt);
portabilityCertify.SetAction((ParseResult result) =>
{
    try
    {
        var schemaSnapshotPath = result.GetValue(portabilitySchemaSnapshotOpt);
        var assemblyPath = result.GetValue(portabilityAssemblyOpt);
        if (!string.IsNullOrWhiteSpace(schemaSnapshotPath) && !string.IsNullOrWhiteSpace(assemblyPath))
        {
            Console.Error.WriteLine("Use either --schema-snapshot or --assembly for schema inspection, not both.");
            return 2;
        }

        SchemaSnapshot? schemaSnapshot = null;
        if (!string.IsNullOrWhiteSpace(assemblyPath))
        {
            if (!File.Exists(assemblyPath))
            {
                Console.Error.WriteLine($"Assembly '{assemblyPath}' not found.");
                return 2;
            }

            var assembly = LoadDesignTimeAssembly(assemblyPath);
            schemaSnapshot = BuildMigrationSnapshot(assembly, result.GetValue(portabilityAttributeOnlyOpt), Array.Empty<string>());
        }

        var targetProviders = ParseProviderTargetList(result.GetValue(portabilityProvidersOpt));
        var targetVersions = new Dictionary<string, Version?>(StringComparer.OrdinalIgnoreCase);
        var targetFindings = new List<ProviderMobilityFinding>();
        var connectionTargets = new (string Provider, string? ConnectionString)[]
        {
            ("sqlite", result.GetValue(portabilitySqliteConnectionOpt)),
            ("sqlserver", result.GetValue(portabilitySqlServerConnectionOpt)),
            ("postgres", result.GetValue(portabilityPostgresConnectionOpt)),
            ("mysql", result.GetValue(portabilityMySqlConnectionOpt))
        };
        var explicitTargetProviders = targetProviders?.ToList();
        foreach (var target in connectionTargets)
        {
            if (string.IsNullOrWhiteSpace(target.ConnectionString))
                continue;

            explicitTargetProviders ??= new List<string>();
            if (!explicitTargetProviders.Contains(target.Provider, StringComparer.OrdinalIgnoreCase))
                explicitTargetProviders.Add(target.Provider);
            ProbeProviderTargetConnection(target.Provider, target.ConnectionString!, targetVersions, targetFindings);
        }

        var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
        {
            ScanPath = result.GetValue(portabilityScanPathOpt)!,
            JsonReportPath = result.GetValue(portabilityReportOpt),
            HtmlReportPath = result.GetValue(portabilityHtmlOpt),
            Profile = result.GetValue(portabilityProfileOpt)!,
            TargetProviders = explicitTargetProviders,
            TargetProviderVersions = targetVersions,
            TargetProviderFindings = targetFindings,
            SchemaSnapshotPath = schemaSnapshotPath,
            SchemaSnapshot = schemaSnapshot
        });

        Console.WriteLine($"Provider mobility certification {report.Status}: {report.ErrorCount} error(s), {report.WarningCount} warning(s), {report.ScannedFiles} file(s) scanned, {report.SchemaTables} schema table(s) inspected.");
        if (!string.IsNullOrWhiteSpace(result.GetValue(portabilityReportOpt)))
            Console.WriteLine($"JSON report written to {Path.GetFullPath(result.GetValue(portabilityReportOpt)!)}");
        if (!string.IsNullOrWhiteSpace(result.GetValue(portabilityHtmlOpt)))
            Console.WriteLine($"HTML report written to {Path.GetFullPath(result.GetValue(portabilityHtmlOpt)!)}");
        return report.Status == "PASS" ? 0 : 1;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});
portability.Add(portabilityCertify);
root.Add(portability);

// migrations add
var migrations = new Command("migrations", "Migration management commands");
var add = new Command("add", "Add a new migration based on model changes.\nExample:\n  norm migrations add InitialCreate --provider sqlserver --assembly App.dll");
var migNameArg = new Argument<string>("name") { Description = "Migration name" };
var addProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name.", Required = true };
// --assembly is optional when --project is supplied; the CLI resolves the output assembly from the project automatically.
var addAsmOpt = new Option<string?>("--assembly") { Description = "Path to assembly containing DbContext and entities. Optional when --project is supplied." };
var addOutOpt = new Option<string>("--output") { Description = "Output directory for migrations", DefaultValueFactory = _ => "Migrations" };
var addForceOpt = new Option<bool>("--force") { Description = "Allow destructive table/column drops when generating the migration." };
var addAttributeOnlyOpt = new Option<bool>("--attribute-only") { Description = "Generate from attribute-only entity scanning instead of a design-time DbContext." };
var addProjectOpt = new Option<string?>("--project") { Description = "Path to the .csproj file. When provided, the output assembly is resolved via 'dotnet msbuild' if --assembly is not supplied." };
// Explicit design-time loading flags for realistic application layouts (multi-project
// solutions, multi-TFM projects, external dependency graphs). All optional; when omitted, the
// design-time loader falls back to the existing project/assembly resolution path.
var addStartupProjectOpt = new Option<string?>("--startup-project") { Description = "Path to the startup .csproj used to host the design-time DbContext (for multi-project solutions)." };
var addDepsOpt = new Option<string?>("--deps") { Description = "Path to a .deps.json file describing assembly dependencies for the design-time load." };
var addRuntimeConfigOpt = new Option<string?>("--runtimeconfig") { Description = "Path to a .runtimeconfig.json file describing the runtime configuration for the design-time load." };
var addTargetFrameworkOpt = new Option<string?>("--target-framework", "--framework") { Description = "Target framework moniker (e.g., net8.0) used when resolving the output assembly from --project." };
var addConfigurationOpt = new Option<string?>("--configuration") { Description = "Build configuration used when resolving the output assembly from --project or --startup-project." };
var addRuntimeOpt = new Option<string?>("--runtime") { Description = "Runtime identifier used when resolving the output assembly from --project or --startup-project." };
var addEnvironmentOpt = new Option<string?>("--environment") { Description = "Design-time environment passed to factories and exposed as ASPNETCORE_ENVIRONMENT/DOTNET_ENVIRONMENT while building the model." };
add.Add(migNameArg);
add.Add(addProvOpt);
add.Add(addAsmOpt);
add.Add(addOutOpt);
add.Add(addForceOpt);
add.Add(addAttributeOnlyOpt);
add.Add(addProjectOpt);
add.Add(addStartupProjectOpt);
add.Add(addDepsOpt);
add.Add(addRuntimeConfigOpt);
add.Add(addTargetFrameworkOpt);
add.Add(addConfigurationOpt);
add.Add(addRuntimeOpt);
add.Add(addEnvironmentOpt);
add.SetAction((ParseResult result) =>
{
    try
    {
        var name = result.GetValue(migNameArg)!;
        var prov = NormalizeProviderName(result.GetValue(addProvOpt)!);
        var asmPath = NullIfWhiteSpace(result.GetValue(addAsmOpt));
        var output = result.GetValue(addOutOpt)!;
        var force = result.GetValue(addForceOpt);
        var attributeOnly = result.GetValue(addAttributeOnlyOpt);
        var projectPath = NullIfWhiteSpace(result.GetValue(addProjectOpt));
        var startupProjectPath = NullIfWhiteSpace(result.GetValue(addStartupProjectOpt));
        var depsPath = NullIfWhiteSpace(result.GetValue(addDepsOpt));
        var runtimeConfigPath = NullIfWhiteSpace(result.GetValue(addRuntimeConfigOpt));
        var targetFramework = NullIfWhiteSpace(result.GetValue(addTargetFrameworkOpt));
        var configuration = NullIfWhiteSpace(result.GetValue(addConfigurationOpt));
        var runtime = NullIfWhiteSpace(result.GetValue(addRuntimeOpt));
        var environment = NullIfWhiteSpace(result.GetValue(addEnvironmentOpt));

        if (!TryResolveExistingDesignTimeFile(depsPath, "Deps file", out var resolvedDepsPath) ||
            !TryResolveExistingDesignTimeFile(runtimeConfigPath, "Runtime config file", out var resolvedRuntimeConfigPath))
        {
            return 2;
        }

        // --startup-project takes precedence over --project for design-time host resolution
        // when both are supplied (matches the EF tooling convention).
        var effectiveProject = startupProjectPath ?? projectPath;

        // When --project (or --startup-project) is supplied and --assembly was not (or points to
        // a non-existent file), resolve the output assembly path via 'dotnet msbuild'.
        if (effectiveProject != null && (string.IsNullOrEmpty(asmPath) || !File.Exists(asmPath)))
        {
            if (!File.Exists(effectiveProject))
            {
                Console.Error.WriteLine($"Project file '{effectiveProject}' not found.");
                return 2;
            }
            asmPath = ResolveAssemblyFromProject(effectiveProject, targetFramework, configuration, runtime);
            if (asmPath == null)
            {
                Console.Error.WriteLine($"Could not determine output assembly from project '{effectiveProject}'. Build the project first or supply --assembly explicitly.");
                return 2;
            }
        }

        if (string.IsNullOrEmpty(asmPath) || !File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return 2;
        }
        var assembly = LoadDesignTimeAssembly(asmPath!, resolvedDepsPath, resolvedRuntimeConfigPath);

        var snapshotPath = Path.Combine(output, "schema.snapshot.json");
        SchemaSnapshot oldSnap = File.Exists(snapshotPath)
            ? JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(snapshotPath)) ?? new SchemaSnapshot()
            : new SchemaSnapshot();

        using var environmentScope = DesignTimeEnvironmentScope.Apply(environment);
        var newSnap = BuildMigrationSnapshot(assembly, attributeOnly, BuildDesignTimeArgs(environment));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        if (!diff.HasChanges)
        {
            Console.WriteLine("No changes detected.");
            return 0;
        }

        var destructiveWarnings = diff.GetDestructiveChangeWarnings();
        if (destructiveWarnings.Count > 0 && !force)
        {
            Console.Error.WriteLine("Destructive schema changes detected. No migration was written.");
            foreach (var warning in destructiveWarnings)
                Console.Error.WriteLine($"  - {warning}");
            Console.Error.WriteLine("Re-run with --force after replacing rename-like drops/adds with explicit rename operations or after accepting the data or integrity risk.");
            return 3;
        }

        IMigrationSqlGenerator generator = prov.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlServerMigrationSqlGenerator(),
            "sqlite" => new SqliteMigrationSqlGenerator(),
            "postgres" or "postgresql" => new PostgresMigrationSqlGenerator(),
            "mysql" => new MySqlMigrationSqlGenerator(),
            _ => throw new ArgumentException($"Provider '{prov}' not supported.")
        };

        var sql = generator.GenerateSql(diff);
        Directory.CreateDirectory(output);

        var version = long.Parse(DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
        var className = $"Migration_{version}_{ToCSharpIdentifier(name)}";
        var filePath = Path.Combine(output, className + ".cs");

        File.WriteAllText(filePath, MigrationCodeWriter.WriteMigrationSource(className, version, name, sql, destructiveWarnings));

        var snapJson = JsonSerializer.Serialize(newSnap, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(snapshotPath, snapJson);
        Console.WriteLine($"Migration '{className}' generated at {filePath}.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});

migrations.Add(add);
root.Add(migrations);

return await root.Parse(NormalizeEfStyleCommandArgs(args)).InvokeAsync(new InvocationConfiguration());

static string[] NormalizeEfStyleCommandArgs(string[] args)
{
    if (args.Length == 0 || !string.Equals(args[0], "dbcontext", StringComparison.OrdinalIgnoreCase))
        return args;

    if (args.Length == 1)
        return new[] { "scaffold", "--help" };

    if (IsHelpToken(args[1]))
        return new[] { "scaffold" }.Concat(args.Skip(1)).ToArray();

    if (string.Equals(args[1], "scaffold", StringComparison.OrdinalIgnoreCase))
    {
        return new[] { "scaffold" }.Concat(args.Skip(2)).ToArray();
    }

    return args;
}
static bool IsHelpToken(string token)
    => token is "--help" or "-h" or "/?";



static IReadOnlyList<string>? ParseProviderTargetList(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    return value
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(static provider => NormalizeProviderTargetName(provider))
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();
}

static string NormalizeProviderTargetName(string providerName)
    => NormalizeProviderName(providerName);

static void ProbeProviderTargetConnection(
    string providerName,
    string connectionString,
    IDictionary<string, Version?> targetVersions,
    ICollection<ProviderMobilityFinding> findings)
{
    try
    {
        var validated = ConnectionStringValidator.Validate(connectionString, providerName);
        using var connection = CreateConnection(providerName, validated.ConnectionString);
        connection.Open();
        var provider = CreateProvider(providerName);
        provider.InitializeConnection(connection);
        ProbeProviderTargetFeatures(providerName, connection, findings);
        targetVersions[providerName] = ProviderMobilityTranslator.ParseProviderVersion(connection.ServerVersion);
    }
    catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
    {
        findings.Add(new ProviderMobilityFinding(
            "provider-target",
            0,
            "provider-target-open",
            "Error",
            $"Provider target '{providerName}' could not be opened and validated: {RedactConnectionStrings(ex.Message)}",
            "Fix the target connection string, driver, credentials, network access, or server version before claiming live provider mobility evidence."));
    }
}

static void ProbeProviderTargetFeatures(
    string providerName,
    DbConnection connection,
    ICollection<ProviderMobilityFinding> findings)
{
    var scalarProbes = providerName.ToLowerInvariant() switch
    {
        "sqlite" => new[]
        {
            ("JSON translation", "SELECT json_extract('{\"a\":1}', '$.a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY 1)")
        },
        "sqlserver" => new[]
        {
            ("JSON translation", "SELECT JSON_VALUE(N'{\"a\":1}', '$.a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1))")
        },
        "postgres" => new[]
        {
            ("JSON translation", "SELECT ('{\"a\":1}'::jsonb ->> 'a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY 1)")
        },
        "mysql" => new[]
        {
            ("JSON translation", "SELECT JSON_EXTRACT('{\"a\":1}', '$.a')"),
            ("window function", "SELECT ROW_NUMBER() OVER (ORDER BY v) FROM (SELECT 1 AS v) AS t")
        },
        _ => Array.Empty<(string Feature, string Sql)>()
    };

    foreach (var (feature, sql) in scalarProbes)
    {
        ProbeProviderTargetFeature(providerName, feature, findings, () => ExecuteProviderTargetScalar(connection, sql));
    }

    ProbeProviderTargetFeature(providerName, "savepoint", findings, () => ProbeProviderTargetSavepoint(providerName, connection));
    ProbeProviderTargetFeature(providerName, "generated-value retrieval", findings, () => ProbeProviderTargetGeneratedValue(providerName, connection));
    ProbeProviderTargetFeature(providerName, "rename column DDL", findings, () => ProbeProviderTargetRenameColumn(providerName, connection));
    ProbeProviderTargetFeature(providerName, "idempotent insert/ignore", findings, () => ProbeProviderTargetIdempotentInsert(providerName, connection));
}

static void ProbeProviderTargetFeature(
    string providerName,
    string feature,
    ICollection<ProviderMobilityFinding> findings,
    Action probe)
{
    try
    {
        probe();
    }
    catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
    {
        findings.Add(new ProviderMobilityFinding(
            "provider-target",
            0,
            "provider-target-capability",
            "Error",
            $"Provider target '{providerName}' failed the {feature} probe: {RedactConnectionStrings(ex.Message)}",
            "Enable the required database feature/extension or remove that provider target before claiming live provider mobility evidence."));
    }
}

static object? ExecuteProviderTargetScalar(DbConnection connection, string sql, DbTransaction? transaction = null)
{
    using var command = connection.CreateCommand();
    command.CommandText = sql;
    command.Transaction = transaction;
    return command.ExecuteScalar();
}

static int ExecuteProviderTargetNonQuery(DbConnection connection, string sql, DbTransaction? transaction = null)
{
    using var command = connection.CreateCommand();
    command.CommandText = sql;
    command.Transaction = transaction;
    return command.ExecuteNonQuery();
}

static string ProviderTargetProbeName()
    => "__norm_cert_" + Guid.NewGuid().ToString("N")[..16];

static string ProviderTargetTable(DatabaseProvider provider, string tableName, string? schema = null)
    => string.IsNullOrWhiteSpace(schema)
        ? provider.Escape(tableName)
        : provider.Escape(schema) + "." + provider.Escape(tableName);

static void ProbeProviderTargetSavepoint(string providerName, DbConnection connection)
{
    using var transaction = connection.BeginTransaction();
    try
    {
        if (providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteProviderTargetNonQuery(connection, "SAVE TRANSACTION norm_cert_sp", transaction);
            ExecuteProviderTargetNonQuery(connection, "ROLLBACK TRANSACTION norm_cert_sp", transaction);
        }
        else
        {
            ExecuteProviderTargetNonQuery(connection, "SAVEPOINT norm_cert_sp", transaction);
            ExecuteProviderTargetNonQuery(connection, "ROLLBACK TO SAVEPOINT norm_cert_sp", transaction);
            ExecuteProviderTargetNonQuery(connection, "RELEASE SAVEPOINT norm_cert_sp", transaction);
        }

        transaction.Rollback();
    }
    catch
    {
        try { transaction.Rollback(); } catch { }
        throw;
    }
}

static void ProbeProviderTargetGeneratedValue(string providerName, DbConnection connection)
{
    var provider = CreateProvider(providerName);
    var tableName = ProviderTargetProbeName();
    var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);

    try
    {
        switch (providerName.ToLowerInvariant())
        {
            case "sqlite":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("Id")} INTEGER PRIMARY KEY AUTOINCREMENT, {provider.Escape("Name")} TEXT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a')");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, "SELECT last_insert_rowid()"));
                break;
            case "sqlserver":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({provider.Escape("Id")} int IDENTITY(1,1) NOT NULL PRIMARY KEY, {provider.Escape("Name")} nvarchar(20) NULL)");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) OUTPUT INSERTED.{provider.Escape("Id")} VALUES (N'a')"));
                break;
            case "postgres":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("Id")} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {provider.Escape("Name")} text NULL)");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a') RETURNING {provider.Escape("Id")}"));
                break;
            case "mysql":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({provider.Escape("Id")} INT NOT NULL AUTO_INCREMENT PRIMARY KEY, {provider.Escape("Name")} varchar(20) NULL)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({provider.Escape("Name")}) VALUES ('a')");
                EnsureProviderTargetScalarValue("generated-value retrieval", ExecuteProviderTargetScalar(connection, "SELECT LAST_INSERT_ID()"));
                break;
        }
    }
    finally
    {
        DropProviderTargetProbeTable(providerName, connection, provider, tableName);
    }
}

static void ProbeProviderTargetRenameColumn(string providerName, DbConnection connection)
{
    var provider = CreateProvider(providerName);
    var tableName = ProviderTargetProbeName();
    var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);

    try
    {
        switch (providerName.ToLowerInvariant())
        {
            case "sqlite":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("OldName")} INTEGER NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                break;
            case "sqlserver":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({provider.Escape("OldName")} int NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"EXEC sp_rename N'dbo.{tableName}.OldName', N'NewName', N'COLUMN'");
                break;
            case "postgres":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({provider.Escape("OldName")} integer NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                break;
            case "mysql":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({provider.Escape("OldName")} INT NOT NULL)");
                ExecuteProviderTargetNonQuery(connection, $"ALTER TABLE {table} RENAME COLUMN {provider.Escape("OldName")} TO {provider.Escape("NewName")}");
                break;
        }
    }
    finally
    {
        DropProviderTargetProbeTable(providerName, connection, provider, tableName);
    }
}

static void ProbeProviderTargetIdempotentInsert(string providerName, DbConnection connection)
{
    var provider = CreateProvider(providerName);
    var tableName = ProviderTargetProbeName();
    var table = ProviderTargetTable(provider, tableName, providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) ? "dbo" : null);
    var code = provider.Escape("Code");

    try
    {
        switch (providerName.ToLowerInvariant())
        {
            case "sqlite":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({code} INTEGER NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT OR IGNORE INTO {table} ({code}) VALUES (1)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT OR IGNORE INTO {table} ({code}) VALUES (1)");
                break;
            case "sqlserver":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TABLE {table} ({code} int NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {code} = 1) INSERT INTO {table} ({code}) VALUES (1)");
                ExecuteProviderTargetNonQuery(connection, $"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {code} = 1) INSERT INTO {table} ({code}) VALUES (1)");
                break;
            case "postgres":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMP TABLE {table} ({code} integer NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({code}) VALUES (1) ON CONFLICT DO NOTHING");
                ExecuteProviderTargetNonQuery(connection, $"INSERT INTO {table} ({code}) VALUES (1) ON CONFLICT DO NOTHING");
                break;
            case "mysql":
                ExecuteProviderTargetNonQuery(connection, $"CREATE TEMPORARY TABLE {table} ({code} INT NOT NULL UNIQUE)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT IGNORE INTO {table} ({code}) VALUES (1)");
                ExecuteProviderTargetNonQuery(connection, $"INSERT IGNORE INTO {table} ({code}) VALUES (1)");
                break;
        }

        var count = Convert.ToInt64(ExecuteProviderTargetScalar(connection, $"SELECT COUNT(*) FROM {table}") ?? 0);
        if (count != 1)
            throw new InvalidOperationException($"Idempotent insert probe expected one row but found {count}.");
    }
    finally
    {
        DropProviderTargetProbeTable(providerName, connection, provider, tableName);
    }
}

static void EnsureProviderTargetScalarValue(string feature, object? value)
{
    if (value is null || value is DBNull)
        throw new InvalidOperationException($"The {feature} probe returned no value.");
}

static void DropProviderTargetProbeTable(string providerName, DbConnection connection, DatabaseProvider provider, string tableName)
{
    try
    {
        if (providerName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteProviderTargetNonQuery(
                connection,
                $"IF OBJECT_ID(N'dbo.{tableName}', N'U') IS NOT NULL DROP TABLE {ProviderTargetTable(provider, tableName, "dbo")}");
            return;
        }

        var table = ProviderTargetTable(provider, tableName);
        if (providerName.Equals("mysql", StringComparison.OrdinalIgnoreCase))
            ExecuteProviderTargetNonQuery(connection, $"DROP TEMPORARY TABLE IF EXISTS {table}");
        else
            ExecuteProviderTargetNonQuery(connection, $"DROP TABLE IF EXISTS {table}");
    }
    catch
    {
        // Best-effort cleanup; the probe failure path reports the original feature error.
    }
}

/// <summary>
/// Resolves the build output assembly path for a project by running
/// <c>dotnet msbuild -getProperty:TargetPath</c> on the project file.
/// Returns null if the path cannot be determined.
/// </summary>
static string? ResolveAssemblyFromProject(
    string projectPath,
    string? targetFramework = null,
    string? configuration = null,
    string? runtime = null)
{
    try
    {
        var startInfo = new System.Diagnostics.ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        startInfo.ArgumentList.Add("msbuild");
        startInfo.ArgumentList.Add(projectPath);
        AddMSBuildProperty(startInfo, "TargetFramework", targetFramework);
        AddMSBuildProperty(startInfo, "Configuration", configuration);
        AddMSBuildProperty(startInfo, "RuntimeIdentifier", runtime);
        startInfo.ArgumentList.Add("-getProperty:TargetPath");
        startInfo.ArgumentList.Add("--verbosity:quiet");

        using var proc = System.Diagnostics.Process.Start(startInfo);
        if (proc == null) return null;
        var output = proc.StandardOutput.ReadToEnd().Trim();
        proc.WaitForExit();
        if (proc.ExitCode == 0 && !string.IsNullOrWhiteSpace(output) && File.Exists(output))
            return output;
        return null;
    }
    catch
    {
        return null;
    }
}

static void AddMSBuildProperty(System.Diagnostics.ProcessStartInfo startInfo, string propertyName, string? value)
{
    if (!string.IsNullOrWhiteSpace(value))
        startInfo.ArgumentList.Add($"-property:{propertyName}={value}");
}

sealed record DesignTimeDependencyProbePaths(
    IReadOnlyDictionary<string, IReadOnlyList<string>> Assemblies,
    IReadOnlyDictionary<string, IReadOnlyList<string>> NativeLibraries)
{
    public static DesignTimeDependencyProbePaths Empty { get; } = new(
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase));
}

sealed class DesignTimeEnvironmentScope : IDisposable
{
    private readonly bool _changed;
    private readonly string? _previousAspNetCoreEnvironment;
    private readonly string? _previousDotnetEnvironment;

    private DesignTimeEnvironmentScope(string? environment)
    {
        if (string.IsNullOrWhiteSpace(environment))
            return;

        _changed = true;
        _previousAspNetCoreEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        _previousDotnetEnvironment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", environment);
        Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", environment);
    }

    public static DesignTimeEnvironmentScope Apply(string? environment) => new(environment);

    public void Dispose()
    {
        if (!_changed)
            return;

        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", _previousAspNetCoreEnvironment);
        Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", _previousDotnetEnvironment);
    }
}

sealed class DesignTimeAssemblyLoadContext : AssemblyLoadContext
{
    private readonly AssemblyDependencyResolver _resolver;
    private readonly IReadOnlyList<string> _probeDirectories;
    private readonly DesignTimeDependencyProbePaths _dependencyProbePaths;

    public DesignTimeAssemblyLoadContext(
        string mainAssemblyPath,
        IReadOnlyList<string> probeDirectories,
        DesignTimeDependencyProbePaths dependencyProbePaths)
    {
        _resolver = new AssemblyDependencyResolver(mainAssemblyPath);
        _probeDirectories = probeDirectories;
        _dependencyProbePaths = dependencyProbePaths;
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var sharedAssembly = AssemblyLoadContext.Default.Assemblies.FirstOrDefault(assembly =>
            AssemblyName.ReferenceMatchesDefinition(assembly.GetName(), assemblyName));
        if (sharedAssembly != null)
            return sharedAssembly;

        var assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
        assemblyPath ??= ResolveAssemblyFromProbeDirectories(assemblyName);
        return assemblyPath == null ? null : LoadFromAssemblyPath(assemblyPath);
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        var libraryPath = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        libraryPath ??= ResolveNativeLibraryFromDeps(unmanagedDllName);
        return libraryPath == null ? IntPtr.Zero : LoadUnmanagedDllFromPath(libraryPath);
    }

    private string? ResolveAssemblyFromProbeDirectories(AssemblyName assemblyName)
    {
        if (string.IsNullOrWhiteSpace(assemblyName.Name))
            return null;

        if (_dependencyProbePaths.Assemblies.TryGetValue(assemblyName.Name, out var dependencyPaths))
        {
            foreach (var dependencyPath in dependencyPaths)
            {
                if (File.Exists(dependencyPath))
                    return dependencyPath;
            }
        }

        foreach (var directory in _probeDirectories)
        {
            var candidate = Path.Combine(directory, assemblyName.Name + ".dll");
            if (File.Exists(candidate))
                return candidate;

            var refsCandidate = Path.Combine(directory, "refs", assemblyName.Name + ".dll");
            if (File.Exists(refsCandidate))
                return refsCandidate;
        }

        return null;
    }

    private string? ResolveNativeLibraryFromDeps(string unmanagedDllName)
    {
        foreach (var key in GetNativeLibraryResolverKeys(unmanagedDllName))
        {
            if (!_dependencyProbePaths.NativeLibraries.TryGetValue(key, out var dependencyPaths))
                continue;

            foreach (var dependencyPath in dependencyPaths)
            {
                if (File.Exists(dependencyPath))
                    return dependencyPath;
            }
        }

        return null;
    }

    private static IEnumerable<string> GetNativeLibraryResolverKeys(string unmanagedDllName)
    {
        yield return unmanagedDllName;

        var fileName = Path.GetFileName(unmanagedDllName);
        if (!string.IsNullOrWhiteSpace(fileName) &&
            !string.Equals(fileName, unmanagedDllName, StringComparison.OrdinalIgnoreCase))
        {
            yield return fileName;
        }

        var stem = Path.GetFileNameWithoutExtension(unmanagedDllName);
        if (!string.IsNullOrWhiteSpace(stem) &&
            !string.Equals(stem, unmanagedDllName, StringComparison.OrdinalIgnoreCase))
        {
            yield return stem;
        }
    }
}
