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

// scaffold command
var scaffold = new Command("scaffold", "Scaffold entity classes and a DbContext from an existing database.\nExample:\n  norm scaffold --connection \"Server=.;Database=AppDb;Trusted_Connection=True;\" --provider sqlserver --output Models");
var connOpt = new Option<string>("--connection") { Description = "Database connection string. e.g. 'Server=.;Database=AppDb;Trusted_Connection=True;'", Required = true };
var providerOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql)", Required = true };
var outputOpt = new Option<string>("--output") { Description = "Output directory for generated code", DefaultValueFactory = _ => "." };
var nsOpt = new Option<string>("--namespace") { Description = "Namespace for generated classes", DefaultValueFactory = _ => "Scaffolded" };
var ctxOpt = new Option<string>("--context") { Description = "DbContext class name", DefaultValueFactory = _ => "AppDbContext" };
var tablesOpt = new Option<string?>("--tables") { Description = "Optional comma-separated table filter. Entries may be table or schema.table names." };
var noOverwriteOpt = new Option<bool>("--no-overwrite") { Description = "Refuse to overwrite existing generated files." };
var failOnWarningsOpt = new Option<bool>("--fail-on-warnings") { Description = "Fail scaffolding when unsupported schema features are reported in nORM.ScaffoldWarnings.md/json." };
scaffold.Add(connOpt);
scaffold.Add(providerOpt);
scaffold.Add(outputOpt);
scaffold.Add(nsOpt);
scaffold.Add(ctxOpt);
scaffold.Add(tablesOpt);
scaffold.Add(noOverwriteOpt);
scaffold.Add(failOnWarningsOpt);
scaffold.SetAction(async (ParseResult result, CancellationToken _) =>
{
    try
    {
        var prov = result.GetValue(providerOpt)!;
        var validated = ConnectionStringValidator.Validate(result.GetValue(connOpt)!, prov);
        var output = result.GetValue(outputOpt)!;
        var ns = result.GetValue(nsOpt)!;
        var ctx = result.GetValue(ctxOpt)!;
        using var connection = CreateConnection(prov, validated.ConnectionString);
        await connection.OpenAsync();
        var provider = CreateProvider(prov);
        var options = new ScaffoldOptions
        {
            Tables = ParseCsvList(result.GetValue(tablesOpt)),
            OverwriteFiles = !result.GetValue(noOverwriteOpt),
            FailOnWarnings = result.GetValue(failOnWarningsOpt)
        };
        await DatabaseScaffolder.ScaffoldAsync(connection, provider, output, ns, ctx, options);
        Console.WriteLine($"Scaffolding completed. Files written to {output}.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});
root.Add(scaffold);

// database update/drop commands
var database = new Command("database", "Database related commands");

var update = new Command("update", "Apply pending migrations to the database.\nExample:\n  norm database update --connection \"...\" --provider sqlserver --assembly Migrations.dll");
var migConnOpt = new Option<string>("--connection") { Description = "Database connection string", Required = true };
var migProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql)", Required = true };
var assemblyOpt = new Option<string>("--assembly") { Description = "Path to migrations assembly (e.g. ./bin/Debug/net8.0/App.Migrations.dll)", Required = true };
update.Add(migConnOpt);
update.Add(migProvOpt);
update.Add(assemblyOpt);
update.SetAction(async (ParseResult result, CancellationToken _) =>
{
    try
    {
        var prov = result.GetValue(migProvOpt)!;
        var validated = ConnectionStringValidator.Validate(result.GetValue(migConnOpt)!, prov);
        var asmPath = result.GetValue(assemblyOpt)!;
        if (!File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return 2;
        }
        using var connection = CreateConnection(prov, validated.ConnectionString);
        await connection.OpenAsync();
        var assembly = LoadDesignTimeAssembly(asmPath);
        IMigrationRunner runner = prov.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlServerMigrationRunner(connection, assembly),
            "sqlite" => new SqliteMigrationRunner(connection, assembly),
            "postgres" or "postgresql" => new PostgresMigrationRunner(connection, assembly),
            "mysql" => new MySqlMigrationRunner(connection, assembly),
            _ => throw new ArgumentException($"Provider '{prov}' does not support migrations.")
        };
        if (!await runner.HasPendingMigrationsAsync())
        {
            Console.WriteLine("No pending migrations found.");
            return 0;
        }
        await runner.ApplyMigrationsAsync();
        Console.WriteLine("Migrations applied successfully.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});

var drop = new Command("drop", "Drop the target database or all tables. Useful for resetting test databases.\nExample:\n  norm database drop --connection \"...\" --provider postgres");
var dropConnOpt = new Option<string>("--connection") { Description = "Database connection string", Required = true };
var dropProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql)", Required = true };
var dropYesOpt = new Option<bool>("--yes") { Description = "Confirm destructive deletion. Required unless --dry-run is used." };
var dropDryRunOpt = new Option<bool>("--dry-run") { Description = "Print the objects that would be dropped without deleting anything." };
drop.Add(dropConnOpt);
drop.Add(dropProvOpt);
drop.Add(dropYesOpt);
drop.Add(dropDryRunOpt);
drop.SetAction(async (ParseResult result, CancellationToken _) =>
{
    try
    {
        var prov = result.GetValue(dropProvOpt)!;
        var validated = ConnectionStringValidator.Validate(result.GetValue(dropConnOpt)!, prov);
        var yes = result.GetValue(dropYesOpt);
        var dryRun = result.GetValue(dropDryRunOpt);
        if (!yes && !dryRun)
        {
            Console.Error.WriteLine("Refusing to run destructive database drop without --yes. Use --dry-run to preview.");
            return 3;
        }

        if (prov.Equals("sqlite", StringComparison.OrdinalIgnoreCase))
        {
            var builder = new SqliteConnectionStringBuilder(validated.ConnectionString);
            var file = builder.DataSource;
            if (dryRun)
            {
                Console.WriteLine(File.Exists(file)
                    ? $"Would delete SQLite database file '{file}'."
                    : $"SQLite database file '{file}' does not exist.");
                return 0;
            }

            if (File.Exists(file))
            {
                File.Delete(file);
                Console.WriteLine($"Database '{file}' deleted.");
            }
            else
            {
                Console.WriteLine($"Database file '{file}' not found.");
            }
            return 0;
        }

        using var connection = CreateConnection(prov, validated.ConnectionString);
        await connection.OpenAsync();
        if (IsProtectedDatabaseName(prov, connection.Database))
        {
            Console.Error.WriteLine($"Refusing to drop protected {prov} database '{connection.Database}'.");
            return 4;
        }

        var provider = CreateProvider(prov);
        var schema = connection.GetSchema("Tables");
        foreach (DataRow row in schema.Rows)
        {
            var tableSchema = row["TABLE_SCHEMA"]?.ToString();
            var tableName = row["TABLE_NAME"]?.ToString();
            if (string.IsNullOrEmpty(tableName)) continue;
            // Exclude system schemas to avoid accidentally dropping provider-internal objects.
            if (IsSystemSchema(prov, tableSchema))
                continue;
            var full = string.IsNullOrEmpty(tableSchema)
                ? provider.Escape(tableName)
                : $"{provider.Escape(tableSchema!)}.{provider.Escape(tableName!)}";
            if (dryRun)
            {
                Console.WriteLine($"Would drop table {full}");
                continue;
            }

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE {full}";
            try { await cmd.ExecuteNonQueryAsync(); }
            catch (DbException ex) { Console.Error.WriteLine($"  Warning: DROP TABLE {full} failed: {ex.Message}"); }
        }
        Console.WriteLine(dryRun ? "Dry run completed." : "Database dropped successfully.");
        return 0;
    }
    catch (Exception ex)
    {
        return Fail(ex);
    }
});

database.Add(update);
database.Add(drop);
root.Add(database);

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
            schemaSnapshot = BuildMigrationSnapshot(assembly, result.GetValue(portabilityAttributeOnlyOpt));
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
var addProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql)", Required = true };
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
var addTargetFrameworkOpt = new Option<string?>("--target-framework") { Description = "Target framework moniker (e.g., net8.0) used when resolving the output assembly from --project." };
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
add.SetAction((ParseResult result) =>
{
    try
    {
        var name = result.GetValue(migNameArg)!;
        var prov = result.GetValue(addProvOpt)!;
        var asmPath = result.GetValue(addAsmOpt);
        var output = result.GetValue(addOutOpt)!;
        var force = result.GetValue(addForceOpt);
        var attributeOnly = result.GetValue(addAttributeOnlyOpt);
        var projectPath = result.GetValue(addProjectOpt);
        var startupProjectPath = result.GetValue(addStartupProjectOpt);
        var depsPath = result.GetValue(addDepsOpt);
        var runtimeConfigPath = result.GetValue(addRuntimeConfigOpt);
        var targetFramework = result.GetValue(addTargetFrameworkOpt);

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
            asmPath = ResolveAssemblyFromProject(effectiveProject, targetFramework);
            if (asmPath == null)
            {
                Console.Error.WriteLine($"Could not determine output assembly from project '{projectPath}'. Build the project first or supply --assembly explicitly.");
                return 2;
            }
        }

        if (string.IsNullOrEmpty(asmPath) || !File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return 2;
        }
        var assembly = LoadDesignTimeAssembly(asmPath!);

        var snapshotPath = Path.Combine(output, "schema.snapshot.json");
        SchemaSnapshot oldSnap = File.Exists(snapshotPath)
            ? JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(snapshotPath)) ?? new SchemaSnapshot()
            : new SchemaSnapshot();

        var newSnap = BuildMigrationSnapshot(assembly, attributeOnly);
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
            Console.Error.WriteLine("Re-run with --force after replacing rename-like drops/adds with explicit rename operations or after accepting the data loss.");
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

return await root.Parse(args).InvokeAsync(new InvocationConfiguration());

static Assembly LoadDesignTimeAssembly(string assemblyPath)
{
    var fullPath = Path.GetFullPath(assemblyPath);
    var loadContext = new DesignTimeAssemblyLoadContext(fullPath);
    return loadContext.LoadFromAssemblyPath(fullPath);
}

static SchemaSnapshot BuildMigrationSnapshot(Assembly assembly, bool attributeOnly)
{
    var factory = FindDesignTimeFactory(assembly);
    if (factory != null)
    {
        using var ctx = CreateDesignTimeContext(factory.Value.FactoryType, factory.Value.InterfaceType);
        Console.WriteLine($"Using design-time DbContext factory {factory.Value.FactoryType.FullName}.");
        return SchemaSnapshotBuilder.Build(ctx);
    }

    if (attributeOnly)
    {
        Console.WriteLine("Using attribute-only model snapshot.");
        return SchemaSnapshotBuilder.Build(assembly);
    }

    var ctxType = assembly.GetTypes()
        .FirstOrDefault(t => t.IsClass && !t.IsAbstract && typeof(DbContext).IsAssignableFrom(t));
    if (ctxType == null)
    {
        throw new InvalidOperationException(
            "No DbContext type was found. Add an INormDesignTimeDbContextFactory<TContext> implementation " +
            "or re-run with --attribute-only to generate from attributes only.");
    }

    try
    {
        using var modelCn = new SqliteConnection("Data Source=:memory:");
        modelCn.Open();
        var provider = new SqliteProvider();
        using var modelCtx = (DbContext)Activator.CreateInstance(ctxType, modelCn, provider)!;
        Console.WriteLine($"Using fluent model from {ctxType.Name}.");
        return SchemaSnapshotBuilder.Build(modelCtx);
    }
    catch (Exception ex) when (ex is MissingMethodException or TargetInvocationException or InvalidOperationException or MemberAccessException)
    {
        throw new InvalidOperationException(
            $"Could not instantiate DbContext type '{ctxType.FullName}' for migration generation. " +
            "Add an INormDesignTimeDbContextFactory<TContext> implementation or re-run with --attribute-only " +
            "if you intentionally want to ignore fluent model configuration.",
            ex);
    }
}

static (Type FactoryType, Type InterfaceType)? FindDesignTimeFactory(Assembly assembly)
{
    foreach (var type in assembly.GetTypes().Where(static t => t.IsClass && !t.IsAbstract))
    {
        var interfaceType = type.GetInterfaces().FirstOrDefault(static i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(INormDesignTimeDbContextFactory<>));
        if (interfaceType != null)
            return (type, interfaceType);
    }

    return null;
}

static DbContext CreateDesignTimeContext(Type factoryType, Type interfaceType)
{
    var factory = Activator.CreateInstance(factoryType)
        ?? throw new InvalidOperationException($"Could not create design-time factory '{factoryType.FullName}'.");
    var method = interfaceType.GetMethod(nameof(INormDesignTimeDbContextFactory<DbContext>.CreateDbContext))
        ?? throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' does not expose CreateDbContext.");
    var context = method.Invoke(factory, new object[] { Array.Empty<string>() })
        ?? throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' returned null.");
    if (context is not DbContext dbContext)
        throw new InvalidOperationException($"Design-time factory '{factoryType.FullName}' did not return a nORM DbContext.");
    return dbContext;
}

static DbConnection CreateConnection(string provider, string connectionString)
{
    try
    {
        return provider.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlConnection(connectionString),
            "sqlite" => new SqliteConnection(connectionString),
            "postgres" or "postgresql" => CreateConnectionFromType(new[] { "Npgsql.NpgsqlConnection, Npgsql" }, "PostgreSQL", connectionString),
            "mysql" => CreateConnectionFromType(new[] { "MySql.Data.MySqlClient.MySqlConnection, MySql.Data", "MySqlConnector.MySqlConnection, MySqlConnector" }, "MySQL", connectionString),
            _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
        };
    }
    catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or TypeLoadException)
    {
        throw new InvalidOperationException($"Failed to create connection: {ex.Message}", ex);
    }
}

static DbConnection CreateConnectionFromType(string[] typeNames, string friendly, string connString)
{
    foreach (var name in typeNames)
    {
        var type = Type.GetType(name);
        if (type != null)
        {
            return (DbConnection)Activator.CreateInstance(type, connString)!;
        }
    }
    throw new InvalidOperationException($"{friendly} provider assembly not found. Ensure the appropriate package is referenced.");
}

static DatabaseProvider CreateProvider(string provider) =>
    provider.ToLowerInvariant() switch
    {
        "sqlserver" => new SqlServerProvider(),
        "sqlite" => new SqliteProvider(),
        "postgres" or "postgresql" => new PostgresProvider(),
        "mysql" => new MySqlProvider(),
        _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
    };

static IReadOnlyCollection<string> ParseCsvList(string? value)
    => string.IsNullOrWhiteSpace(value)
        ? Array.Empty<string>()
        : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

static bool IsSystemSchema(string provider, string? schemaName)
{
    if (string.IsNullOrWhiteSpace(schemaName))
        return false;

    var s = schemaName.Trim();
    // Universal system schemas present in multiple providers.
    if (s.Equals("sys", StringComparison.OrdinalIgnoreCase)
        || s.Equals("information_schema", StringComparison.OrdinalIgnoreCase))
        return true;

    return provider.ToLowerInvariant() switch
    {
        // PostgreSQL system schemas: pg_catalog, pg_toast, pg_temp_*, pg_toast_temp_*, pg_*.
        "postgres" or "postgresql" =>
            s.StartsWith("pg_", StringComparison.OrdinalIgnoreCase),
        // MySQL: the mysql system database tables appear under the "mysql" schema.
        "mysql" =>
            s.Equals("mysql", StringComparison.OrdinalIgnoreCase)
            || s.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
}

static bool IsProtectedDatabaseName(string provider, string databaseName)
{
    if (string.IsNullOrWhiteSpace(databaseName))
        return false;

    var normalized = databaseName.Trim();
    return provider.ToLowerInvariant() switch
    {
        "sqlserver" => normalized.Equals("master", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("model", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("msdb", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
        "postgres" or "postgresql" => normalized.Equals("postgres", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("template0", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("template1", StringComparison.OrdinalIgnoreCase),
        "mysql" => normalized.Equals("mysql", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("sys", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
}

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
    => providerName.Trim().ToLowerInvariant() switch
    {
        "mssql" => "sqlserver",
        "postgresql" => "postgres",
        "mariadb" => "mysql",
        var normalized => normalized
    };

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
    var probes = providerName.ToLowerInvariant() switch
    {
        "sqlite" => new[] { ("JSON translation", "SELECT json_extract('{\"a\":1}', '$.a')") },
        "sqlserver" => new[] { ("JSON translation", "SELECT JSON_VALUE(N'{\"a\":1}', '$.a')") },
        "postgres" => new[] { ("JSON translation", "SELECT ('{\"a\":1}'::jsonb ->> 'a')") },
        "mysql" => new[] { ("JSON translation", "SELECT JSON_EXTRACT('{\"a\":1}', '$.a')") },
        _ => Array.Empty<(string Feature, string Sql)>()
    };

    foreach (var (feature, sql) in probes)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            _ = command.ExecuteScalar();
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
}

static int Fail(Exception ex, int exitCode = 1)
{
    Console.Error.WriteLine($"Error: {RedactConnectionStrings(ex.Message)}");
    return exitCode;
}

/// <summary>
/// Replaces any recognizable connection string segments containing sensitive key=value pairs
/// with [REDACTED] so that passwords and secrets are not printed to stderr on error.
/// </summary>
static string RedactConnectionStrings(string message)
{
    if (string.IsNullOrEmpty(message))
        return message;

    // Sensitive keys whose values must be redacted (case-insensitive).
    var sensitiveKeys = new[] { "password", "pwd", "user password", "access token", "accesstoken", "token", "secret" };

    // Replace patterns of the form  key=somevalue;  or  key=somevalue (end of string).
    // Use a simple approach: for each sensitive key, replace the value portion.
    var result = message;
    foreach (var key in sensitiveKeys)
    {
        var pattern = key + "=";
        int idx = 0;
        while (true)
        {
            var pos = result.IndexOf(pattern, idx, StringComparison.OrdinalIgnoreCase);
            if (pos < 0) break;
            var valStart = pos + pattern.Length;
            var valEnd = result.IndexOf(';', valStart);
            if (valEnd < 0) valEnd = result.Length;
            result = result.Substring(0, valStart) + "[REDACTED]" + result.Substring(valEnd);
            idx = valStart + "[REDACTED]".Length;
        }
    }
    return result;
}

/// <summary>
/// Resolves the build output assembly path for a project by running
/// <c>dotnet msbuild -getProperty:TargetPath</c> on the project file.
/// Returns null if the path cannot be determined.
/// </summary>
static string? ResolveAssemblyFromProject(string projectPath, string? targetFramework = null)
{
    try
    {
        // When a target framework is supplied, scope the MSBuild evaluation so multi-TFM
        // projects resolve the correct output assembly.
        var tfmArg = string.IsNullOrEmpty(targetFramework)
            ? string.Empty
            : $" -property:TargetFramework={targetFramework}";
        var startInfo = new System.Diagnostics.ProcessStartInfo("dotnet",
            $"msbuild \"{projectPath}\"{tfmArg} -getProperty:TargetPath --verbosity:quiet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
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

static string ToCSharpIdentifier(string value)
{
    var builder = new StringBuilder(value.Length);
    for (var i = 0; i < value.Length; i++)
    {
        var ch = value[i];
        builder.Append(i == 0
            ? (char.IsLetter(ch) || ch == '_' ? ch : '_')
            : (char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_'));
    }

    return builder.Length == 0 ? "_" : builder.ToString();
}

sealed class DesignTimeAssemblyLoadContext : AssemblyLoadContext
{
    private readonly AssemblyDependencyResolver _resolver;

    public DesignTimeAssemblyLoadContext(string mainAssemblyPath)
    {
        _resolver = new AssemblyDependencyResolver(mainAssemblyPath);
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var sharedAssembly = AssemblyLoadContext.Default.Assemblies.FirstOrDefault(assembly =>
            AssemblyName.ReferenceMatchesDefinition(assembly.GetName(), assemblyName));
        if (sharedAssembly != null)
            return sharedAssembly;

        var assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
        return assemblyPath == null ? null : LoadFromAssemblyPath(assemblyPath);
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        var libraryPath = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        return libraryPath == null ? IntPtr.Zero : LoadUnmanagedDllFromPath(libraryPath);
    }
}
