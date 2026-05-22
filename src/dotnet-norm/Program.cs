using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
var scaffold = new Command("scaffold", "Preview: scaffold entity classes and a DbContext from an existing database.\nExample:\n  norm scaffold --connection \"Server=.;Database=AppDb;Trusted_Connection=True;\" --provider sqlserver --output Models");
var connOpt = new Option<string>("--connection") { Description = "Database connection string. e.g. 'Server=.;Database=AppDb;Trusted_Connection=True;'", Required = true };
var providerOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql)", Required = true };
var outputOpt = new Option<string>("--output") { Description = "Output directory for generated code", DefaultValueFactory = _ => "." };
var nsOpt = new Option<string>("--namespace") { Description = "Namespace for generated classes", DefaultValueFactory = _ => "Scaffolded" };
var ctxOpt = new Option<string>("--context") { Description = "DbContext class name", DefaultValueFactory = _ => "AppDbContext" };
scaffold.Add(connOpt);
scaffold.Add(providerOpt);
scaffold.Add(outputOpt);
scaffold.Add(nsOpt);
scaffold.Add(ctxOpt);
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
        await DatabaseScaffolder.ScaffoldAsync(connection, provider, output, ns, ctx);
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
        var assembly = Assembly.LoadFrom(asmPath);
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

// migrations add
var migrations = new Command("migrations", "Migration management commands");
var add = new Command("add", "Add a new migration based on model changes.\nExample:\n  norm migrations add InitialCreate --provider sqlserver --assembly App.dll");
var migNameArg = new Argument<string>("name") { Description = "Migration name" };
var addProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql)", Required = true };
var addAsmOpt = new Option<string>("--assembly") { Description = "Path to assembly containing DbContext and entities", Required = true };
var addOutOpt = new Option<string>("--output") { Description = "Output directory for migrations", DefaultValueFactory = _ => "Migrations" };
var addForceOpt = new Option<bool>("--force") { Description = "Allow destructive table/column drops when generating the migration." };
var addAttributeOnlyOpt = new Option<bool>("--attribute-only") { Description = "Generate from attribute-only entity scanning instead of a design-time DbContext." };
add.Add(migNameArg);
add.Add(addProvOpt);
add.Add(addAsmOpt);
add.Add(addOutOpt);
add.Add(addForceOpt);
add.Add(addAttributeOnlyOpt);
add.SetAction((ParseResult result) =>
{
    try
    {
        var name = result.GetValue(migNameArg)!;
        var prov = result.GetValue(addProvOpt)!;
        var asmPath = result.GetValue(addAsmOpt)!;
        var output = result.GetValue(addOutOpt)!;
        var force = result.GetValue(addForceOpt);
        var attributeOnly = result.GetValue(addAttributeOnlyOpt);
        if (!File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return 2;
        }
        var assembly = Assembly.LoadFrom(asmPath);

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

static int Fail(Exception ex, int exitCode = 1)
{
    Console.Error.WriteLine($"Error: {ex.Message}");
    return exitCode;
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
