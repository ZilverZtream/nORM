using System;
using System.CommandLine;
using System.Data.Common;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Migration;
using nORM.Providers;
using nORM.Scaffolding;

var root = new RootCommand("nORM command line tools");

var scaffold = new Command("scaffold", "Scaffold entity classes and a DbContext from an existing database");
var connOpt = new Option<string>("--connection", description: "Connection string") { IsRequired = true };
var providerOpt = new Option<string>("--provider", description: "Database provider (sqlserver, sqlite)") { IsRequired = true };
var outputOpt = new Option<string>("--output", () => ".", "Output directory for generated code");
var nsOpt = new Option<string>("--namespace", () => "Scaffolded", "Namespace for generated classes");
var ctxOpt = new Option<string>("--context", () => "AppDbContext", "DbContext class name");
scaffold.AddOption(connOpt);
scaffold.AddOption(providerOpt);
scaffold.AddOption(outputOpt);
scaffold.AddOption(nsOpt);
scaffold.AddOption(ctxOpt);

scaffold.SetHandler(async (conn, prov, output, ns, ctx) =>
{
    using var connection = CreateConnection(prov, conn);
    var provider = CreateProvider(prov);
    await DatabaseScaffolder.ScaffoldAsync(connection, provider, output!, ns!, ctx!);
    Console.WriteLine($"Scaffolding completed. Files written to {output}.");
}, connOpt, providerOpt, outputOpt, nsOpt, ctxOpt);

root.AddCommand(scaffold);

var database = new Command("database", "Database related commands");
var update = new Command("update", "Apply pending migrations to the database");
var migConnOpt = new Option<string>("--connection", description: "Connection string") { IsRequired = true };
var migProvOpt = new Option<string>("--provider", description: "Database provider (sqlserver)") { IsRequired = true };
var assemblyOpt = new Option<string>("--assembly", description: "Path to migrations assembly") { IsRequired = true };
update.AddOption(migConnOpt);
update.AddOption(migProvOpt);
update.AddOption(assemblyOpt);
update.SetHandler(async (conn, prov, asmPath) =>
{
    using var connection = CreateConnection(prov, conn);
    var assembly = Assembly.LoadFrom(asmPath!);
    IMigrationRunner runner = prov.ToLowerInvariant() switch
    {
        "sqlserver" => new SqlServerMigrationRunner(connection, assembly),
        _ => throw new ArgumentException($"Provider '{prov}' does not support migrations.")
    };

    if (!await runner.HasPendingMigrationsAsync())
    {
        Console.WriteLine("No pending migrations found.");
        return;
    }

    await runner.ApplyMigrationsAsync();
    Console.WriteLine("Migrations applied successfully.");
}, migConnOpt, migProvOpt, assemblyOpt);

database.AddCommand(update);
root.AddCommand(database);

return await root.InvokeAsync(args);

static DbConnection CreateConnection(string provider, string connectionString)
    => provider.ToLowerInvariant() switch
    {
        "sqlserver" => new SqlConnection(connectionString),
        "sqlite" => new SqliteConnection(connectionString),
        _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
    };

static DatabaseProvider CreateProvider(string provider)
    => provider.ToLowerInvariant() switch
    {
        "sqlserver" => new SqlServerProvider(),
        "sqlite" => new SqliteProvider(),
        _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
    };
