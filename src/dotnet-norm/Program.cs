using System;
using System.CommandLine;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Migration;
using nORM.Providers;
using nORM.Scaffolding;

var root = new RootCommand("Command line tools for the nORM ORM framework");

var scaffold = new Command("scaffold", "Scaffold entity classes and a DbContext from an existing database.\nExample:\n  norm scaffold --connection \"Server=.;Database=AppDb;Trusted_Connection=True;\" --provider sqlserver --output Models");
var connOpt = new Option<string>("--connection", description: "Database connection string. e.g. 'Server=.;Database=AppDb;Trusted_Connection=True;'") { IsRequired = true };
var providerOpt = new Option<string>("--provider", description: "Database provider (sqlserver, sqlite, postgres, mysql)") { IsRequired = true };
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
    try
    {
        using var connection = CreateConnection(prov, conn);
        await connection.OpenAsync();
        var provider = CreateProvider(prov);
        await DatabaseScaffolder.ScaffoldAsync(connection, provider, output!, ns!, ctx!);
        Console.WriteLine($"Scaffolding completed. Files written to {output}.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
    }
}, connOpt, providerOpt, outputOpt, nsOpt, ctxOpt);

root.AddCommand(scaffold);

var database = new Command("database", "Database related commands");
var update = new Command("update", "Apply pending migrations to the database.\nExample:\n  norm database update --connection \"...\" --provider sqlserver --assembly Migrations.dll");
var migConnOpt = new Option<string>("--connection", "Database connection string") { IsRequired = true };
var migProvOpt = new Option<string>("--provider", "Database provider. Currently only 'sqlserver' is supported for applying migrations.") { IsRequired = true };
var assemblyOpt = new Option<string>("--assembly", "Path to migrations assembly (e.g. ./bin/Debug/net8.0/App.Migrations.dll)") { IsRequired = true };
update.AddOption(migConnOpt);
update.AddOption(migProvOpt);
update.AddOption(assemblyOpt);
update.SetHandler(async (conn, prov, asmPath) =>
{
    try
    {
        if (!File.Exists(asmPath!))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return;
        }
        using var connection = CreateConnection(prov, conn);
        await connection.OpenAsync();
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
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
    }
}, migConnOpt, migProvOpt, assemblyOpt);

var drop = new Command("drop", "Drop the target database or all tables. Useful for resetting test databases.\nExample:\n  norm database drop --connection \"...\" --provider postgres");
var dropConnOpt = new Option<string>("--connection", "Database connection string") { IsRequired = true };
var dropProvOpt = new Option<string>("--provider", "Database provider (sqlserver, sqlite, postgres, mysql)") { IsRequired = true };
drop.AddOption(dropConnOpt);
drop.AddOption(dropProvOpt);
drop.SetHandler(async (conn, prov) =>
{
    try
    {
        if (prov.Equals("sqlite", StringComparison.OrdinalIgnoreCase))
        {
            var builder = new SqliteConnectionStringBuilder(conn);
            var file = builder.DataSource;
            if (File.Exists(file))
            {
                File.Delete(file);
                Console.WriteLine($"Database '{file}' deleted.");
            }
            else
            {
                Console.WriteLine($"Database file '{file}' not found.");
            }
            return;
        }

        using var connection = CreateConnection(prov, conn);
        await connection.OpenAsync();
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
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE {full}";
            try
            {
                await cmd.ExecuteNonQueryAsync();
            }
            catch (DbException)
            {
                // ignore failures due to dependencies
            }
        }
        Console.WriteLine("Database dropped successfully.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
    }
}, dropConnOpt, dropProvOpt);

database.AddCommand(update);
database.AddCommand(drop);
root.AddCommand(database);

var migrations = new Command("migrations", "Migration management commands");
var add = new Command("add", "Add a new migration based on model changes.\nExample:\n  norm migrations add InitialCreate --provider sqlserver --assembly App.dll");
var migNameArg = new Argument<string>("name", description: "Migration name");
var addProvOpt = new Option<string>("--provider", description: "Database provider (sqlserver, sqlite, postgres, mysql)") { IsRequired = true };
var addAsmOpt = new Option<string>("--assembly", description: "Path to assembly containing DbContext and entities") { IsRequired = true };
var addOutOpt = new Option<string>("--output", () => "Migrations", "Output directory for migrations");
add.AddArgument(migNameArg);
add.AddOption(addProvOpt);
add.AddOption(addAsmOpt);
add.AddOption(addOutOpt);

add.SetHandler((string name, string prov, string asmPath, string output) =>
{
    try
    {
        if (!File.Exists(asmPath))
        {
            Console.Error.WriteLine($"Assembly '{asmPath}' not found.");
            return;
        }
        var assembly = Assembly.LoadFrom(asmPath);

        var snapshotPath = Path.Combine(output, "schema.snapshot.json");
        SchemaSnapshot oldSnap = File.Exists(snapshotPath)
            ? JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(snapshotPath)) ?? new SchemaSnapshot()
            : new SchemaSnapshot();

        var newSnap = SchemaSnapshotBuilder.Build(assembly);
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        if (!diff.HasChanges)
        {
            Console.WriteLine("No changes detected.");
            return;
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
        var className = $"{version}_{name}";
        var filePath = Path.Combine(output, className + ".cs");

        var sb = new StringBuilder();
        sb.AppendLine("using System.Data.Common;");
        sb.AppendLine("using nORM.Migration;");
        sb.AppendLine();
        sb.AppendLine($"public class {className} : Migration");
        sb.AppendLine("{");
        sb.AppendLine($"    public {className}() : base({version}, \"{name}\") {{ }}");
        AppendMethod("Up", sql.Up, sb);
        AppendMethod("Down", sql.Down, sb);
        sb.AppendLine("}");
        File.WriteAllText(filePath, sb.ToString());

        var snapJson = JsonSerializer.Serialize(newSnap, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(snapshotPath, snapJson);
        Console.WriteLine($"Migration '{className}' generated at {filePath}.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
    }
}, migNameArg, addProvOpt, addAsmOpt, addOutOpt);

migrations.AddCommand(add);
root.AddCommand(migrations);

return await root.InvokeAsync(args);

static DbConnection CreateConnection(string provider, string connectionString)
{
    try
    {
        return provider.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlConnection(connectionString),
            "sqlite" => new SqliteConnection(connectionString),
            "postgres" or "postgresql" => CreateConnectionFromType("Npgsql.NpgsqlConnection, Npgsql", "PostgreSQL", connectionString),
            "mysql" => CreateConnectionFromType(new[] { "MySql.Data.MySqlClient.MySqlConnection, MySql.Data", "MySqlConnector.MySqlConnection, MySqlConnector" }, "MySQL", connectionString),
            _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
        };
    }
    catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or TypeLoadException)
    {
        throw new InvalidOperationException($"Failed to create connection: {ex.Message}", ex);
    }
}

static DbConnection CreateConnectionFromType(string typeName, string friendly, string connString)
    => CreateConnectionFromType(new[] { typeName }, friendly, connString);

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

static DatabaseProvider CreateProvider(string provider)
    => provider.ToLowerInvariant() switch
    {
        "sqlserver" => new SqlServerProvider(),
        "sqlite" => new SqliteProvider(),
        "postgres" or "postgresql" => new PostgresProvider(),
        "mysql" => new MySqlProvider(),
        _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
    };

static void AppendMethod(string methodName, IReadOnlyList<string> statements, StringBuilder sb)
{
    sb.AppendLine($"    public override void {methodName}(DbConnection connection, DbTransaction transaction)");
    sb.AppendLine("    {");
    sb.AppendLine("        foreach (var sql in new[] {");
    for (int i = 0; i < statements.Count; i++)
    {
        var stmt = statements[i].Replace("\"", "\\\"");
        var comma = i < statements.Count - 1 ? "," : string.Empty;
        sb.AppendLine($"            \"{stmt}\"{comma}");
    }
    sb.AppendLine("        })");
    sb.AppendLine("        {");
    sb.AppendLine("            using var cmd = connection.CreateCommand();");
    sb.AppendLine("            cmd.Transaction = transaction;");
    sb.AppendLine("            cmd.CommandText = sql;");
    sb.AppendLine("            cmd.ExecuteNonQuery();");
    sb.AppendLine("        }");
    sb.AppendLine("    }");
}
