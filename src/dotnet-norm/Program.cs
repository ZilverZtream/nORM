using System;
using System.CommandLine;
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

var migrations = new Command("migrations", "Migration management commands");
var add = new Command("add", "Add a new migration");
var migNameArg = new Argument<string>("name", description: "Migration name");
var addProvOpt = new Option<string>("--provider", description: "Database provider (sqlserver)") { IsRequired = true };
var addAsmOpt = new Option<string>("--assembly", description: "Path to assembly containing DbContext and entities") { IsRequired = true };
var addOutOpt = new Option<string>("--output", () => "Migrations", "Output directory for migrations");
add.AddArgument(migNameArg);
add.AddOption(addProvOpt);
add.AddOption(addAsmOpt);
add.AddOption(addOutOpt);

add.SetHandler((string name, string prov, string asmPath, string output) =>
{
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
}, migNameArg, addProvOpt, addAsmOpt, addOutOpt);

migrations.AddCommand(add);
root.AddCommand(migrations);

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
