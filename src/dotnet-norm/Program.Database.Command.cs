using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using nORM.Security;

partial class Program
{
    static Command CreateDatabaseCommand()
    {
        // database update/drop commands
        var database = new Command("database", "Database related commands");

        var update = new Command("update", "Apply pending migrations to the database.\nExample:\n  norm database update --connection \"...\" --provider sqlserver --assembly Migrations.dll");
        var migConnOpt = new Option<string>("--connection") { Description = "Database connection string", Required = true };
        var migProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name.", Required = true };
        var assemblyOpt = new Option<string>("--assembly") { Description = "Path to migrations assembly (e.g. ./bin/Debug/net8.0/App.Migrations.dll)", Required = true };
        update.Add(migConnOpt);
        update.Add(migProvOpt);
        update.Add(assemblyOpt);
        update.SetAction(async (ParseResult result, CancellationToken _) =>
        {
            try
            {
                var prov = NormalizeProviderName(result.GetValue(migProvOpt)!);
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
        var dropProvOpt = new Option<string>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name.", Required = true };
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
                var prov = NormalizeProviderName(result.GetValue(dropProvOpt)!);
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
        return database;
    }
}
