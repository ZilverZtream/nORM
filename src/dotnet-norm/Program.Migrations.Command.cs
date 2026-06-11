using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.IO;
using System.Text.Json;
using nORM.Cli;
using nORM.Migration;

partial class Program
{
    static Command CreateMigrationsCommand()
    {
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
        return migrations;
    }
}
