using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using nORM.Cli;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
[Fact]
    public void Migrations_add_generates_compilable_literals_for_special_sql_text()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), WeirdModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add WeirdMigration --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)} --attribute-only",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_WeirdMigration.cs").Single();
            Assert.Contains("\\n", File.ReadAllText(generated), StringComparison.Ordinal);

            RunDotNet("build -c Release --no-restore --nologo", tempRoot);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_without_context_requires_explicit_attribute_only()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_no_context_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), WeirdModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add NeedsExplicitMode --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("No DbContext type was found", result.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("--attribute-only", result.Stderr, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_requires_force_for_destructive_column_drop()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_destructive_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), RenameModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            Directory.CreateDirectory(migrationsDir);
            var oldSnapshot = new SchemaSnapshot
            {
                Tables =
                {
                    new TableSchema
                    {
                        Name = "Orders",
                        Columns =
                        {
                            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                            new ColumnSchema { Name = "TotalCost", ClrType = typeof(decimal).FullName!, IsNullable = true }
                        }
                    }
                }
            };
            File.WriteAllText(
                Path.Combine(migrationsDir, "schema.snapshot.json"),
                JsonSerializer.Serialize(oldSnapshot, new JsonSerializerOptions { WriteIndented = true }),
                Encoding.UTF8);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var blocked = RunCli(
                $"migrations add RenameTotal --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)} --attribute-only",
                root);

            Assert.Equal(3, blocked.ExitCode);
            Assert.Contains("Destructive schema changes detected", blocked.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Orders.TotalCost", blocked.Stderr, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(migrationsDir, "Migration_*_RenameTotal.cs"));

            var forced = RunCli(
                $"migrations add RenameTotal --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)} --attribute-only --force",
                root);

            Assert.Equal(0, forced.ExitCode);
            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_RenameTotal.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("WARNING: destructive schema changes", source, StringComparison.Ordinal);
            Assert.Contains("Possible rename candidate", source, StringComparison.Ordinal);
            Assert.Contains("Orders.TotalAmount", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_design_time_factory_for_fluent_model()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_factory_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add FactoryModel --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_FactoryModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("FactoryTable", source, StringComparison.Ordinal);
            Assert.Contains("display_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_redacts_design_time_factory_failure_details()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_factory_secret_" + Guid.NewGuid().ToString("N"));
        const string secret = "FactorySecret123!";
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), FailingDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add FactorySecretFailure --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Design-time factory", result.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FailingDesignTimeContext", result.Stderr, StringComparison.Ordinal);
            Assert.Contains("Password=[REDACTED]", result.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_resolves_project_output_using_configuration_and_passes_environment()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_project_config_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            var projectPath = Path.Combine(tempRoot, "CliModel.csproj");
            File.WriteAllText(projectPath, ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), EnvironmentAwareDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var debugAssembly = Path.Combine(tempRoot, "bin", "Debug", "net8.0", "CliModel.dll");
            Assert.False(File.Exists(debugAssembly), "The test must prove --configuration Release instead of accidentally loading Debug output.");

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add ReleaseProjectModel --provider sqlite --project {Quote(projectPath)} --configuration Release --target-framework net8.0 --environment Staging --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_ReleaseProjectModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("StagingTable", source, StringComparison.Ordinal);
            Assert.Contains("Staging_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_resolves_multitargeted_project_output_using_framework_alias()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_multi_tfm_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            var projectPath = Path.Combine(tempRoot, "CliModel.csproj");
            File.WriteAllText(projectPath, MultiTargetModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), EnvironmentAwareDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release -f net8.0 --nologo", tempRoot);
            Assert.True(File.Exists(Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll")));
            Assert.False(File.Exists(Path.Combine(tempRoot, "bin", "Release", "netstandard2.1", "CliModel.dll")));

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add MultiTargetProjectModel --provider sqlite --project {Quote(projectPath)} --framework net8.0 --configuration Release --environment MultiTarget --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_MultiTargetProjectModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("MultiTargetTable", source, StringComparison.Ordinal);
            Assert.Contains("MultiTarget_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_startup_project_as_design_time_host()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_startup_host_" + Guid.NewGuid().ToString("N"));
        var targetDir = Path.Combine(tempRoot, "TargetModel");
        var startupDir = Path.Combine(tempRoot, "StartupHost");
        Directory.CreateDirectory(targetDir);
        Directory.CreateDirectory(startupDir);

        try
        {
            var targetProjectPath = Path.Combine(targetDir, "TargetModel.csproj");
            var startupProjectPath = Path.Combine(startupDir, "StartupHost.csproj");
            File.WriteAllText(targetProjectPath, SimpleNet8ProjectXml, Encoding.UTF8);
            File.WriteAllText(startupProjectPath, StartupHostProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(startupDir, "Model.cs"), EnvironmentAwareDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build StartupHost.csproj -c Release --nologo", startupDir);
            Assert.False(File.Exists(Path.Combine(targetDir, "bin", "Release", "net8.0", "TargetModel.dll")));

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add StartupHostModel --provider sqlite --project {Quote(targetProjectPath)} --startup-project {Quote(startupProjectPath)} --configuration Release --target-framework net8.0 --environment StartupHost --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_StartupHostModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("StartupHostTable", source, StringComparison.Ordinal);
            Assert.Contains("StartupHost_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_resolves_design_time_assembly_dependencies()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_deps_" + Guid.NewGuid().ToString("N"));
        var dependencyDir = Path.Combine(tempRoot, "DependencyLib");
        Directory.CreateDirectory(dependencyDir);

        try
        {
            var dependencyProject = Path.Combine(dependencyDir, "DependencyLib.csproj");
            File.WriteAllText(dependencyProject, DependencyProjectXml, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dependencyDir, "ModelNames.cs"), DependencyModelNamesSource, Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectWithDependencyXml(root, dependencyProject), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DependencyBackedDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add DependencyModel --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_DependencyModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("DependentTable", source, StringComparison.Ordinal);
            Assert.Contains("dependency_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_explicit_deps_probe_directory_for_design_time_dependencies()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_explicit_deps_" + Guid.NewGuid().ToString("N"));
        var dependencyDir = Path.Combine(tempRoot, "DependencyLib");
        Directory.CreateDirectory(dependencyDir);

        try
        {
            var dependencyProject = Path.Combine(dependencyDir, "DependencyLib.csproj");
            File.WriteAllText(dependencyProject, DependencyProjectXml, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dependencyDir, "ModelNames.cs"), DependencyModelNamesSource, Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectWithDependencyXml(root, dependencyProject), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DependencyBackedDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var outputDir = Path.Combine(tempRoot, "bin", "Release", "net8.0");
            var explicitDepsDir = Path.Combine(tempRoot, "ExplicitDeps");
            Directory.CreateDirectory(explicitDepsDir);

            var dependencyAssembly = Path.Combine(outputDir, "DependencyLib.dll");
            var explicitDependencyDir = Path.Combine(explicitDepsDir, "lib", "net8.0");
            Directory.CreateDirectory(explicitDependencyDir);
            var explicitDependencyAssembly = Path.Combine(explicitDependencyDir, "DependencyLib.dll");
            File.Copy(dependencyAssembly, explicitDependencyAssembly);
            File.Delete(dependencyAssembly);

            var outputDeps = Path.Combine(outputDir, "CliModel.deps.json");
            var explicitDeps = Path.Combine(explicitDepsDir, "CliModel.deps.json");
            var depsJson = File.ReadAllText(outputDeps).Replace("\"DependencyLib.dll\"", "\"lib/net8.0/DependencyLib.dll\"", StringComparison.Ordinal);
            File.WriteAllText(explicitDeps, depsJson, Encoding.UTF8);

            var modelAssembly = Path.Combine(outputDir, "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add ExplicitDepsModel --provider sqlite --assembly {Quote(modelAssembly)} --deps {Quote(explicitDeps)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_ExplicitDepsModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("DependentTable", source, StringComparison.Ordinal);
            Assert.Contains("dependency_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_runtimeconfig_additional_probing_paths_for_deps_packages()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_runtimeconfig_probe_" + Guid.NewGuid().ToString("N"));
        var dependencyDir = Path.Combine(tempRoot, "DependencyLib");
        Directory.CreateDirectory(dependencyDir);

        try
        {
            var dependencyProject = Path.Combine(dependencyDir, "DependencyLib.csproj");
            File.WriteAllText(dependencyProject, DependencyProjectXml, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dependencyDir, "ModelNames.cs"), DependencyModelNamesSource, Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectWithDependencyXml(root, dependencyProject), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DependencyBackedDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var outputDir = Path.Combine(tempRoot, "bin", "Release", "net8.0");
            var explicitDepsDir = Path.Combine(tempRoot, "ExplicitDeps");
            var packageRoot = Path.Combine(tempRoot, "PackageRoot");
            var packageDependencyDir = Path.Combine(packageRoot, "dependencylib", "1.0.0", "lib", "net8.0");
            Directory.CreateDirectory(explicitDepsDir);
            Directory.CreateDirectory(packageDependencyDir);

            var dependencyAssembly = Path.Combine(outputDir, "DependencyLib.dll");
            File.Copy(dependencyAssembly, Path.Combine(packageDependencyDir, "DependencyLib.dll"));
            File.Delete(dependencyAssembly);

            var outputDeps = Path.Combine(outputDir, "CliModel.deps.json");
            var explicitDeps = Path.Combine(explicitDepsDir, "CliModel.deps.json");
            var depsJson = File.ReadAllText(outputDeps).Replace("\"DependencyLib.dll\"", "\"lib/net8.0/DependencyLib.dll\"", StringComparison.Ordinal);
            File.WriteAllText(explicitDeps, depsJson, Encoding.UTF8);

            var runtimeConfig = Path.Combine(explicitDepsDir, "CliModel.runtimeconfig.json");
            var escapedPackageRoot = packageRoot.Replace("\\", "\\\\", StringComparison.Ordinal);
            File.WriteAllText(runtimeConfig, $$"""
                {
                  "runtimeOptions": {
                    "tfm": "net8.0",
                    "additionalProbingPaths": [
                      "{{escapedPackageRoot}}"
                    ]
                  }
                }
                """, Encoding.UTF8);

            var modelAssembly = Path.Combine(outputDir, "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add RuntimeConfigProbeModel --provider sqlite --assembly {Quote(modelAssembly)} --deps {Quote(explicitDeps)} --runtimeconfig {Quote(runtimeConfig)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_RuntimeConfigProbeModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("DependentTable", source, StringComparison.Ordinal);
            Assert.Contains("dependency_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_explicit_deps_native_runtime_assets()
    {
        if (!OperatingSystem.IsWindows())
            return;

        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_native_deps_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), NativeProbeDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var explicitDepsDir = Path.Combine(tempRoot, "ExplicitDeps");
            var nativeDir = Path.Combine(explicitDepsDir, "runtimes", "win-x64", "native");
            Directory.CreateDirectory(nativeDir);
            File.Copy(
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), "kernel32.dll"),
                Path.Combine(nativeDir, "NativeProbe.dll"));
            File.WriteAllText(Path.Combine(explicitDepsDir, "CliModel.deps.json"), NativeProbeDepsJson, Encoding.UTF8);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add NativeDepsModel --provider sqlite --assembly {Quote(modelAssembly)} --deps {Quote(Path.Combine(explicitDepsDir, "CliModel.deps.json"))} --output {Quote(migrationsDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("NormMissingNativeEntryPoint", result.Stderr, StringComparison.Ordinal);
            Assert.DoesNotContain("Unable to load DLL 'NativeProbe'", result.Stderr, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void MigrationCodeWriter_generated_source_compiles_and_roundtrips_adversarial_sql()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_migration_writer_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        var preUp = "PRAGMA foreign_keys=OFF; -- \"quoted\" path C:\\temp\\up";
        var up = """"
            CREATE FUNCTION public.norm_test()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $norm$
            BEGIN
                RAISE NOTICE 'quote " and backslash \ and raw delimiter """';
                RETURN NEW;
            END;
            $norm$;
            """";
        var up2 = "CREATE TRIGGER `trg_norm` BEFORE INSERT ON `Odd` FOR EACH ROW SET NEW.`Path` = 'C:\\\\norm\\nline';";
        var down = "DROP FUNCTION IF EXISTS public.norm_test();\r\n-- line after CRLF";
        var postDown = "PRAGMA foreign_keys=ON; -- trailing \u001f control";
        var migrationSql = new MigrationSqlStatements(
            Up: new[] { up, up2 },
            Down: new[] { down },
            PreTransactionUp: new[] { preUp },
            PostTransactionDown: new[] { postDown });

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "RoundTrip.csproj"), RoundTripProjectXml(root), Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(tempRoot, "Migration_202605220001_Adversarial.cs"),
                MigrationCodeWriter.WriteMigrationSource("Migration_202605220001_Adversarial", 202605220001, "Adversarial", migrationSql),
                Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Program.cs"), RoundTripProgramSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);
            var roundTripPath = Path.Combine(tempRoot, "roundtrip.txt");
            var appDll = Path.Combine(tempRoot, "bin", "Release", "net8.0", "RoundTrip.dll");
            var runResult = RunProcess("dotnet", $"{Quote(appDll)} {Quote(roundTripPath)}", tempRoot);
            Assert.True(runResult.ExitCode == 0,
                $"RoundTrip.dll failed with exit code {runResult.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{runResult.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{runResult.Stderr}");

            var actual = File.ReadAllLines(roundTripPath)
                .Select(line => Encoding.UTF8.GetString(Convert.FromBase64String(line)))
                .ToArray();

            Assert.Equal(new[] { preUp, up, up2, down, postDown }, actual);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }
}
