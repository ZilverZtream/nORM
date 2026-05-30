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

[Xunit.Trait("Category", "Fast")]
public class CliIntegrationTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);

    [Fact]
    public void Database_update_missing_assembly_returns_nonzero_without_leaking_connection_secret()
    {
        var root = FindRepositoryRoot();
        var secret = "TopSecret123!";
        var connection = $"Server=localhost;Database=norm;User ID=sa;Password={secret};Encrypt=True;TrustServerCertificate=True";
        var missingAssembly = Path.Combine(root, "missing-migrations.dll");

        var result = RunCli(
            $"database update --connection {Quote(connection)} --provider sqlserver --assembly {Quote(missingAssembly)}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Database_drop_sqlite_requires_yes_or_dry_run()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_guard_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        try
        {
            var result = RunCli(
                $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite",
                root);

            Assert.Equal(3, result.ExitCode);
            Assert.Contains("--yes", result.Stderr, StringComparison.Ordinal);
            Assert.True(File.Exists(dbFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public void Database_drop_sqlite_dry_run_does_not_delete_file()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_dry_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        try
        {
            var result = RunCli(
                $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite --dry-run",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("Would delete", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(dbFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public void Database_drop_sqlite_yes_deletes_file()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_yes_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        var result = RunCli(
            $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite --yes",
            root);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("deleted", result.Stdout, StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(dbFile));
    }

    [Fact]
    public void Scaffold_help_describes_bounded_contract_and_warning_reports()
    {
        var root = FindRepositoryRoot();

        var result = RunCli("scaffold --help", root);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("bounded v1 nORM model", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("nORM.ScaffoldWarnings.md/json", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--fail-on-warnings", result.Stdout, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_fail_on_warnings_returns_nonzero_after_writing_report()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE WarningRow (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Status TEXT NOT NULL DEFAULT 'new'
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --fail-on-warnings",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Scaffolding produced warnings", result.Stderr + result.Stdout, StringComparison.Ordinal);
            Assert.Contains("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("SCF100=1", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("schema-feature=1", result.Stdout, StringComparison.Ordinal);
            var warningFile = Path.Combine(output, "nORM.ScaffoldWarnings.md");
            Assert.True(File.Exists(warningFile));
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("Default", File.ReadAllText(warningFile), StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_with_warnings_returns_zero_and_prints_warning_paths()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_ok_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_ok_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE WarningRow (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Status TEXT NOT NULL DEFAULT 'new'
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Scaffolding completed", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("nORM.ScaffoldWarnings.md", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("nORM.ScaffoldWarnings.json", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("SCF100=1", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("schema-feature=1", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_overwrite_with_stale_warning_report_does_not_print_stale_summary()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_stale_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_stale_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE CleanRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""");

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-overwrite",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("stale scaffold warning report", result.Stderr + result.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("99", result.Stdout, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_clean_run_removes_stale_warning_reports_without_printing_summary()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_clean_stale_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_clean_stale_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE CleanRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md"), "# stale");
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""");

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Scaffolding completed", result.Stdout, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
            Assert.DoesNotContain("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("totalWarnings", result.Stdout, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_sqlite_output_builds_as_consumer_project()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_compile_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_compile_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE Author (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL,
                        "bad""col\name<&>
                    line" TEXT NOT NULL
                    );
                    CREATE TABLE Book (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Author_Id INTEGER NOT NULL,
                        Title TEXT NOT NULL,
                        CONSTRAINT FK_Book_Author FOREIGN KEY (Author_Id) REFERENCES Author(Id)
                    );
                    CREATE TABLE Label (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE BookLabel (
                        BookId INTEGER NOT NULL,
                        LabelId INTEGER NOT NULL,
                        PRIMARY KEY (BookId, LabelId),
                        CONSTRAINT FK_BookLabel_Book FOREIGN KEY (BookId) REFERENCES Book(Id),
                        CONSTRAINT FK_BookLabel_Label FOREIGN KEY (LabelId) REFERENCES Label(Id)
                    );
                    CREATE TABLE Address (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Line1 TEXT NOT NULL
                    );
                    CREATE TABLE Shipment (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        BillingAddressId INTEGER NOT NULL,
                        ShippingAddressId INTEGER NOT NULL,
                        CONSTRAINT FK_Shipment_BillingAddress FOREIGN KEY (BillingAddressId) REFERENCES Address(Id),
                        CONSTRAINT FK_Shipment_ShippingAddress FOREIGN KEY (ShippingAddressId) REFERENCES Address(Id)
                    );
                    CREATE TABLE "audit.events" (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        "value.part" TEXT NOT NULL
                    );
                    CREATE INDEX IX_Book_Author_Title ON Book(Author_Id, Title);
                    CREATE INDEX "IX_Author_Bad""Col
                    Line" ON Author("bad""col\name<&>
                    line");
                    """;
                cmd.ExecuteNonQuery();
            }

            var scaffold = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            File.WriteAllText(Path.Combine(output, "CliScaffolded.csproj"), $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>enable</Nullable>
                    <ImplicitUsings>enable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <ProjectReference Include="{{Path.Combine(root, "src", "nORM.csproj")}}" />
                  </ItemGroup>
                </Project>
                """, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

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

    private static string ModelProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string ModelProjectWithDependencyXml(string root, string dependencyProject)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
                <ProjectReference Include="{{dependencyProject}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string DependencyProjectXml = """
        <Project Sdk="Microsoft.NET.Sdk">
          <PropertyGroup>
            <TargetFramework>net8.0</TargetFramework>
            <Nullable>enable</Nullable>
          </PropertyGroup>
        </Project>
        """;

    private static string RoundTripProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <OutputType>Exe</OutputType>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <UseAppHost>false</UseAppHost>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string WeirdModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Odd\"Table\\Line\nBreak")]
        public sealed class WeirdEntity
        {
            [Key]
            public int Id { get; set; }

            [Column("Value\"Column\\Line\nBreak")]
            public string Value { get; set; } = "";
        }
        """;

    private const string RenameModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Orders")]
        public sealed class Order
        {
            [Key]
            public int Id { get; set; }

            public decimal? TotalAmount { get; set; }
        }
        """;

    private const string DesignTimeFactoryModelSource = """
        using System.Data.Common;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class FactoryEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class FactoryContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class FactoryDesignTimeContext : INormDesignTimeDbContextFactory<FactoryContext>
        {
            public FactoryContext CreateDbContext(string[] args)
            {
                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<FactoryEntity>()
                        .ToTable("FactoryTable")
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName("display_name")
                };
                return new FactoryContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string DependencyModelNamesSource = """
        namespace DependencyLib;

        public static class ModelNames
        {
            public const string TableName = "DependentTable";
            public const string NameColumn = "dependency_name";
        }
        """;

    private const string DependencyBackedDesignTimeFactoryModelSource = """
        using System.Data.Common;
        using DependencyLib;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class DependentEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class DependentContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class DependentDesignTimeContext : INormDesignTimeDbContextFactory<DependentContext>
        {
            public DependentContext CreateDbContext(string[] args)
            {
                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<DependentEntity>()
                        .ToTable(ModelNames.TableName)
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName(ModelNames.NameColumn)
                };
                return new DependentContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string RoundTripProgramSource = """
        using System;
        using System.Collections.Generic;
        using System.Data;
        using System.Data.Common;
        using System.IO;
        using System.Linq;
        using System.Text;

        var connection = new RecordingConnection();
        var transaction = new RecordingTransaction(connection);
        var migration = new Migration_202605220001_Adversarial();
        migration.Up(connection, transaction);
        migration.Down(connection, transaction);
        File.WriteAllLines(args[0], connection.Commands.Select(sql => Convert.ToBase64String(Encoding.UTF8.GetBytes(sql))));

        internal sealed class RecordingConnection : DbConnection
        {
            public List<string> Commands { get; } = new();
            public override string ConnectionString { get; set; } = "";
            public override string Database => "Test";
            public override string DataSource => "Test";
            public override string ServerVersion => "1";
            public override ConnectionState State => ConnectionState.Open;
            public override void ChangeDatabase(string databaseName) { }
            public override void Close() { }
            public override void Open() { }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new RecordingTransaction(this);
            protected override DbCommand CreateDbCommand() => new RecordingCommand(this);
        }

        internal sealed class RecordingTransaction(RecordingConnection connection) : DbTransaction
        {
            public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
            protected override DbConnection DbConnection => connection;
            public override void Commit() { }
            public override void Rollback() { }
        }

        internal sealed class RecordingCommand(RecordingConnection connection) : DbCommand
        {
            private readonly DbParameterCollection _parameters = new RecordingParameterCollection();
            public override string CommandText { get; set; } = "";
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get; set; } = connection;
            protected override DbParameterCollection DbParameterCollection => _parameters;
            protected override DbTransaction? DbTransaction { get; set; }
            public override void Cancel() { }
            public override int ExecuteNonQuery() { connection.Commands.Add(CommandText); return 0; }
            public override object? ExecuteScalar() => null;
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
        }

        internal sealed class RecordingParameterCollection : DbParameterCollection
        {
            private readonly List<object> _items = new();
            public override int Count => _items.Count;
            public override object SyncRoot => this;
            public override int Add(object value) { _items.Add(value); return _items.Count - 1; }
            public override void AddRange(Array values) { foreach (var value in values) Add(value!); }
            public override void Clear() => _items.Clear();
            public override bool Contains(object value) => _items.Contains(value);
            public override bool Contains(string value) => false;
            public override void CopyTo(Array array, int index) => _items.ToArray().CopyTo(array, index);
            public override System.Collections.IEnumerator GetEnumerator() => _items.GetEnumerator();
            public override int IndexOf(object value) => _items.IndexOf(value);
            public override int IndexOf(string parameterName) => -1;
            public override void Insert(int index, object value) => _items.Insert(index, value);
            public override void Remove(object value) => _items.Remove(value);
            public override void RemoveAt(int index) => _items.RemoveAt(index);
            public override void RemoveAt(string parameterName) { }
            protected override DbParameter GetParameter(int index) => (DbParameter)_items[index];
            protected override DbParameter GetParameter(string parameterName) => throw new IndexOutOfRangeException(parameterName);
            protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
            protected override void SetParameter(string parameterName, DbParameter value) => Add(value);
        }
        """;

    private static CliResult RunCli(string arguments, string workingDirectory)
    {
        var root = FindRepositoryRoot();
        var toolPath = Path.Combine(root, "src", "dotnet-norm", "bin", "Release", "net8.0", "dotnet-norm.dll");
        Assert.True(File.Exists(toolPath), $"CLI tool was not built at {toolPath}.");

        return RunProcess("dotnet", $"{Quote(toolPath)} {arguments}", workingDirectory);
    }

    private static void RunDotNet(string arguments, string workingDirectory)
    {
        var result = RunProcess("dotnet", arguments, workingDirectory);
        Assert.True(result.ExitCode == 0,
            $"dotnet {arguments} failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
    }

    private static CliResult RunProcess(string fileName, string arguments, string workingDirectory)
    {
        var startInfo = new ProcessStartInfo(fileName, arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(ProcessTimeout))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // The process may exit between timeout detection and Kill.
            }

            process.WaitForExit();
            var timedOutStdout = stdoutTask.GetAwaiter().GetResult();
            var timedOutStderr = stderrTask.GetAwaiter().GetResult();
            throw new TimeoutException(
                $"{fileName} {arguments} did not exit within {ProcessTimeout.TotalSeconds:N0} seconds.{Environment.NewLine}STDOUT:{Environment.NewLine}{timedOutStdout}{Environment.NewLine}STDERR:{Environment.NewLine}{timedOutStderr}");
        }

        process.WaitForExit();
        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();
        return new CliResult(process.ExitCode, stdout, stderr);
    }

    private static string Quote(string value) => "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; failed deletion only leaves a temp directory.
        }
    }

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr);
}
