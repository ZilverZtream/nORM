using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
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

            var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(Path.Combine(output, "CliScaffolded.csproj"), $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>enable</Nullable>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", output);

            var scaffoldAssembly = Path.Combine(output, "bin", "Release", "net8.0", "CliScaffolded.dll");
            var migrationsDir = Path.Combine(output, "Migrations");
            var migration = RunCli(
                $"migrations add ScaffoldedInitial --provider sqlite --assembly {Quote(scaffoldAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(migration.ExitCode == 0,
                $"Migration CLI failed with exit code {migration.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{migration.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{migration.Stderr}");

            var generatedMigration = Directory.EnumerateFiles(migrationsDir, "Migration_*_ScaffoldedInitial.cs").Single();
            var migrationSource = File.ReadAllText(generatedMigration);
            Assert.Contains("CREATE INDEX \\\"IX_Book_Author_Title\\\" ON \\\"Book\\\" (\\\"Author_Id\\\", \\\"Title\\\")", migrationSource, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_emit_query_artifacts_generates_read_only_view_and_builds()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_query_artifact_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_query_artifact_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Product (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL,
                        Price NUMERIC NOT NULL
                    );
                    CREATE VIEW ProductReport AS
                        SELECT Id, Name, Price FROM Product;
                    """;
                cmd.ExecuteNonQuery();
            }

            var scaffold = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table ProductReport --emit-query-artifacts",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, "ProductReport.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"ProductReport\")]", viewCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ProductReport> ProductReports", contextCode, StringComparison.Ordinal);
            Assert.Contains("SCF116=1", scaffold.Stdout, StringComparison.Ordinal);

            var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(Path.Combine(output, "CliScaffolded.csproj"), $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>enable</Nullable>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
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
}
