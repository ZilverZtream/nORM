using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Migration;
using nORM.Cli;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class ProviderMobilityCertificationRunnerTests
{
    [Fact]
    public async Task Portability_certification_passes_for_generated_nORM_source()
    {
        var root = CreateTempDirectory();
        var jsonPath = Path.Combine(root, "report.json");
        var htmlPath = Path.Combine(root, "report.html");
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using nORM.Core;

                public sealed class Repository
                {
                    public async System.Threading.Tasks.Task<System.Collections.Generic.List<RowDto>> Run(DbContext db)
                    {
                        return await db.Query<Row>()
                            .Where(row => row.Id > 0)
                            .OrderBy(row => row.Name)
                            .Select(row => new RowDto { Id = row.Id, Name = row.Name })
                            .ToListAsync();
                    }
                }

                public sealed class Row { public int Id { get; set; } public string Name { get; set; } = ""; }
                public sealed class RowDto { public int Id { get; set; } public string Name { get; set; } = ""; }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                JsonReportPath = jsonPath,
                HtmlReportPath = htmlPath
            });

            Assert.Equal("PASS", report.Status);
            Assert.Equal(0, report.ErrorCount);
            Assert.True(report.WarningCount > 0);
            Assert.Empty(report.Findings);
            Assert.Equal(4, report.ProviderTargets.Count);
            Assert.Contains(report.ProviderTargets, target =>
                target.Provider == "SQLite" &&
                target.Decisions.Any(decision =>
                    decision.Feature == ProviderMobilityProviderFeature.BulkInsert &&
                    decision.Support == ProviderMobilitySupport.Emulated &&
                    decision.CertificationSeverity == "Warning"));
            Assert.Contains(report.Recommendations, recommendation =>
                recommendation.Kind == "provider-target-BulkInsert" &&
                recommendation.Priority == "P2");
            Assert.Contains(report.Recommendations, recommendation =>
                recommendation.Kind == "provider-target-DecimalComparisonNormalization" &&
                recommendation.Priority == "P2");
            Assert.Contains(report.Recommendations, recommendation =>
                recommendation.Kind == "provider-target-RegexTranslation" &&
                recommendation.SuggestedFix.Contains("SQL Server", StringComparison.OrdinalIgnoreCase));
            Assert.True(File.Exists(jsonPath));
            Assert.True(File.Exists(htmlPath));

            var fromDisk = JsonSerializer.Deserialize<ProviderMobilityCertificationReport>(
                await File.ReadAllTextAsync(jsonPath),
                ReportJsonOptions());
            Assert.NotNull(fromDisk);
            Assert.Equal("PASS", fromDisk!.Status);
            Assert.Equal(4, fromDisk.ProviderTargets.Count);
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_fails_for_provider_bound_source()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using Microsoft.Data.SqlClient;
                using System.ComponentModel.DataAnnotations.Schema;
                using nORM.Core;
                using nORM.Query;

                public sealed class Repository
                {
                    public async System.Threading.Tasks.Task Run(DbContext db)
                    {
                        _ = db.Connection;
                        _ = db.Provider;
                        _ = db.Query("Rows");
                        await db.FromSqlRawAsync<Row>("SELECT * FROM dbo.Rows");
                        await db.ExecuteStoredProcedureAsync<Row>("dbo.GetRows");
                        db.Options.CommandInterceptors.Add(null!);
                    }
                }

                public sealed class Row
                {
                    public int Id { get; set; }
                    [Column(TypeName = "money")]
                    public decimal Amount { get; set; }
                }

                public static class ProviderSpecificFunctions
                {
                    [SqlFunction("SOUNDEX({0})")]
                    public static string Soundex(string value) => value;
                }
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "Procedure.sql"), """
                CREATE PROCEDURE dbo.GetRows
                AS
                SELECT * FROM dbo.Rows WITH (NOLOCK) WHERE CreatedAt > DATEADD(day, -7, GETDATE());
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                Profile = "sqlserver-postgres"
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Equal(2, report.ProviderTargets.Count);
            Assert.Contains(report.ProviderTargets, target => target.Provider == "SQL Server");
            Assert.Contains(report.ProviderTargets, target => target.Provider == "PostgreSQL");
            Assert.True(report.ErrorCount > 0);
            Assert.Contains(report.Findings, finding => finding.Kind == "raw-sql");
            Assert.Contains(report.Findings, finding => finding.Kind == "stored-procedure");
            Assert.Contains(report.Findings, finding => finding.Kind == "direct-connection");
            Assert.Contains(report.Findings, finding => finding.Kind == "direct-provider-access");
            Assert.Contains(report.Findings, finding => finding.Kind == "dynamic-table-query");
            Assert.Contains(report.Findings, finding => finding.Kind == "command-interceptor");
            Assert.Contains(report.Findings, finding => finding.Kind == "custom-sql-function");
            Assert.Contains(report.Findings, finding => finding.Kind == "provider-specific-package");
            Assert.Contains(report.Findings, finding => finding.Kind == "provider-specific-column-type");
            Assert.Contains(report.Findings, finding => finding.Kind == "stored-procedure-definition");
            Assert.Contains(report.Findings, finding => finding.Kind == "sql-server-specific-sql");
            Assert.Contains(report.Recommendations, recommendation => recommendation.Priority == "P0");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_flags_ef_core_and_dapper_provider_bound_usage()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "LegacyRepository.cs"), """
                using Dapper;
                using Microsoft.EntityFrameworkCore;
                using Microsoft.Data.SqlClient;

                public sealed class LegacyRepository
                {
                    public void Configure(DbContextOptionsBuilder builder)
                    {
                        builder.UseSqlServer("Server=.;Database=Legacy");
                    }

                    public async System.Threading.Tasks.Task Run(Microsoft.EntityFrameworkCore.DbContext db, System.Data.Common.DbConnection cn)
                    {
                        await db.Set<Row>().FromSqlRaw("SELECT * FROM dbo.Rows").ToListAsync();
                        await db.Database.ExecuteSqlRawAsync("UPDATE dbo.Rows SET Name = Name");
                        _ = await cn.QueryAsync<Row>("SELECT * FROM dbo.Rows");
                        _ = new SqlCommand("SELECT * FROM dbo.Rows", new SqlConnection("Server=.;Database=Legacy"));
                    }

                    public void ConfigureModel(ModelBuilder modelBuilder)
                    {
                        modelBuilder.Entity<Row>().Property(r => r.Name).HasDefaultValueSql("GETDATE()");
                        modelBuilder.Entity<Row>().Property(r => r.Code).HasComputedColumnSql("LOWER([Name])");
                        modelBuilder.UseCollation("SQL_Latin1_General_CP1_CI_AS");
                        modelBuilder.Entity<Row>().Property(r => r.Name).UseCollation("Latin1_General_100_BIN2");
                    }

                    public void Migration(MigrationBuilder migrationBuilder)
                    {
                        migrationBuilder.Sql("UPDATE dbo.Rows SET Name = Name");
                        migrationBuilder.AlterColumn<string>("Name", "Rows").Annotation("SqlServer:Collation", "Latin1_General_100_BIN2");
                        migrationBuilder.AlterColumn<int>("Id", "Rows").Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);
                    }
                }

                public sealed class Row { public int Id { get; set; } public string Name { get; set; } = ""; public string Code { get; set; } = ""; }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Contains(report.Findings, finding => finding.Kind == "raw-sql" && finding.Severity == "Error");
            Assert.Contains(report.Findings, finding => finding.Kind == "provider-bootstrap-connection" && finding.Severity == "Warning");
            Assert.Contains(report.Findings, finding => finding.Kind == "schema-provider-specific-default" && finding.Severity == "Error");
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "direct-command-access" &&
                finding.SuggestedFix.Contains("provider-specific command", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "raw-sql" &&
                finding.SuggestedFix.Contains("EF migration SQL", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "schema-provider-specific-default" &&
                finding.SuggestedFix.Contains("SQL Server-specific EF migration annotations", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "schema-provider-specific-default" &&
                finding.SuggestedFix.Contains("provider-specific collation", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "schema-provider-specific-default" &&
                finding.SuggestedFix.Contains("value generation", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "certification-unclassified-finding");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_scans_project_files_for_provider_bound_packages()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using nORM.Core;

                public sealed class Repository
                {
                    public System.Linq.IQueryable<Row> Run(DbContext db) => db.Query<Row>();
                }

                public sealed class Row { public int Id { get; set; } }
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "LegacyApp.csproj"), """
                <Project Sdk="Microsoft.NET.Sdk">
                  <ItemGroup>
                    <PackageReference
                      Include='Dapper'
                      Version='2.1.66' />
                    <PackageReference Update='Microsoft.EntityFrameworkCore.SqlServer' Version='9.0.0' />
                    <PackageReference Include="Npgsql" Version="9.0.0" />
                  </ItemGroup>
                </Project>
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "Directory.Packages.props"), """
                <Project>
                  <ItemGroup>
                    <PackageVersion Include='Pomelo.EntityFrameworkCore.MySql' Version='9.0.0' />
                  </ItemGroup>
                </Project>
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "postgres" }
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Equal(3, report.ScannedFiles);
            Assert.Contains(report.Findings, finding =>
                finding.Path.EndsWith("LegacyApp.csproj", StringComparison.OrdinalIgnoreCase) &&
                finding.Kind == "raw-sql" &&
                finding.SuggestedFix.Contains("Dapper package", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Path.EndsWith("LegacyApp.csproj", StringComparison.OrdinalIgnoreCase) &&
                finding.Kind == "provider-specific-package" &&
                finding.Severity == "Warning");
            Assert.Contains(report.Findings, finding =>
                finding.Path.EndsWith("Directory.Packages.props", StringComparison.OrdinalIgnoreCase) &&
                finding.Kind == "provider-specific-package" &&
                finding.SuggestedFix.Contains("EF Core MySQL", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "certification-unclassified-finding");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_warns_for_constrained_sequence_linq_shapes()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using System.Text.RegularExpressions;
                using nORM.Core;

                public sealed class Repository
                {
                    public System.Linq.IQueryable<Row> Run(DbContext db) =>
                        db.Query<Row>().Where(row => Regex.IsMatch(row.Name, "^[A-Z]"));

                    public System.Linq.IQueryable<string> Project(DbContext db) =>
                        db.Query<Row>().Select(row => Regex.Replace(row.Name, "[0-9]+", "#"));

                    public System.Linq.IQueryable<System.DateTime> LocalProjection(DbContext db) =>
                        db.Query<Row>().Select(row => row.Stamp.LocalDateTime);
                }

                public sealed class Row { public int Id { get; set; } public string Name { get; set; } = ""; public System.DateTimeOffset Stamp { get; set; } }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "sqlserver", "postgres" }
            });

            Assert.Equal("PASS", report.Status);
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "constrained-linq-shape" &&
                finding.Severity == "Warning" &&
                finding.SuggestedFix.Contains("Regex LINQ", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(report.Findings, finding =>
                finding.Kind == "constrained-linq-shape" &&
                finding.SuggestedFix.Contains("LocalDateTime", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "certification-unclassified-finding");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_warns_for_constrained_linq_shapes()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using System;
                using nORM.Core;

                public sealed class Repository
                {
                    public System.Linq.IQueryable<Row> TakeWhileShape(DbContext db) =>
                        db.Query<Row>().OrderBy(row => row.Id).TakeWhile(row => row.Id < 10);

                    public System.Linq.IQueryable<Row> SkipWhileShape(DbContext db) =>
                        db.Query<Row>().OrderBy(row => row.Id).SkipWhile(row => row.Id < 10);

                    public bool SequenceEqualShape(DbContext db, System.Linq.IQueryable<Row> other) =>
                        db.Query<Row>().SequenceEqual(other);

                }

                public sealed class Row { public int Id { get; set; } public Guid ExternalId { get; set; } }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root
            });

            Assert.Equal("PASS", report.Status);
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "constrained-linq-shape" &&
                finding.Severity == "Warning" &&
                finding.SuggestedFix.Contains("TakeWhile", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "constrained-linq-shape" &&
                finding.Severity == "Warning" &&
                finding.SuggestedFix.Contains("SkipWhile", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "constrained-linq-shape" &&
                finding.Severity == "Warning" &&
                finding.SuggestedFix.Contains("SequenceEqual", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "certification-unclassified-finding");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_ignores_generated_artifact_and_dependency_folders()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using nORM.Core;

                public sealed class Repository
                {
                    public System.Linq.IQueryable<Row> Run(DbContext db) => db.Query<Row>();
                }

                public sealed class Row { public int Id { get; set; } }
                """);

            foreach (var folder in new[] { "bin", "obj", "artifacts", ".tmp", "node_modules", "packages" })
            {
                var path = Path.Combine(root, folder);
                Directory.CreateDirectory(path);
                await File.WriteAllTextAsync(Path.Combine(path, "Generated.cs"), """
                    public sealed class Generated
                    {
                        public const string Sql = "SELECT * FROM dbo.Rows WITH (NOLOCK)";
                        public void Run(dynamic db) => db.FromSqlRawAsync<Row>("SELECT * FROM dbo.Rows");
                    }
                    """);
            }

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "postgres" }
            });

            Assert.Equal("PASS", report.Status);
            Assert.Equal(1, report.ScannedFiles);
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "raw-sql");
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "sql-server-specific-sql");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_scans_explicit_root_even_when_root_name_is_excluded_folder()
    {
        var parent = CreateTempDirectory();
        var root = Path.Combine(parent, ".tmp");
        Directory.CreateDirectory(root);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using nORM.Core;

                public sealed class Repository
                {
                    public System.Linq.IQueryable<Row> Run(DbContext db) => db.Query<Row>();
                }

                public sealed class Row { public int Id { get; set; } }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "postgres" }
            });

            Assert.Equal("PASS", report.Status);
            Assert.Equal(1, report.ScannedFiles);
        }
        finally
        {
            DeleteTempDirectory(parent);
        }
    }

    [Fact]
    public async Task Portability_certification_ignores_provider_bound_patterns_inside_block_comments()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using nORM.Core;

                /*
                   db.FromSqlRawAsync<Row>("SELECT * FROM dbo.Rows");
                   db.ExecuteStoredProcedureAsync<Row>("dbo.GetRows");
                */
                public sealed class Repository
                {
                    public System.Linq.IQueryable<Row> Run(DbContext db) => db.Query<Row>();
                }

                public sealed class Row { public int Id { get; set; } }
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "Script.sql"), """
                /*
                  CREATE PROCEDURE dbo.GetRows AS SELECT * FROM dbo.Rows WITH (NOLOCK);
                */
                SELECT 1;
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "postgres" }
            });

            Assert.Equal("PASS", report.Status);
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "raw-sql");
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "stored-procedure");
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "stored-procedure-definition");
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "sql-server-specific-sql");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_treats_warn_client_projection_as_warning_and_allow_as_error()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "WarnRepository.cs"), """
                using nORM.Configuration;

                public sealed class WarnRepository
                {
                    public void Configure()
                    {
                        _ = new DbContextOptions().UseStrictProviderMobility(ClientEvaluationPolicy.Warn);
                    }
                }
                """);

            var warnReport = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root
            });

            Assert.Equal("PASS", warnReport.Status);
            Assert.Equal(0, warnReport.ErrorCount);
            Assert.Contains(warnReport.Findings, finding =>
                finding.Kind == "client-projection-tail" && finding.Severity == "Warning");

            await File.WriteAllTextAsync(Path.Combine(root, "AllowRepository.cs"), """
                using nORM.Configuration;

                public sealed class AllowRepository
                {
                    public void Configure()
                    {
                        _ = new DbContextOptions { ClientEvaluationPolicy = ClientEvaluationPolicy.Allow };
                    }
                }
                """);

            var allowReport = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root
            });

            Assert.Equal("FAIL", allowReport.Status);
            Assert.Contains(allowReport.Findings, finding =>
                finding.Kind == "client-evaluation" && finding.Severity == "Error");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_treats_provider_bootstrap_as_warning_inventory()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Program.cs"), """
                using Microsoft.Data.Sqlite;
                using nORM.Core;
                using nORM.Providers;

                public static class Program
                {
                    public static DbContext Create(string connectionString)
                    {
                        var connection = new SqliteConnection(connectionString);
                        return new DbContext(connection, new SqliteProvider());
                    }
                }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root
            });

            Assert.Equal("PASS", report.Status);
            Assert.Equal(0, report.ErrorCount);
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "provider-bootstrap-connection" && finding.Severity == "Warning");
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "provider-specific-package" && finding.Severity == "Warning");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_fails_for_unknown_provider_target()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "sqlite", "oracle" }
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Single(report.ProviderTargets);
            Assert.Contains(report.ProviderTargets, target => target.Provider == "SQLite");
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "provider-target-unknown" &&
                finding.Severity == "Error" &&
                finding.Reason.Contains("oracle", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_canonicalizes_provider_target_aliases()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "mssql", "sqlserver", "postgresql", "postgres", "mariadb", "mysql" },
                TargetProviderVersions = new Dictionary<string, Version?>
                {
                    ["mssql"] = new Version(16, 0),
                    ["postgresql"] = new Version(17, 0),
                    ["mariadb"] = new Version(8, 0)
                }
            });

            Assert.Equal("PASS", report.Status);
            Assert.Equal(
                new[] { "MySQL", "PostgreSQL", "SQL Server" },
                report.ProviderTargets
                    .Select(static target => target.Provider)
                    .OrderBy(static provider => provider, StringComparer.Ordinal)
                    .ToArray());
            Assert.Contains(report.ProviderTargets, target =>
                target.Provider == "SQL Server" &&
                target.Decisions.Any(decision =>
                    decision.Feature == ProviderMobilityProviderFeature.ServerVersion &&
                    decision.ActualServerVersion == new Version(16, 0)));
            Assert.Contains(report.ProviderTargets, target =>
                target.Provider == "PostgreSQL" &&
                target.Decisions.Any(decision =>
                    decision.Feature == ProviderMobilityProviderFeature.ServerVersion &&
                    decision.ActualServerVersion == new Version(17, 0)));
            Assert.Contains(report.ProviderTargets, target =>
                target.Provider == "MySQL" &&
                target.Decisions.Any(decision =>
                    decision.Feature == ProviderMobilityProviderFeature.ServerVersion &&
                    decision.ActualServerVersion == new Version(8, 0)));
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_records_actual_provider_target_version()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "sqlite" },
                TargetProviderVersions = new Dictionary<string, Version?>
                {
                    ["sqlite"] = new Version(3, 46, 1)
                }
            });

            Assert.Equal("PASS", report.Status);
            var target = Assert.Single(report.ProviderTargets);
            var versionDecision = Assert.Single(target.Decisions.Where(decision =>
                decision.Feature == ProviderMobilityProviderFeature.ServerVersion));
            Assert.Equal(new Version(3, 46, 1), versionDecision.ActualServerVersion);
            Assert.Equal(ProviderMobilitySupport.Portable, versionDecision.Support);
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_fails_when_required_actual_provider_version_is_unavailable()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "sqlite" },
                TargetProviderVersions = new Dictionary<string, Version?>
                {
                    ["sqlite"] = null
                }
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Equal(report.Findings.Count(finding =>
                finding.Severity.Equals("Error", StringComparison.OrdinalIgnoreCase)), report.ErrorCount);
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "provider-target-capability" &&
                finding.Severity == "Error" &&
                finding.Reason.Contains("could not be determined", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_fails_for_feature_specific_provider_version_floor()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                TargetProviders = new[] { "sqlserver" },
                TargetProviderVersions = new Dictionary<string, Version?>
                {
                    ["sqlserver"] = new Version(13, 0)
                }
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Contains(report.ProviderTargets, target =>
                target.Provider == "SQL Server" &&
                target.Decisions.Any(decision =>
                    decision.Feature == ProviderMobilityProviderFeature.OrderedStringAggregate &&
                    decision.CertificationSeverity == "Error" &&
                    decision.MinimumServerVersion == new Version(14, 0)));
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "provider-target-capability" &&
                finding.Reason.Contains("ordered string aggregate", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_inspects_schema_snapshot()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                SchemaSnapshot = new SchemaSnapshot
                {
                    Tables =
                    {
                        new TableSchema
                        {
                            Name = "Orders",
                            Columns =
                            {
                                new ColumnSchema
                                {
                                    Name = "Id",
                                    ClrType = typeof(int).FullName!,
                                    IsPrimaryKey = true,
                                    IsIdentity = true
                                },
                                new ColumnSchema
                                {
                                    Name = "TenantId",
                                    ClrType = typeof(Guid).FullName!
                                },
                                new ColumnSchema
                                {
                                    Name = "PlacedAt",
                                    ClrType = typeof(DateTimeOffset).FullName!
                                },
                                new ColumnSchema
                                {
                                    Name = "Total",
                                    ClrType = typeof(decimal).FullName!,
                                    DefaultValue = "0"
                                }
                            }
                        }
                    }
                }
            });

            Assert.Equal("PASS", report.Status);
            Assert.Equal(1, report.SchemaTables);
            Assert.DoesNotContain(report.Findings, finding => finding.Kind.StartsWith("schema-", System.StringComparison.Ordinal));
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_warns_for_database_evaluated_standard_defaults()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                SchemaSnapshot = new SchemaSnapshot
                {
                    Tables =
                    {
                        new TableSchema
                        {
                            Name = "Orders",
                            Columns =
                            {
                                new ColumnSchema
                                {
                                    Name = "Id",
                                    ClrType = typeof(int).FullName!,
                                    IsPrimaryKey = true,
                                    IsIdentity = true
                                },
                                new ColumnSchema
                                {
                                    Name = "CreatedAt",
                                    ClrType = typeof(DateTime).FullName!,
                                    DefaultValue = "CURRENT_TIMESTAMP"
                                }
                            }
                        }
                    }
                }
            });

            Assert.Equal("PASS", report.Status);
            Assert.True(report.WarningCount > 0);
            Assert.Contains(report.Findings, finding =>
                finding.Kind == "schema-default-needs-review" &&
                finding.Reason.Contains("database-evaluated", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "schema-provider-specific-default");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_fails_for_nonportable_schema_metadata()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                SchemaSnapshot = new SchemaSnapshot
                {
                    Tables =
                    {
                        new TableSchema
                        {
                            Name = "Orders",
                            Columns =
                            {
                                new ColumnSchema
                                {
                                    Name = "Id",
                                    ClrType = typeof(Guid).FullName!,
                                    IsPrimaryKey = true,
                                    IsIdentity = true
                                },
                                new ColumnSchema
                                {
                                    Name = "Payload",
                                    ClrType = typeof(System.Text.Json.JsonDocument).FullName!
                                },
                                new ColumnSchema
                                {
                                    Name = "CreatedAt",
                                    ClrType = typeof(DateTime).FullName!,
                                    DefaultValue = "GETDATE()"
                                }
                            }
                        }
                    }
                }
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Contains(report.Findings, finding => finding.Kind == "schema-nonportable-identity");
            Assert.Contains(report.Findings, finding => finding.Kind == "schema-unsupported-clr-type");
            Assert.Contains(report.Findings, finding => finding.Kind == "schema-provider-specific-default");
            foreach (var finding in report.Findings.Where(f => f.Kind.StartsWith("schema-", StringComparison.Ordinal)))
            {
                Assert.True(
                    ProviderMobilityTranslator.TryDecideFindingKind(finding.Kind, out _),
                    $"Schema finding kind '{finding.Kind}' is missing from ProviderMobilityTranslator.");
            }
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    [Fact]
    public async Task Portability_certification_fails_for_missing_schema_snapshot_path()
    {
        var root = CreateTempDirectory();
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), "public sealed class Repository { }");

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root,
                SchemaSnapshotPath = Path.Combine(root, "missing.schema.snapshot.json")
            });

            Assert.Equal("FAIL", report.Status);
            Assert.Contains(report.Findings, finding => finding.Kind == "schema-snapshot-missing");
            Assert.DoesNotContain(report.Findings, finding => finding.Kind == "certification-unclassified-finding");
        }
        finally
        {
            DeleteTempDirectory(root);
        }
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "norm-portability-" + Path.GetRandomFileName());
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempDirectory(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private static JsonSerializerOptions ReportJsonOptions()
    {
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }
}
