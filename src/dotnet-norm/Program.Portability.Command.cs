using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.IO;
using System.Linq;
using nORM.Cli;
using nORM.Migration;

partial class Program
{
    static Command CreatePortabilityCommand()
    {
        var portability = new Command("portability", "Provider mobility certification and diagnostics");
        var portabilityCertify = new Command("certify", "Scan an application for provider-bound code that cannot pass strict provider mobility certification.");
        var portabilityScanPathOpt = new Option<string>("--scan-path") { Description = "Application source directory or file to scan.", Required = true };
        var portabilityReportOpt = new Option<string?>("--report") { Description = "Path to write the JSON certification report." };
        var portabilityHtmlOpt = new Option<string?>("--html") { Description = "Path to write an HTML certification dashboard." };
        var portabilityProfileOpt = new Option<string>("--profile") { Description = "Certification profile label, e.g. all-four, sqlite-postgres, sqlserver-postgres.", DefaultValueFactory = _ => "all-four" };
        var portabilityProvidersOpt = new Option<string?>("--providers") { Description = "Comma-separated provider target list for capability profiling, e.g. sqlite,postgres,mysql." };
        var portabilitySqliteConnectionOpt = new Option<string?>("--sqlite-connection") { Description = "Optional SQLite target connection string to open and validate for actual provider mobility evidence." };
        var portabilitySqlServerConnectionOpt = new Option<string?>("--sqlserver-connection") { Description = "Optional SQL Server target connection string to open and validate for actual provider mobility evidence." };
        var portabilityPostgresConnectionOpt = new Option<string?>("--postgres-connection") { Description = "Optional PostgreSQL target connection string to open and validate for actual provider mobility evidence." };
        var portabilityMySqlConnectionOpt = new Option<string?>("--mysql-connection") { Description = "Optional MySQL target connection string to open and validate for actual provider mobility evidence." };
        var portabilitySchemaSnapshotOpt = new Option<string?>("--schema-snapshot") { Description = "Optional nORM schema.snapshot.json to inspect for provider-mobile schema metadata." };
        var portabilityAssemblyOpt = new Option<string?>("--assembly") { Description = "Optional application assembly containing a design-time DbContext used to inspect provider-mobile schema metadata." };
        var portabilityAttributeOnlyOpt = new Option<bool>("--attribute-only") { Description = "When --assembly is used, build the schema from attributes only instead of requiring a design-time DbContext." };
        portabilityCertify.Add(portabilityScanPathOpt);
        portabilityCertify.Add(portabilityReportOpt);
        portabilityCertify.Add(portabilityHtmlOpt);
        portabilityCertify.Add(portabilityProfileOpt);
        portabilityCertify.Add(portabilityProvidersOpt);
        portabilityCertify.Add(portabilitySqliteConnectionOpt);
        portabilityCertify.Add(portabilitySqlServerConnectionOpt);
        portabilityCertify.Add(portabilityPostgresConnectionOpt);
        portabilityCertify.Add(portabilityMySqlConnectionOpt);
        portabilityCertify.Add(portabilitySchemaSnapshotOpt);
        portabilityCertify.Add(portabilityAssemblyOpt);
        portabilityCertify.Add(portabilityAttributeOnlyOpt);
        portabilityCertify.SetAction((ParseResult result) =>
        {
            try
            {
                var schemaSnapshotPath = result.GetValue(portabilitySchemaSnapshotOpt);
                var assemblyPath = result.GetValue(portabilityAssemblyOpt);
                if (!string.IsNullOrWhiteSpace(schemaSnapshotPath) && !string.IsNullOrWhiteSpace(assemblyPath))
                {
                    Console.Error.WriteLine("Use either --schema-snapshot or --assembly for schema inspection, not both.");
                    return 2;
                }

                SchemaSnapshot? schemaSnapshot = null;
                if (!string.IsNullOrWhiteSpace(assemblyPath))
                {
                    if (!File.Exists(assemblyPath))
                    {
                        Console.Error.WriteLine($"Assembly '{assemblyPath}' not found.");
                        return 2;
                    }

                    var assembly = LoadDesignTimeAssembly(assemblyPath);
                    schemaSnapshot = BuildMigrationSnapshot(assembly, result.GetValue(portabilityAttributeOnlyOpt), Array.Empty<string>());
                }

                var targetProviders = ParseProviderTargetList(result.GetValue(portabilityProvidersOpt));
                var targetVersions = new Dictionary<string, Version?>(StringComparer.OrdinalIgnoreCase);
                var targetFindings = new List<ProviderMobilityFinding>();
                var connectionTargets = new (string Provider, string? ConnectionString)[]
                {
                    ("sqlite", result.GetValue(portabilitySqliteConnectionOpt)),
                    ("sqlserver", result.GetValue(portabilitySqlServerConnectionOpt)),
                    ("postgres", result.GetValue(portabilityPostgresConnectionOpt)),
                    ("mysql", result.GetValue(portabilityMySqlConnectionOpt))
                };
                var explicitTargetProviders = targetProviders?.ToList();
                foreach (var target in connectionTargets)
                {
                    if (string.IsNullOrWhiteSpace(target.ConnectionString))
                        continue;

                    explicitTargetProviders ??= new List<string>();
                    if (!explicitTargetProviders.Contains(target.Provider, StringComparer.OrdinalIgnoreCase))
                        explicitTargetProviders.Add(target.Provider);
                    ProbeProviderTargetConnection(target.Provider, target.ConnectionString!, targetVersions, targetFindings);
                }

                var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
                {
                    ScanPath = result.GetValue(portabilityScanPathOpt)!,
                    JsonReportPath = result.GetValue(portabilityReportOpt),
                    HtmlReportPath = result.GetValue(portabilityHtmlOpt),
                    Profile = result.GetValue(portabilityProfileOpt)!,
                    TargetProviders = explicitTargetProviders,
                    TargetProviderVersions = targetVersions,
                    TargetProviderFindings = targetFindings,
                    SchemaSnapshotPath = schemaSnapshotPath,
                    SchemaSnapshot = schemaSnapshot
                });

                Console.WriteLine($"Provider mobility certification {report.Status}: {report.ErrorCount} error(s), {report.WarningCount} warning(s), {report.ScannedFiles} file(s) scanned, {report.SchemaTables} schema table(s) inspected.");
                if (!string.IsNullOrWhiteSpace(result.GetValue(portabilityReportOpt)))
                    Console.WriteLine($"JSON report written to {Path.GetFullPath(result.GetValue(portabilityReportOpt)!)}");
                if (!string.IsNullOrWhiteSpace(result.GetValue(portabilityHtmlOpt)))
                    Console.WriteLine($"HTML report written to {Path.GetFullPath(result.GetValue(portabilityHtmlOpt)!)}");
                return report.Status == "PASS" ? 0 : 1;
            }
            catch (Exception ex)
            {
                return Fail(ex);
            }
        });
        portability.Add(portabilityCertify);
        return portability;
    }
}