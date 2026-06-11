using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using nORM.Configuration;

namespace nORM.Cli;

public sealed record ProviderMobilityScanResult(string RootPath, int ScannedFiles, IReadOnlyList<ProviderMobilityFinding> Findings);

public static class ProviderMobilitySourceScanner
{
    private static readonly (string Pattern, string Kind, string SuggestedFix)[] CSharpRules =
    {
        ("FromSqlRawAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("FromSqlInterpolatedAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("FromSqlRaw(", "raw-sql", "Replace EF Core raw SQL queries with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound migration finding."),
        ("FromSqlInterpolated(", "raw-sql", "Replace EF Core raw SQL queries with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound migration finding."),
        ("ExecuteSqlRaw(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("ExecuteSqlInterpolated(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("ExecuteSql(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("ExecuteSqlAsync(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("migrationBuilder.Sql(", "raw-sql", "Inventory hand-authored EF migration SQL and replace portable schema changes with nORM migrations; keep remaining SQL as explicit per-provider migration work."),
        ("using Dapper", "raw-sql", "Inventory Dapper SQL and rewrite provider-mobile paths to generated nORM LINQ/write APIs; keep remaining SQL as provider-bound migration work."),
        ("SqlMapper.", "raw-sql", "Inventory Dapper SQL and rewrite provider-mobile paths to generated nORM LINQ/write APIs; keep remaining SQL as provider-bound migration work."),
        ("QueryUnchangedAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("QueryUnchangedInterpolatedAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("ExecuteStoredProcedure", "stored-procedure", "Move data access semantics to generated nORM LINQ/write APIs where possible. If the procedure contains business logic, flag it for human rewrite or provider-specific deployment."),
        ("SqlFunction", "custom-sql-function", "Replace custom SQL fragments with built-in nORM/provider translations or keep them outside strict certification with per-provider evidence."),
        ("Regex.IsMatch(", "constrained-linq-shape", "Regex.IsMatch has an all-four simple subset (literal text, ^/$ anchors, simple ASCII bracket classes, \\d, \\w); Regex.Replace has an all-four literal-pattern/literal-replacement subset. Keep regex LINQ inside the documented subset or add provider-owned live evidence."),
        ("Regex.Replace(", "constrained-linq-shape", "Regex.IsMatch has an all-four simple subset (literal text, ^/$ anchors, simple ASCII bracket classes, \\d, \\w); Regex.Replace has an all-four literal-pattern/literal-replacement subset. Keep regex LINQ inside the documented subset or add provider-owned live evidence."),
        (".TakeWhile(", "constrained-linq-shape", "TakeWhile is provider-mobile only with an explicit OrderBy/ThenBy sequence and the one-argument or index-aware predicate overload. Unordered and post-Take/Skip forms fail closed."),
        (".SkipWhile(", "constrained-linq-shape", "SkipWhile is provider-mobile only with an explicit OrderBy/ThenBy sequence and the one-argument or index-aware predicate overload. Unordered and post-Take/Skip forms fail closed."),
        (".SequenceEqual(", "constrained-linq-shape", "SequenceEqual is provider-mobile when queryable sources are explicitly ordered and the default equality comparer is used; local mapped-entity second sequences are parameterized as ordered derived tables. Unordered queryable sources and comparer overloads fail closed."),
        ("CompileTimeQuery", "compile-time-raw-sql", "Replace raw SQL [CompileTimeQuery] methods with Norm.CompileQuery LINQ where possible. Keep provider-specific SQL outside strict provider mobility certification."),
        ("CreateCompiledQueryCommandAsync", "compile-time-raw-sql", "Use Norm.CompileQuery LINQ for provider-mobile compiled queries. Direct command construction is a provider-bound escape hatch."),
        ("ctx.Connection", "direct-connection", "Use generated nORM APIs. Direct DbContext.Connection access is caller-owned provider language and cannot be certified as provider-mobile."),
        ("context.Connection", "direct-connection", "Use generated nORM APIs. Direct DbContext.Connection access is caller-owned provider language and cannot be certified as provider-mobile."),
        ("db.Connection", "direct-connection", "Use generated nORM APIs. Direct DbContext.Connection access is caller-owned provider language and cannot be certified as provider-mobile."),
        ("ctx.Provider", "direct-provider-access", "Use generated nORM APIs. Direct DatabaseProvider access is provider-specific and cannot be certified as provider-mobile."),
        ("context.Provider", "direct-provider-access", "Use generated nORM APIs. Direct DatabaseProvider access is provider-specific and cannot be certified as provider-mobile."),
        ("db.Provider", "direct-provider-access", "Use generated nORM APIs. Direct DatabaseProvider access is provider-specific and cannot be certified as provider-mobile."),
        ("ctx.Query(\"", "dynamic-table-query", "Use typed Query<T>() with mapped entities so nORM owns provider translation and schema semantics."),
        ("context.Query(\"", "dynamic-table-query", "Use typed Query<T>() with mapped entities so nORM owns provider translation and schema semantics."),
        ("db.Query(\"", "dynamic-table-query", "Use typed Query<T>() with mapped entities so nORM owns provider translation and schema semantics."),
        ("CurrentTransaction", "direct-transaction-access", "Use nORM's DbContextTransaction wrapper for generated nORM operations. Direct DbTransaction access is provider-specific ADO.NET surface."),
        (".Transaction", "direct-transaction-access", "Use nORM's transaction wrapper without exposing the raw DbTransaction handle in strict provider-mobile code."),
        ("DbTransaction", "direct-transaction-access", "Use nORM's transaction wrapper for generated nORM operations. Raw DbTransaction usage is provider-specific ADO.NET surface."),
        ("DbConnection", "provider-bootstrap-connection", "Keep provider selection and connection construction in composition-root infrastructure; strict-certified data access should use generated nORM APIs."),
        ("DbCommand", "direct-command-access", "Use generated nORM query/write APIs. Direct DbCommand usage is caller-authored provider language."),
        ("CreateCommand(", "direct-command-access", "Use generated nORM query/write APIs. Direct command construction is caller-authored provider language."),
        ("SqlCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("NpgsqlCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("MySqlCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("SqliteCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("SqlDataAdapter", "direct-command-access", "Replace provider-specific data adapters with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("NpgsqlDataAdapter", "direct-command-access", "Replace provider-specific data adapters with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("MySqlDataAdapter", "direct-command-access", "Replace provider-specific data adapters with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("IDbCommandInterceptor", "command-interceptor", "Keep command interception outside strict certification because it can inspect, rewrite, or suppress generated provider commands."),
        ("CommandInterceptors", "command-interceptor", "Keep command interception outside strict certification because it can inspect, rewrite, or suppress generated provider commands."),
        ("SqlConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("NpgsqlConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("MySqlConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("SqliteConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("UseSqlServer(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("UseNpgsql(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("UseMySql(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("UseSqlite(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("Microsoft.Data.SqlClient", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("Microsoft.Data.Sqlite", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("Npgsql", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("MySqlConnector", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("MySql.Data", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("EnableNativeTenantSessionContext", "provider-native-tenant-security", "Use generated-path tenant enforcement for provider mobility; keep native RLS/session context as explicit defense-in-depth deployment work."),
        ("TemporalStorageMode.ProviderNative", "provider-native-temporal", "Use nORM-managed temporal history for provider mobility; keep provider-native temporal mode as SQL Server-specific deployment work."),
        ("ProviderNative", "provider-native-temporal", "Use nORM-managed temporal history for provider mobility; keep provider-native temporal mode as provider-specific deployment work."),
        ("ClientEvaluationPolicy.Allow", "client-evaluation", "Translate the expression to supported nORM LINQ or switch to ClientEvaluationPolicy.Warn for explicit top-level projection tails after server filtering/paging."),
        ("ClientEvaluationPolicy.Warn", "client-projection-tail", "Keep this as an explicit, reviewed projection-tail choice: server filters, ordering and paging must run before the client projection."),
        ("TypeName =", "provider-specific-column-type", "Use CLR type mapping and nORM migrations for provider-mobile schema. Provider-specific column type strings must stay outside strict certification."),
        ("TypeName=", "provider-specific-column-type", "Use CLR type mapping and nORM migrations for provider-mobile schema. Provider-specific column type strings must stay outside strict certification."),
        ("HasColumnType", "provider-specific-column-type", "Use CLR type mapping and nORM migrations for provider-mobile schema. Provider-specific column type strings must stay outside strict certification."),
        ("HasDefaultValueSql", "schema-provider-specific-default", "Replace provider SQL defaults with application-stamped values, simple literals, or explicit provider-specific migrations outside strict certification."),
        ("HasComputedColumnSql", "schema-provider-specific-default", "Computed column SQL is provider language. Replace with generated/application logic or keep as provider-specific migration work."),
        ("HasIdentityOptions", "schema-provider-specific-default", "Identity seed/increment metadata is provider DDL. Keep it as reviewed provider-specific migration work outside strict certification."),
        ("UseCollation(", "schema-provider-specific-default", "Inventory provider-specific collation choices and replace them with provider-neutral comparison semantics or reviewed per-provider migration work."),
        ("HasCollation(", "schema-provider-specific-default", "Inventory provider-specific collation choices and replace them with provider-neutral comparison semantics or reviewed per-provider migration work."),
        ("Annotation(\"SqlServer:", "schema-provider-specific-default", "Inventory SQL Server-specific EF migration annotations and replace them with provider-neutral nORM schema metadata where possible."),
        ("Annotation(\"Npgsql:", "schema-provider-specific-default", "Inventory PostgreSQL-specific EF migration annotations and replace them with provider-neutral nORM schema metadata where possible."),
        ("Annotation(\"MySql:", "schema-provider-specific-default", "Inventory MySQL-specific EF migration annotations and replace them with provider-neutral nORM schema metadata where possible."),
        ("SqlServerValueGenerationStrategy", "schema-provider-specific-default", "Inventory SQL Server-specific EF value generation annotations and replace them with provider-neutral nORM identity/schema metadata where possible."),
        ("NpgsqlValueGenerationStrategy", "schema-provider-specific-default", "Inventory PostgreSQL-specific EF value generation annotations and replace them with provider-neutral nORM identity/schema metadata where possible."),
        ("MySqlValueGenerationStrategy", "schema-provider-specific-default", "Inventory MySQL-specific EF value generation annotations and replace them with provider-neutral nORM identity/schema metadata where possible.")
    };

    private static readonly (string Pattern, string Kind, string SuggestedFix)[] SqlRules =
    {
        ("CREATE PROCEDURE", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("CREATE PROC", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("ALTER PROCEDURE", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("ALTER PROC", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("WITH (NOLOCK)", "sql-server-specific-sql", "Replace SQL Server locking hints with nORM/provider-neutral query semantics or document a provider-specific concurrency design."),
        ("GETDATE()", "sql-server-specific-sql", "Use nORM-generated temporal/date expressions or provider-neutral application timestamps where equivalent semantics are required."),
        ("DATEADD(", "sql-server-specific-sql", "Use supported nORM DateTime/TimeSpan LINQ translation where possible; otherwise rewrite per provider with explicit evidence."),
        ("FOR JSON", "sql-server-specific-sql", "Use application-side JSON projection or provider-specific reviewed SQL outside strict provider mobility certification.")
    };

    private static readonly (string Pattern, string Kind, string SuggestedFix)[] ProjectRules =
    {
        ("Dapper", "raw-sql", "Inventory Dapper package usage and rewrite provider-mobile data access to generated nORM LINQ/write APIs; keep remaining Dapper SQL outside strict certification."),
        ("Microsoft.EntityFrameworkCore.SqlServer", "provider-specific-package", "Inventory EF Core SQL Server provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Microsoft.EntityFrameworkCore.Sqlite", "provider-specific-package", "Inventory EF Core SQLite provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Npgsql.EntityFrameworkCore.PostgreSQL", "provider-specific-package", "Inventory EF Core PostgreSQL provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Pomelo.EntityFrameworkCore.MySql", "provider-specific-package", "Inventory EF Core MySQL/MariaDB provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("MySql.EntityFrameworkCore", "provider-specific-package", "Inventory EF Core MySQL provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Microsoft.Data.SqlClient", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("System.Data.SqlClient", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("Microsoft.Data.Sqlite", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("Npgsql", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("MySqlConnector", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("MySql.Data", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions.")
    };

    public static ProviderMobilityScanResult Scan(string rootPath)
    {
        var fullRoot = Path.GetFullPath(rootPath);
        if (!Directory.Exists(fullRoot) && !File.Exists(fullRoot))
        {
            return new ProviderMobilityScanResult(fullRoot, 0, new[]
            {
                new ProviderMobilityFinding(
                    fullRoot,
                    0,
                    "scan-path-missing",
                    "Error",
                    "The requested portability scan path does not exist.",
                    "Pass a valid application source directory or file.")
            });
        }

        var files = File.Exists(fullRoot)
            ? new[] { fullRoot }
            : Directory.EnumerateFiles(fullRoot, "*.*", SearchOption.AllDirectories)
                .Where(path => !IsGeneratedOrBuildOutput(fullRoot, path))
                .Where(static path =>
                    path.EndsWith(".cs", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".props", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".targets", StringComparison.OrdinalIgnoreCase))
                .ToArray();

        var findings = new List<ProviderMobilityFinding>();
        foreach (var file in files)
            ScanFile(fullRoot, file, findings);

        return new ProviderMobilityScanResult(fullRoot, files.Length, findings);
    }

    private static void ScanFile(string root, string file, List<ProviderMobilityFinding> findings)
    {
        if (IsProjectFile(file) && TryScanProjectFile(root, file, findings))
            return;

        var rules = RulesForFile(file);
        var lines = File.ReadAllLines(file);
        var inBlockComment = false;
        for (var i = 0; i < lines.Length; i++)
        {
            var line = StripBlockComments(lines[i], ref inBlockComment);
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith("--", StringComparison.Ordinal))
                continue;

            foreach (var rule in rules)
            {
                if (!line.Contains(rule.Pattern, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (rule.Pattern == "ProviderNative" && line.Contains("TemporalStorageMode.ProviderNative", StringComparison.Ordinal))
                    continue;

                AddFinding(root, file, i + 1, rule, findings);
            }
        }
    }

    private static bool TryScanProjectFile(string root, string file, List<ProviderMobilityFinding> findings)
    {
        try
        {
            var document = XDocument.Load(file, LoadOptions.SetLineInfo);
            foreach (var package in document.Descendants().Where(static element =>
                         element.Name.LocalName is "PackageReference" or "PackageVersion"))
            {
                var packageId = (string?)package.Attribute("Include") ?? (string?)package.Attribute("Update");
                if (string.IsNullOrWhiteSpace(packageId))
                    continue;

                foreach (var rule in ProjectRules)
                {
                    if (!packageId.Equals(rule.Pattern, StringComparison.OrdinalIgnoreCase))
                        continue;

                    AddFinding(
                        root,
                        file,
                        package is IXmlLineInfo lineInfo && lineInfo.HasLineInfo() ? lineInfo.LineNumber : 0,
                        rule,
                        findings);
                }
            }

            return true;
        }
        catch (XmlException)
        {
            return false;
        }
    }

    private static void AddFinding(
        string root,
        string file,
        int line,
        (string Pattern, string Kind, string SuggestedFix) rule,
        List<ProviderMobilityFinding> findings)
    {
        var reason = $"Provider-bound usage '{rule.Pattern}' is outside strict provider mobility certification.";
        var suggestedFix = rule.SuggestedFix;
        var severity = SeverityFor(rule.Kind);
        if (ProviderMobilityTranslator.TryDecideFindingKind(rule.Kind, out var decision))
        {
            reason = decision.Reason;
            suggestedFix = rule.SuggestedFix.Equals(decision.SuggestedFix, StringComparison.Ordinal)
                ? decision.SuggestedFix
                : decision.SuggestedFix + " Pattern-specific remediation: " + rule.SuggestedFix;
            severity = decision.CertificationSeverity;
        }

        findings.Add(new ProviderMobilityFinding(
            Path.GetRelativePath(root, file),
            line,
            rule.Kind,
            severity,
            reason,
            suggestedFix));
    }

    private static (string Pattern, string Kind, string SuggestedFix)[] RulesForFile(string file)
    {
        if (file.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            return SqlRules;
        if (IsProjectFile(file))
            return ProjectRules;

        return CSharpRules;
    }

    private static bool IsProjectFile(string file)
        => file.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase) ||
           file.EndsWith(".props", StringComparison.OrdinalIgnoreCase) ||
           file.EndsWith(".targets", StringComparison.OrdinalIgnoreCase);

    private static string StripBlockComments(string line, ref bool inBlockComment)
    {
        var current = line;
        while (true)
        {
            if (inBlockComment)
            {
                var end = current.IndexOf("*/", StringComparison.Ordinal);
                if (end < 0)
                    return string.Empty;

                current = current[(end + 2)..];
                inBlockComment = false;
            }

            var start = current.IndexOf("/*", StringComparison.Ordinal);
            if (start < 0)
                return current;

            var close = current.IndexOf("*/", start + 2, StringComparison.Ordinal);
            if (close < 0)
            {
                inBlockComment = true;
                return current[..start];
            }

            current = current.Remove(start, close - start + 2);
        }
    }

    private static string SeverityFor(string kind)
        => kind is "provider-specific-package" or "provider-bootstrap-connection" or "client-projection-tail"
            ? "Warning"
            : "Error";

    private static bool IsGeneratedOrBuildOutput(string root, string path)
    {
        var normalized = Path.GetRelativePath(root, path).Replace('\\', '/');
        return normalized.StartsWith("bin/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/bin/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("obj/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/obj/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("artifacts/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/artifacts/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith(".git/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/.git/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith(".tmp/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/.tmp/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("node_modules/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/node_modules/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("packages/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/packages/", StringComparison.OrdinalIgnoreCase) ||
               normalized.EndsWith(".g.cs", StringComparison.OrdinalIgnoreCase) ||
               normalized.EndsWith(".designer.cs", StringComparison.OrdinalIgnoreCase);
    }
}
