using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    [Fact]
    public void Scaffold_connection_option_and_positional_provider_generate_model()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_mixed_positional_provider_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_mixed_positional_provider_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} sqlite --output-dir {Quote(output)} -n CliScaffolded -c CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_explicit_connection_and_provider_options_override_positionals()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_explicit_precedence_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_explicit_precedence_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Data Source=ignored.db")} notaprovider --connection {Quote($"Data Source={dbFile}")} --provider Microsoft.EntityFrameworkCore.Sqlite --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
        }
        finally
        {
            TryDeleteDirectory(output);
            try { File.Delete(dbFile); } catch { }
            try { File.Delete(Path.Combine(root, "ignored.db")); } catch { }
        }
    }

    [Fact]
    public void Scaffold_existing_output_requires_force_by_default()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_no_overwrite_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_no_overwrite_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Directory.CreateDirectory(output);
            var existingFile = Path.Combine(output, "Customer.cs");
            File.WriteAllText(existingFile, "// keep me");

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("already exists", result.Stderr + result.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("// keep me", File.ReadAllText(existingFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_force_and_no_overwrite_conflict_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_force_conflict_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_force_conflict_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} -o {Quote(output)} --namespace CliScaffolded --context CliCtx --force --no-overwrite",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("--force", result.Stderr, StringComparison.Ordinal);
            Assert.Contains("--no-overwrite", result.Stderr, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_schema_filter_generates_matching_schema_tables()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE SchemaFiltered (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --schema main",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "SchemaFiltered.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[Table(\"SchemaFiltered\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SchemaFiltered> SchemaFilteredRows", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_schema_filter_accepts_multiple_values_after_single_option()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_multi_value_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_multi_value_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE SchemaFiltered (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --schema main main",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "SchemaFiltered.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[Table(\"SchemaFiltered\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SchemaFiltered> SchemaFilteredRows", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_pluralize_generates_singular_query_properties()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_pluralize_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_pluralize_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Category (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-pluralize",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("IQueryable<Category> Category", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("IQueryable<Category> Categories", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_relationships_omits_navigation_properties_and_model_relationships()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_relationships_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_relationships_out_" + Guid.NewGuid().ToString("N"));

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
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE Book (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Author_Id INTEGER NOT NULL,
                        Title TEXT NOT NULL,
                        FOREIGN KEY (Author_Id) REFERENCES Author(Id)
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-relationships",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var authorCode = File.ReadAllText(Path.Combine(output, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.DoesNotContain("List<Book>", authorCode, StringComparison.Ordinal);
            Assert.Contains("AuthorId", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain("public Author Author", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasForeignKey(", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_context_dir_and_namespace_generate_split_context()
    {
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_out_" + Guid.NewGuid().ToString("N"));
        var workDir = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_work_" + Guid.NewGuid().ToString("N"));

        try
        {
            Directory.CreateDirectory(workDir);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE ContextPlaced (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded.Entities --context CliCtx --context-dir Data/Contexts --context-namespace CliScaffolded.Contexts",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "ContextPlaced.cs"));
            var contextCode = File.ReadAllText(Path.Combine(workDir, "Data", "Contexts", "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace CliScaffolded.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using CliScaffolded.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ContextPlaced> ContextPlacedRows", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
            TryDeleteDirectory(workDir);
        }
    }

    [Fact]
    public void Scaffold_context_dir_absolute_path_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_absolute_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_absolute_out_" + Guid.NewGuid().ToString("N"));
        var absoluteContextDir = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_absolute_ctx_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --context-dir {Quote(absoluteContextDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Scaffold --context-dir must be a relative path", result.Stderr, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
            Assert.False(Directory.Exists(absoluteContextDir));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
            TryDeleteDirectory(absoluteContextDir);
        }
    }

    [Fact]
    public void Scaffold_invalid_namespace_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_invalid_namespace_out_" + Guid.NewGuid().ToString("N"));

        var result = RunCli(
            $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} --output {Quote(output)} --namespace Bad-Name --context CliCtx",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Scaffold --namespace 'Bad-Name' is not a valid C# namespace", result.Stderr, StringComparison.Ordinal);
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void Scaffold_blank_explicit_string_options_fail_before_writing()
    {
        var root = FindRepositoryRoot();
        var cases = new[]
        {
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_namespace_" + Guid.NewGuid().ToString("N")))} --namespace {Quote(" ")} --context CliCtx",
                Message: "Scaffold --namespace must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_context_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context {Quote(" ")}",
                Message: "Scaffold --context must not be blank."
            ),
            (
                Arguments: $"--output {Quote(" ")} --namespace CliScaffolded --context CliCtx",
                Message: "Scaffold --output must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_project_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --project {Quote(" ")}",
                Message: "Scaffold --project must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_startup_project_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --startup-project {Quote(" ")}",
                Message: "Scaffold --startup-project must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_framework_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --framework {Quote(" ")}",
                Message: "Scaffold --framework must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_target_framework_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --target-framework {Quote(" ")}",
                Message: "Scaffold --framework must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_configuration_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --configuration {Quote(" ")}",
                Message: "Scaffold --configuration must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_runtime_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --runtime {Quote(" ")}",
                Message: "Scaffold --runtime must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_msbuild_ext_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context CliCtx --msbuildprojectextensionspath {Quote(" ")}",
                Message: "Scaffold --msbuildprojectextensionspath must not be blank."
            )
        };

        foreach (var (arguments, message) in cases)
        {
            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} {arguments}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(message, result.Stderr, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Scaffold_invalid_context_namespace_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_invalid_context_namespace_out_" + Guid.NewGuid().ToString("N"));

        var result = RunCli(
            $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --context-namespace Bad-Name",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Scaffold --context-namespace 'Bad-Name' is not a valid C# namespace", result.Stderr, StringComparison.Ordinal);
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void Scaffold_invalid_explicit_context_name_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_invalid_context_name_out_" + Guid.NewGuid().ToString("N"));

        var result = RunCli(
            $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} --output {Quote(output)} --namespace CliScaffolded --context Bad-Name",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Scaffold --context class name 'Bad-Name' is not a valid C# type identifier", result.Stderr, StringComparison.Ordinal);
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void Scaffold_qualified_context_name_derives_context_namespace()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_qualified_context_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_qualified_context_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE ContextQualified (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded.Entities --context CliScaffolded.Contexts.CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "ContextQualified.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace CliScaffolded.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using CliScaffolded.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliCtx", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_omitted_context_name_derives_from_sqlite_database_file()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_context_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "customer-orders.db");
        var output = Path.Combine(tempRoot, "Models");

        try
        {
            Directory.CreateDirectory(tempRoot);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CustomerOrdersContext.cs"));
            Assert.Contains("public partial class CustomerOrdersContext", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_use_database_names_preserves_legal_clr_names()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_database_names_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_database_names_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE order_line (
                        order_id INTEGER PRIMARY KEY,
                        SKU TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --use-database-names",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "order_line.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("public partial class order_line", entityCode, StringComparison.Ordinal);
            Assert.Contains("public long order_id { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string SKU { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<order_line> order_lines", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OrderLine", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OrderId", entityCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_onconfiguring_is_accepted_without_emitting_onconfiguring()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_onconfiguring_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_onconfiguring_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-onconfiguring",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.DoesNotContain("OnConfiguring", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Data Source=", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

}
