using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using nORM.Cli;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public class CliIntegrationTests
{
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
                $"migrations add WeirdMigration --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.Equal(0, result.ExitCode);

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
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
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
