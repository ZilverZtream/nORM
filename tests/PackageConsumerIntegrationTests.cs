using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Xunit;

namespace nORM.Tests;

public class PackageConsumerIntegrationTests
{
    private const string PackageVersion = "0.9.0-preview.1";

    [Fact]
    public void Runtime_package_contains_source_generator_analyzer()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);
        var packagePath = Path.Combine(root, "src", "bin", "Release", $"nORM.{PackageVersion}.nupkg");

        using var archive = ZipFile.OpenRead(packagePath);
        Assert.Contains(archive.Entries, entry =>
            string.Equals(entry.FullName, "analyzers/dotnet/cs/nORM.SourceGenerators.dll", StringComparison.Ordinal));
    }

    [Fact]
    public void Runtime_package_contains_release_assets_metadata_and_symbols()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);
        var packagePath = Path.Combine(root, "src", "bin", "Release", $"nORM.{PackageVersion}.nupkg");
        var symbolsPath = Path.Combine(root, "src", "bin", "Release", $"nORM.{PackageVersion}.snupkg");

        using (var archive = ZipFile.OpenRead(packagePath))
        {
            AssertPackageEntry(archive, "lib/net8.0/nORM.dll");
            AssertPackageEntry(archive, "lib/net8.0/nORM.xml");
            AssertPackageEntry(archive, "README.md");
            AssertPackageEntry(archive, "analyzers/dotnet/cs/nORM.SourceGenerators.dll");

            var nuspec = ReadEntryText(archive, "nORM.nuspec");
            Assert.Contains("<license type=\"expression\">MIT</license>", nuspec, StringComparison.Ordinal);
            Assert.Contains("<readme>README.md</readme>", nuspec, StringComparison.Ordinal);
            Assert.Contains("<repository type=\"git\" url=\"https://github.com/zilverztream/nORM\"", nuspec, StringComparison.Ordinal);
            Assert.Contains("<tags>ORM Database LINQ Performance Entity Framework Dapper SQL SQLite PostgreSQL MySQL</tags>", nuspec, StringComparison.Ordinal);
            Assert.Contains("<dependency id=\"Microsoft.Data.SqlClient\"", nuspec, StringComparison.Ordinal);
            Assert.Contains("<dependency id=\"Microsoft.Data.Sqlite\"", nuspec, StringComparison.Ordinal);
            Assert.DoesNotContain("<dependency id=\"Npgsql\"", nuspec, StringComparison.Ordinal);
            Assert.DoesNotContain("<dependency id=\"MySqlConnector\"", nuspec, StringComparison.Ordinal);
            Assert.DoesNotContain("<dependency id=\"MySql.Data\"", nuspec, StringComparison.Ordinal);
        }

        using (var symbols = ZipFile.OpenRead(symbolsPath))
        {
            AssertPackageEntry(symbols, "lib/net8.0/nORM.pdb");
        }
    }

    [Fact]
    public void Package_reference_consumer_can_use_source_generation_features()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);

        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_pkg_" + Guid.NewGuid().ToString("N"));
        var packageCache = Path.Combine(tempRoot, "packages");
        Directory.CreateDirectory(tempRoot);
        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "Consumer.csproj"), ConsumerProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Program.cs"), ConsumerProgram, Encoding.UTF8);

            RunDotNet("restore --no-cache", tempRoot, packageCache);
            RunDotNet("build -c Release --no-restore", tempRoot, packageCache);
            RunDotNet(Quote(Path.Combine(tempRoot, "bin", "Release", "net8.0", "Consumer.dll")), tempRoot, packageCache);

            var generatedFiles = Directory.EnumerateFiles(Path.Combine(tempRoot, "Generated"), "*.g.cs", SearchOption.AllDirectories).ToList();
            var generatedSources = generatedFiles.Select(File.ReadAllText).ToList();
            Assert.Contains(generatedSources, source => source.Contains("CompiledMaterializerStore.Add<global::PackageUser>", StringComparison.Ordinal));
            Assert.Contains(generatedSources, source => source.Contains("GetUsers(", StringComparison.Ordinal));
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Tool_package_installs_from_nupkg_and_runs_help()
    {
        var root = FindRepositoryRoot();
        EnsureToolPackage(root);

        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_tool_pkg_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            var packageSource = Path.Combine(root, "src", "dotnet-norm", "bin", "Release");
            RunDotNet("new tool-manifest --force", tempRoot, null);
            RunDotNet($"tool install dotnet-norm --version {PackageVersion} --add-source {Quote(packageSource)} --ignore-failed-sources", tempRoot, null);
            var output = RunDotNet("tool run norm -- --help", tempRoot, null);

            Assert.Contains("Command line tools for the nORM ORM framework", output, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    private static void AssertPackageEntry(ZipArchive archive, string fullName)
    {
        Assert.Contains(archive.Entries, entry => string.Equals(entry.FullName, fullName, StringComparison.Ordinal));
    }

    private static string ReadEntryText(ZipArchive archive, string fullName)
    {
        var entry = archive.GetEntry(fullName) ?? throw new InvalidOperationException($"Package entry '{fullName}' was not found.");
        using var stream = entry.Open();
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static string ConsumerProjectXml(string root)
    {
        var packageSource = Path.Combine(root, "src", "bin", "Release");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <OutputType>Exe</OutputType>
                <TargetFramework>net8.0</TargetFramework>
                <LangVersion>12.0</LangVersion>
                <Nullable>enable</Nullable>
                <UseAppHost>false</UseAppHost>
                <EmitCompilerGeneratedFiles>true</EmitCompilerGeneratedFiles>
                <CompilerGeneratedFilesOutputPath>Generated</CompilerGeneratedFilesOutputPath>
                <RestoreAdditionalProjectSources>{{packageSource}}</RestoreAdditionalProjectSources>
              </PropertyGroup>
              <ItemGroup>
                <PackageReference Include="nORM" Version="{{PackageVersion}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string ConsumerProgram = """"
        using System;
        using System.Collections.Generic;
        using System.ComponentModel.DataAnnotations;
        using System.Threading.Tasks;
        using Microsoft.Data.Sqlite;
        using nORM.Core;
        using nORM.Providers;
        using nORM.SourceGeneration;

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                CREATE TABLE PackageUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO PackageUser (Name) VALUES ('Ada');
                """;
            await command.ExecuteNonQueryAsync();
        }

        using var context = new DbContext(connection, new SqliteProvider());
        var users = await PackageQueries.GetUsers(context);
        if (users.Count != 1 || users[0].Name != "Ada")
            throw new InvalidOperationException("Package consumer source-generation query returned unexpected data.");

        [GenerateMaterializer]
        public sealed class PackageUser
        {
            [Key]
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public static partial class PackageQueries
        {
            [CompileTimeQuery("SELECT Id, Name FROM PackageUser")]
            public static partial Task<List<PackageUser>> GetUsers(DbContext context);
        }
        """";

    private static void EnsureRuntimePackage(string root)
    {
        RunDotNet("pack src\\nORM.csproj -c Release --no-restore --nologo", root, null);
    }

    private static void EnsureToolPackage(string root)
    {
        RunDotNet("pack src\\dotnet-norm\\dotnet-norm.csproj -c Release --no-restore --nologo", root, null);
    }

    private static string RunDotNet(string arguments, string workingDirectory, string? packageCache)
    {
        var startInfo = new ProcessStartInfo("dotnet", arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        if (packageCache != null)
            startInfo.Environment["NUGET_PACKAGES"] = packageCache;

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Failed to start dotnet process.");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0,
            $"dotnet {arguments} failed with exit code {process.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{stderr}");

        return stdout + stderr;
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
}
