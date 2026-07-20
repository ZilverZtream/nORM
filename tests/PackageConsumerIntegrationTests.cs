using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Xunit;

namespace nORM.Tests;

// Package-consumer tests all run `dotnet pack` on the runtime project, which touches the same
// nupkg outputs. The xUnit collection serializes them so parallel test execution cannot race
// the pack outputs and corrupt each other.
[Collection("PackageConsumerSerial")]
[Trait("Category", TestCategory.PackageConsumer)]
public class PackageConsumerIntegrationTests
{
    private static string PackageVersion => ReadPackageVersion(FindRepositoryRoot());

    [Fact]
    public void Runtime_package_contains_source_generator_analyzer()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);
        var packagePath = Path.Combine(root, "src", "bin", "Release", $"TheNorm.{PackageVersion}.nupkg");

        using var archive = ZipFile.OpenRead(packagePath);
        Assert.Contains(archive.Entries, entry =>
            string.Equals(entry.FullName, "analyzers/dotnet/cs/nORM.SourceGenerators.dll", StringComparison.Ordinal));
    }

    [Fact]
    public void Runtime_package_contains_release_assets_metadata_and_symbols()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);
        var packagePath = Path.Combine(root, "src", "bin", "Release", $"TheNorm.{PackageVersion}.nupkg");
        var symbolsPath = Path.Combine(root, "src", "bin", "Release", $"TheNorm.{PackageVersion}.snupkg");

        using (var archive = ZipFile.OpenRead(packagePath))
        {
            AssertPackageEntry(archive, "lib/net8.0/nORM.dll");
            AssertPackageEntry(archive, "lib/net8.0/nORM.xml");
            AssertPackageEntry(archive, "README.md");
            AssertPackageEntry(archive, "analyzers/dotnet/cs/nORM.SourceGenerators.dll");

            var nuspec = ReadEntryText(archive, "TheNorm.nuspec");
            Assert.Contains("<license type=\"expression\">MIT</license>", nuspec, StringComparison.Ordinal);
            Assert.Contains("<readme>README.md</readme>", nuspec, StringComparison.Ordinal);
            Assert.Contains("<repository type=\"git\" url=\"https://github.com/zilverztream/nORM\"", nuspec, StringComparison.Ordinal);
            // Tags feed NuGet discoverability. Assert the important tags are present without pinning
            // the exact set or order, so adding a tag (e.g. brand or provider-mobility terms) does not
            // silently break the metadata contract the way an exact-string match does.
            var tagsStart = nuspec.IndexOf("<tags>", StringComparison.Ordinal);
            var tagsEnd = nuspec.IndexOf("</tags>", StringComparison.Ordinal);
            Assert.True(tagsStart >= 0 && tagsEnd > tagsStart, "TheNorm.nuspec is missing a <tags> element");
            var tags = nuspec
                .Substring(tagsStart + "<tags>".Length, tagsEnd - tagsStart - "<tags>".Length)
                .Split(' ', StringSplitOptions.RemoveEmptyEntries);
            foreach (var required in new[] { "TheNorm", "ORM", "Database", "LINQ", "SQLite", "PostgreSQL", "MySQL", "provider-mobile" })
                Assert.Contains(required, tags);
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
            Assert.Contains(generatedSources, source => source.Contains("CompiledMaterializerStore.AddPermanent<global::PackageUser>", StringComparison.Ordinal));
            Assert.Contains(generatedSources, source => source.Contains("GetUsers(", StringComparison.Ordinal));
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Package_reference_consumer_gets_source_generation_limit_diagnostics()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);

        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_pkg_diag_" + Guid.NewGuid().ToString("N"));
        var packageCache = Path.Combine(tempRoot, "packages");
        Directory.CreateDirectory(tempRoot);
        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "Consumer.csproj"), ConsumerProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Program.cs"), UnsupportedMaterializerProgram, Encoding.UTF8);

            RunDotNet("restore --no-cache", tempRoot, packageCache);
            var result = RunDotNetAllowFailure("build -c Release --no-restore", tempRoot, packageCache);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("nORMSG005", result.Output, StringComparison.Ordinal);
            Assert.Contains("Computed", result.Output, StringComparison.Ordinal);
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

    [Fact]
    public void Package_outputs_are_cleaned_before_pack()
    {
        var root = FindRepositoryRoot();
        EnsureRuntimePackage(root);
        EnsureToolPackage(root);

        AssertCurrentPackagesOnly(
            Path.Combine(root, "src", "bin", "Release"),
            "TheNorm",
            PackageVersion);
        AssertCurrentPackagesOnly(
            Path.Combine(root, "src", "dotnet-norm", "bin", "Release"),
            "dotnet-norm",
            PackageVersion);
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
                <PackageReference Include="TheNorm" Version="{{PackageVersion}}" />
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

    private const string UnsupportedMaterializerProgram = """"
        using System;
        using nORM.SourceGeneration;

        Console.WriteLine(typeof(UnsupportedUser).Name);

        [GenerateMaterializer]
        public sealed class UnsupportedUser
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
            public string Computed => Name.ToUpperInvariant();
        }
        """";

    private static void EnsureRuntimePackage(string root)
    {
        CleanPackageOutput(Path.Combine(root, "src", "bin", "Release"), "TheNorm");
        RunDotNet("pack src/nORM.csproj -c Release --no-restore --nologo", root, null);
    }

    private static void EnsureToolPackage(string root)
    {
        CleanPackageOutput(Path.Combine(root, "src", "dotnet-norm", "bin", "Release"), "dotnet-norm");
        RunDotNet("pack src/dotnet-norm/dotnet-norm.csproj -c Release --no-restore --nologo", root, null);
    }

    private static string ReadPackageVersion(string root)
    {
        var props = XDocument.Load(Path.Combine(root, "Directory.Build.props"));
        return props.Descendants("NormVersion").Single().Value;
    }

    private static void CleanPackageOutput(string directory, string packageId)
    {
        if (!Directory.Exists(directory))
            return;

        // A previous test or an interrupted gate run can leave another process holding an open
        // handle to the .nupkg (NuGet restore cache, parallel pack, or a stale testhost). Retry
        // the delete with exponential backoff before failing the test so a transient lock does
        // not poison the suite.
        foreach (var package in Directory.EnumerateFiles(directory, $"{packageId}.*.nupkg")
                     .Concat(Directory.EnumerateFiles(directory, $"{packageId}.*.snupkg"))
                     .ToList())
        {
            var attempts = 0;
            while (true)
            {
                try
                {
                    File.Delete(package);
                    break;
                }
                catch (IOException) when (attempts < 8)
                {
                    System.Threading.Thread.Sleep(250 * (attempts + 1));
                    attempts++;
                }
                catch (UnauthorizedAccessException) when (attempts < 8)
                {
                    System.Threading.Thread.Sleep(250 * (attempts + 1));
                    attempts++;
                }
            }
        }
    }

    private static void AssertCurrentPackagesOnly(string directory, string packageId, string version)
    {
        var expected = new[]
        {
            $"{packageId}.{version}.nupkg",
            $"{packageId}.{version}.snupkg"
        };

        var actual = Directory.EnumerateFiles(directory, $"{packageId}.*.*nupkg")
            .Select(Path.GetFileName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(expected.OrderBy(name => name, StringComparer.Ordinal), actual);
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

    private static (int ExitCode, string Output) RunDotNetAllowFailure(string arguments, string workingDirectory, string? packageCache)
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
        return (process.ExitCode, stdout + stderr);
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
