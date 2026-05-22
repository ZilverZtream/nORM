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

    private static void RunDotNet(string arguments, string workingDirectory, string? packageCache)
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
