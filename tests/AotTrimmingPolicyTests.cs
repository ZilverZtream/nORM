using System;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using nORM.Core;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class AotTrimmingPolicyTests
{
    [Fact]
    public void Dynamic_table_query_is_annotated_for_aot_and_trimming()
    {
        var method = typeof(DbContext).GetMethod(nameof(DbContext.Query), new[] { typeof(string) })
            ?? throw new MissingMethodException(nameof(DbContext), nameof(DbContext.Query));

        Assert.NotNull(method.GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(method.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
    }

    [Fact]
    public void Dynamic_entity_generator_is_annotated_for_aot_and_trimming()
    {
        Assert.NotNull(typeof(DynamicEntityTypeGenerator).GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(typeof(DynamicEntityTypeGenerator).GetCustomAttribute<RequiresUnreferencedCodeAttribute>());

        var methods = typeof(DynamicEntityTypeGenerator)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m.Name is nameof(DynamicEntityTypeGenerator.GenerateEntityType) or nameof(DynamicEntityTypeGenerator.GenerateEntityTypeAsync))
            .ToArray();

        Assert.NotEmpty(methods);
        foreach (var method in methods)
        {
            Assert.NotNull(method.GetCustomAttribute<RequiresDynamicCodeAttribute>());
            Assert.NotNull(method.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
        }
    }

    [Fact]
    public void Dynamic_table_query_annotation_message_mentions_aot()
    {
        // The RequiresDynamicCode message must mention AOT or NativeAOT so callers
        // have actionable context when the build warning fires.
        var method = typeof(DbContext).GetMethod(nameof(DbContext.Query), new[] { typeof(string) })
            ?? throw new MissingMethodException(nameof(DbContext), nameof(DbContext.Query));

        var dynAttr = method.GetCustomAttribute<RequiresDynamicCodeAttribute>();
        Assert.NotNull(dynAttr);
        Assert.Contains("AOT", dynAttr!.Message, StringComparison.OrdinalIgnoreCase);

        var trimAttr = method.GetCustomAttribute<RequiresUnreferencedCodeAttribute>();
        Assert.NotNull(trimAttr);
        Assert.Contains("trim", trimAttr!.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Dynamic_entity_generator_annotation_messages_mention_aot_and_trim()
    {
        // Type-level annotations must provide actionable messages.
        var typeRdc = typeof(DynamicEntityTypeGenerator).GetCustomAttribute<RequiresDynamicCodeAttribute>();
        Assert.NotNull(typeRdc);
        Assert.Contains("AOT", typeRdc!.Message, StringComparison.OrdinalIgnoreCase);

        var typeRuc = typeof(DynamicEntityTypeGenerator).GetCustomAttribute<RequiresUnreferencedCodeAttribute>();
        Assert.NotNull(typeRuc);
        Assert.Contains("trim", typeRuc!.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NormUnsupportedFeatureException_is_public_and_carries_message()
    {
        // Verify the exception taxonomy type used for unsupported feature paths is
        // publicly accessible and preserves the caller-supplied message, so that
        // future AOT/trimming runtime guards can surface actionable diagnostics.
        var ex = new NormUnsupportedFeatureException("AOT and trimming are not supported for this path.");
        Assert.Contains("AOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsAssignableFrom<NormException>(ex);
    }

    [Fact]
    public void PublishTrimmed_smoke_fails_with_trim_diagnostics_for_v1_runtime_package()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_trim_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "TrimSmoke.csproj"), TrimSmokeProject(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Program.cs"), TrimSmokeProgram, Encoding.UTF8);

            var result = RunDotNet("publish -c Release --nologo", tempRoot);

            Assert.NotEqual(0, result.ExitCode);
            Assert.True(
                result.Output.Contains("IL2", StringComparison.OrdinalIgnoreCase) ||
                result.Output.Contains("NETSDK", StringComparison.OrdinalIgnoreCase) ||
                result.Output.Contains("CreateAppHost", StringComparison.OrdinalIgnoreCase) ||
                result.Output.Contains("trim", StringComparison.OrdinalIgnoreCase),
                result.Output);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    private static string TrimSmokeProject(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <OutputType>Exe</OutputType>
                <TargetFramework>net8.0</TargetFramework>
                <LangVersion>12.0</LangVersion>
                <Nullable>enable</Nullable>
                <RuntimeIdentifier>{{CurrentRuntimeIdentifier()}}</RuntimeIdentifier>
                <SelfContained>true</SelfContained>
                <UseAppHost>true</UseAppHost>
                <PublishTrimmed>true</PublishTrimmed>
                <TrimMode>partial</TrimMode>
                <SuppressTrimAnalysisWarnings>false</SuppressTrimAnalysisWarnings>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string TrimSmokeProgram = """
        using System;
        using System.ComponentModel.DataAnnotations;
        using System.Linq;
        using Microsoft.Data.Sqlite;
        using nORM.Core;
        using nORM.Providers;

        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TrimUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO TrimUser (Name) VALUES ('Ada');";
            await cmd.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());
        var users = await ctx.Query<TrimUser>().Where(u => u.Name == "Ada").ToListAsync();
        if (users.Count != 1)
            throw new InvalidOperationException($"Expected 1 row, got {users.Count}.");

        public sealed class TrimUser
        {
            [Key]
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }
        """;

    private static string CurrentRuntimeIdentifier()
    {
        var arch = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.Arm64 => "arm64",
            Architecture.X64 => "x64",
            Architecture.X86 => "x86",
            _ => "x64"
        };

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return $"win-{arch}";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return $"linux-{arch}";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            return $"osx-{arch}";

        return $"win-{arch}";
    }

    private static (int ExitCode, string Output) RunDotNet(string arguments, string workingDirectory)
    {
        var startInfo = new ProcessStartInfo("dotnet", arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Failed to start dotnet process.");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        return (process.ExitCode, stdout + stderr);
    }

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

    [Fact]
    public void LazyNavigationCollection_is_annotated_for_aot_and_trimming()
    {
        var t = typeof(nORM.Navigation.LazyNavigationCollection<object>);
        Assert.NotNull(t.GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(t.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
    }

    [Fact]
    public void LazyNavigationReference_is_annotated_for_aot_and_trimming()
    {
        var t = typeof(nORM.Navigation.LazyNavigationReference<object>);
        Assert.NotNull(t.GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(t.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
    }

    [Fact]
    public void CacheableExtensions_Cacheable_is_annotated_for_aot_and_trimming()
    {
        var method = typeof(CacheableExtensions).GetMethod(nameof(CacheableExtensions.Cacheable))
            ?? throw new MissingMethodException(nameof(CacheableExtensions), nameof(CacheableExtensions.Cacheable));
        Assert.NotNull(method.GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(method.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
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
