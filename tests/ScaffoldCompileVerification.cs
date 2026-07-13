using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Compiles a scaffolded output directory IN PROCESS with Roslyn instead of shelling
/// out to <c>dotnet build</c>. The temp projects the scaffold tests write reference
/// nORM by direct DLL path, so the only thing a real build added was MSBuild
/// evaluation and implicit restore — which stall for minutes under machine
/// contention and were the reason live suites ran hours of wall-clock over ~17
/// minutes of test time. The directory's own .csproj is honored: Nullable,
/// ImplicitUsings (the SDK's default global usings are injected), WarningsAsErrors,
/// and DLL Reference HintPaths. Tests that exercise real MSBuild semantics (startup
/// project and configuration resolution, CLI integration smoke) keep real builds.
/// </summary>
internal static class ScaffoldCompileVerification
{
    private static readonly Lazy<IReadOnlyList<MetadataReference>> RuntimeReferences = new(() =>
    {
        var tpa = (string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!;
        return tpa.Split(Path.PathSeparator)
            .Where(p => p.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToArray();
    });

    private static readonly ConcurrentDictionary<string, MetadataReference> HintPathReferences =
        new(StringComparer.OrdinalIgnoreCase);

    // The net8.0 SDK's default implicit global usings for the Microsoft.NET.Sdk project type.
    private const string ImplicitGlobalUsings = """
        global using global::System;
        global using global::System.Collections.Generic;
        global using global::System.IO;
        global using global::System.Linq;
        global using global::System.Net.Http;
        global using global::System.Threading;
        global using global::System.Threading.Tasks;
        """;

    /// <summary>
    /// Asserts that every .cs file under <paramref name="projectDirectory"/> compiles
    /// cleanly under the settings of the directory's .csproj.
    /// </summary>
    internal static void AssertCompiles(string projectDirectory)
    {
        var csprojFiles = Directory.GetFiles(projectDirectory, "*.csproj", SearchOption.TopDirectoryOnly);
        Assert.True(csprojFiles.Length == 1,
            $"Expected exactly one .csproj in '{projectDirectory}' but found {csprojFiles.Length}.");
        var csproj = XDocument.Load(csprojFiles[0]);

        string? Property(string name) => csproj.Descendants(name).FirstOrDefault()?.Value?.Trim();

        var nullableEnabled = string.Equals(Property("Nullable"), "enable", StringComparison.OrdinalIgnoreCase);
        var implicitUsings = string.Equals(Property("ImplicitUsings"), "enable", StringComparison.OrdinalIgnoreCase);
        var warningsAsErrors = (Property("WarningsAsErrors") ?? string.Empty)
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var treatAllWarningsAsErrors = string.Equals(Property("TreatWarningsAsErrors"), "true", StringComparison.OrdinalIgnoreCase);

        var references = new List<MetadataReference>();
        var referencedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var hintPath in csproj.Descendants("HintPath").Select(h => h.Value.Trim()))
        {
            var resolved = Path.IsPathRooted(hintPath)
                ? hintPath
                : Path.GetFullPath(Path.Combine(projectDirectory, hintPath));
            Assert.True(File.Exists(resolved), $"Referenced assembly not found: {resolved}");
            references.Add(HintPathReferences.GetOrAdd(resolved, p => MetadataReference.CreateFromFile(p)));
            referencedNames.Add(Path.GetFileName(resolved));
        }
        // Runtime references, skipping any assembly the csproj referenced explicitly so the
        // compilation never sees two assemblies with the same identity (CS1703).
        foreach (var reference in RuntimeReferences.Value)
        {
            if (reference.Display is { } display && referencedNames.Contains(Path.GetFileName(display)))
                continue;
            references.Add(reference);
        }

        var parseOptions = new CSharpParseOptions(LanguageVersion.Latest);
        var trees = Directory.GetFiles(projectDirectory, "*.cs", SearchOption.AllDirectories)
            .Select(f => CSharpSyntaxTree.ParseText(File.ReadAllText(f), parseOptions, path: f))
            .ToList();
        Assert.True(trees.Count > 0, $"No .cs files found under '{projectDirectory}'.");
        if (implicitUsings)
            trees.Add(CSharpSyntaxTree.ParseText(ImplicitGlobalUsings, parseOptions, path: "ImplicitUsings.g.cs"));

        var specificDiagnostics = warningsAsErrors.ToDictionary(
            id => id, _ => ReportDiagnostic.Error, StringComparer.OrdinalIgnoreCase);

        var compilation = CSharpCompilation.Create(
            "ScaffoldCompileCheck_" + Path.GetFileNameWithoutExtension(csprojFiles[0]),
            trees,
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithNullableContextOptions(nullableEnabled ? NullableContextOptions.Enable : NullableContextOptions.Disable)
                .WithGeneralDiagnosticOption(treatAllWarningsAsErrors ? ReportDiagnostic.Error : ReportDiagnostic.Default)
                .WithSpecificDiagnosticOptions(specificDiagnostics));

        using var peStream = new MemoryStream();
        var emit = compilation.Emit(peStream);
        if (!emit.Success)
        {
            var failures = emit.Diagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Take(20)
                .Select(d => d.ToString());
            Assert.Fail(
                $"Scaffolded output in '{projectDirectory}' failed to compile:{Environment.NewLine}" +
                string.Join(Environment.NewLine, failures));
        }
    }
}
