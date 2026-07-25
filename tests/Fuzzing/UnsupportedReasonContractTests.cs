using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using nORM.Core;
using Xunit;

namespace nORM.Tests.Fuzzing;

/// <summary>
/// Reason-code contract for <see cref="NormUnsupportedFeatureException"/> — the "code-derived support
/// contract" from the fuzzing vision. EVERY throw-site in src/nORM must carry a stable
/// <see cref="NormUnsupportedReason"/> code. Without this, a throw-site that begins rejecting a shape which
/// used to translate stays GREEN (an "unsupported" rejection has no test budget), silently eroding
/// capability; a required, stable reason code makes that erosion visible to the fuzz support-contract.
/// The scan covers the WHOLE production source tree (no allowlist): the moment a new throw-site is added
/// without a code, this test fails — no per-file maintenance, no way to forget.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class UnsupportedReasonContractTests
{
    private static readonly string RepoRoot =
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));

    private static string SourceRoot => Path.Combine(RepoRoot, "src", "nORM");

    // Every production .cs file, excluding build output. A throw-site anywhere here must carry a code.
    private static IEnumerable<string> ProductionSourceFiles()
        => Directory.EnumerateFiles(SourceRoot, "*.cs", SearchOption.AllDirectories)
            .Where(p =>
            {
                var norm = p.Replace('\\', '/');
                return !norm.Contains("/obj/") && !norm.Contains("/bin/");
            });

    private static ISet<string> CatalogCodeNames()
        => typeof(NormUnsupportedReason)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => f.Name)
            .ToHashSet(StringComparer.Ordinal);

    [Fact]
    public void Catalog_codes_are_nonempty_unique_kebab_case()
    {
        var values = typeof(NormUnsupportedReason)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();

        Assert.NotEmpty(values);
        Assert.All(values, v => Assert.False(string.IsNullOrWhiteSpace(v)));
        Assert.Equal(values.Length, values.Distinct(StringComparer.Ordinal).Count());
        Assert.All(values, v => Assert.Matches("^[a-z0-9]+(-[a-z0-9]+)*$", v));
    }

    [Fact]
    public void Internal_ctor_carries_reason_code_and_message_only_ctor_does_not()
    {
        var classified = new NormUnsupportedFeatureException("msg", NormUnsupportedReason.StringMethodUntranslatable);
        Assert.Equal(NormUnsupportedReason.StringMethodUntranslatable, classified.ReasonCode);
        Assert.Null(new NormUnsupportedFeatureException("msg").ReasonCode);
    }

    [Fact]
    public void Every_unsupported_throw_site_carries_a_registered_reason_code()
    {
        var catalog = CatalogCodeNames();
        var offenders = new List<string>();
        var scanned = 0;

        foreach (var path in ProductionSourceFiles())
        {
            var root = CSharpSyntaxTree.ParseText(File.ReadAllText(path)).GetRoot();
            var creations = root.DescendantNodes()
                .OfType<ObjectCreationExpressionSyntax>()
                .Where(o => TypeName(o.Type) == "NormUnsupportedFeatureException");

            foreach (var creation in creations)
            {
                scanned++;
                var args = creation.ArgumentList?.Arguments;
                var line = creation.GetLocation().GetLineSpan().StartLinePosition.Line + 1;
                var rel = Path.GetRelativePath(SourceRoot, path).Replace('\\', '/');

                // The reason code is the 2nd argument and must be a NormUnsupportedReason.<Code> reference —
                // written either unqualified (NormUnsupportedReason.X) or fully qualified
                // (nORM.Core.NormUnsupportedReason.X).
                var codeArg = args is { Count: >= 2 } ? args.Value[1].Expression as MemberAccessExpressionSyntax : null;
                if (codeArg is null || !RefersToReasonCatalog(codeArg.Expression))
                {
                    offenders.Add($"{rel}:{line} — throw new NormUnsupportedFeatureException without a NormUnsupportedReason code");
                    continue;
                }

                var codeName = codeArg.Name.Identifier.Text;
                if (!catalog.Contains(codeName))
                    offenders.Add($"{rel}:{line} — references unknown NormUnsupportedReason.{codeName}");
            }
        }

        // Guard against a broken glob silently passing by scanning nothing.
        Assert.True(scanned > 100, $"expected the whole-codebase scan to find many throw-sites, found {scanned}");

        Assert.True(offenders.Count == 0,
            "Every NormUnsupportedFeatureException in src/nORM must carry a registered reason code:\n  " +
            string.Join("\n  ", offenders));
    }

    private static string? TypeName(TypeSyntax type) => type switch
    {
        IdentifierNameSyntax id => id.Identifier.Text,
        QualifiedNameSyntax q => q.Right.Identifier.Text,
        _ => null,
    };

    // The left side of a NormUnsupportedReason.<Code> access: an identifier (unqualified use) or a
    // member access ending in NormUnsupportedReason (fully-qualified nORM.Core.NormUnsupportedReason).
    private static bool RefersToReasonCatalog(ExpressionSyntax expression) => expression switch
    {
        IdentifierNameSyntax id => id.Identifier.Text == "NormUnsupportedReason",
        MemberAccessExpressionSyntax member => member.Name.Identifier.Text == "NormUnsupportedReason",
        _ => false,
    };
}
