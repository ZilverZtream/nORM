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
/// contract" from the fuzzing vision. Every throw-site in a CLASSIFIED source file must carry a stable
/// <see cref="NormUnsupportedReason"/> code. Without this, a throw-site that begins rejecting a shape which
/// used to translate stays GREEN (an "unsupported" rejection has no test budget), silently eroding
/// capability; a required, stable reason code makes that erosion visible to the fuzz support-contract.
/// The scan is scoped to an append-only allowlist that grows as each query-path family is reason-coded.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class UnsupportedReasonContractTests
{
    private static readonly string RepoRoot =
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));

    // Files whose EVERY NormUnsupportedFeatureException throw has been assigned a reason code.
    // APPEND-ONLY — removing a file would silently drop its capability-erosion coverage.
    private static readonly string[] ClassifiedFiles =
    {
        "Query/ExpressionToSqlVisitor.MethodCallTranslators.String.cs",
        "Query/ExpressionToSqlVisitor.MethodCallTranslators.cs",
        "Query/ExpressionToSqlVisitor.MethodCallTranslators.Enumerable.cs",
        "Query/ExpressionToSqlVisitor.NavigationSubqueries.cs",
        "Query/ExpressionToSqlVisitor.Binary.cs",
        "Query/ExpressionToSqlVisitor.ControlFlow.cs",
        "Query/ExpressionToSqlVisitor.Members.cs",
        "Query/SelectClauseVisitor.NavigationAggregates.cs",
        "Query/SelectClauseVisitor.NavigationDistinctCount.cs",
        "Query/SelectClauseVisitor.NavigationFirst.cs",
        "Query/SelectClauseVisitor.Helpers.cs",
        "Query/SelectClauseVisitor.MethodCalls.cs",
        "Query/QueryTranslator.SequenceTailTranslators.cs",
        "Query/QueryTranslator.IncludeTranslators.cs",
        "Query/QueryTranslator.PagingTranslators.cs",
        "Query/QueryTranslator.SetOperationTranslators.cs",
        "Query/QueryTranslator.SequenceEqualTranslator.cs",
        "Query/QueryTranslator.TerminalTranslators.cs",
        "Query/QueryTranslator.GroupByProjection.cs",
        "Query/QueryTranslator.FilterProjectionTranslators.cs",
        "Query/QueryTranslator.SplitQueries.cs",
        "Query/QueryTranslator.GroupJoins.cs",
        "Query/QueryTranslator.TemporalScope.cs",
        "Query/QueryTranslator.PlanGeneration.cs",
        "Query/QueryTranslator.OrderByTranslator.cs",
        "Query/QueryTranslator.Joins.cs",
        "Query/QueryTranslator.GroupByClient.cs",
        "Query/QueryTranslator.AggregateDelegates.cs",
        "Providers/DatabaseProvider.SqlExpressions.cs",
        "Query/BulkCudBuilder.cs",
    };

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
    public void Classified_throw_sites_all_carry_a_registered_reason_code()
    {
        var catalog = CatalogCodeNames();
        var offenders = new List<string>();

        foreach (var rel in ClassifiedFiles)
        {
            var path = Path.Combine(RepoRoot, "src", "nORM", rel.Replace('/', Path.DirectorySeparatorChar));
            Assert.True(File.Exists(path), $"classified file not found: {path}");

            var root = CSharpSyntaxTree.ParseText(File.ReadAllText(path)).GetRoot();
            var creations = root.DescendantNodes()
                .OfType<ObjectCreationExpressionSyntax>()
                .Where(o => TypeName(o.Type) == "NormUnsupportedFeatureException");

            foreach (var creation in creations)
            {
                var args = creation.ArgumentList?.Arguments;
                var line = creation.GetLocation().GetLineSpan().StartLinePosition.Line + 1;

                // The reason code is the 2nd argument and must be a NormUnsupportedReason.<Code> reference.
                var codeArg = args is { Count: >= 2 } ? args.Value[1].Expression as MemberAccessExpressionSyntax : null;
                if (codeArg is null
                    || (codeArg.Expression as IdentifierNameSyntax)?.Identifier.Text != "NormUnsupportedReason")
                {
                    offenders.Add($"{rel}:{line} — throw new NormUnsupportedFeatureException without a NormUnsupportedReason code");
                    continue;
                }

                var codeName = codeArg.Name.Identifier.Text;
                if (!catalog.Contains(codeName))
                    offenders.Add($"{rel}:{line} — references unknown NormUnsupportedReason.{codeName}");
            }
        }

        Assert.True(offenders.Count == 0,
            "Every NormUnsupportedFeatureException in a classified file must carry a registered reason code:\n  " +
            string.Join("\n  ", offenders));
    }

    private static string? TypeName(TypeSyntax type) => type switch
    {
        IdentifierNameSyntax id => id.Identifier.Text,
        QualifiedNameSyntax q => q.Right.Identifier.Text,
        _ => null,
    };
}
