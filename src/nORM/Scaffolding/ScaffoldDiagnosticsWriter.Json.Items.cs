#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static readonly IReadOnlyDictionary<string, object?> EmptyMetadata =
            new Dictionary<string, object?>(0, StringComparer.Ordinal);

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class CompositeForeignKeyJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string constraint { get; init; } = string.Empty;
            public string dependentTable { get; init; } = string.Empty;
            public IReadOnlyList<string> dependentColumns { get; init; } = Array.Empty<string>();
            public string principalTable { get; init; } = string.Empty;
            public IReadOnlyList<string> principalColumns { get; init; } = Array.Empty<string>();
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class PossibleJoinTableJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string table { get; init; } = string.Empty;
            public IReadOnlyList<string> principalTables { get; init; } = Array.Empty<string>();
            public IReadOnlyList<string> constraints { get; init; } = Array.Empty<string>();
            public IReadOnlyList<string> reasons { get; init; } = Array.Empty<string>();
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ProviderOwnedSchemaFeatureJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string kind { get; init; } = string.Empty;
            public string table { get; init; } = string.Empty;
            public string name { get; init; } = string.Empty;
            public string detail { get; init; } = string.Empty;
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class SkippedDatabaseObjectJsonItem
        {
            public string code { get; init; } = string.Empty;
            public string severity { get; init; } = string.Empty;
            public string category { get; init; } = string.Empty;
            public string kind { get; init; } = string.Empty;
            public string name { get; init; } = string.Empty;
            public string detail { get; init; } = string.Empty;
            public IReadOnlyDictionary<string, object?> metadata { get; init; } = EmptyMetadata;
            public string suggestedAction { get; init; } = string.Empty;
        }
    }
}
