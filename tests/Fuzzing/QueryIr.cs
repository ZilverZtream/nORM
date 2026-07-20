using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text.Json;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// A typed, serializable query IR — the durable reproduction contract that a seed only points at. A case
    /// carries its own data (<see cref="Rows"/>) and a flat list of query <see cref="Steps"/>, so the SAME
    /// object compiles independently to the nORM query and to the LINQ-to-Objects oracle, serializes into a
    /// corpus, and is structurally shrinkable (drop a step, drop a row, shrink a constant) without depending on
    /// the generator that produced it. This is the query-family slice of the fuzzing architecture's item A.
    /// </summary>
    public sealed record QueryIr
    {
        public required IReadOnlyList<IrRow> Rows { get; init; }
        public required IReadOnlyList<IrStep> Steps { get; init; }

        public string ToJson() => JsonSerializer.Serialize(this, FuzzRunManifest.ManifestJsonOptions);
        public static QueryIr FromJson(string json) =>
            JsonSerializer.Deserialize<QueryIr>(json, FuzzRunManifest.ManifestJsonOptions)
            ?? throw new InvalidOperationException("Empty QueryIr JSON.");

        /// <summary>A compact one-line description for labels and failure signatures.</summary>
        public string Describe() =>
            $"rows={Rows.Count} | " + string.Join(".", Steps.Select(s => s.Describe()));
    }

    /// <summary>The fixed entity schema the query IR operates over. Small, with an int key, two numeric columns, and a string.</summary>
    [Table("IrRow")]
    public sealed class IrRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public string Name { get; set; } = "";
    }

    public enum IrStepKind { Where, OrderBy, Skip, Take, Distinct }
    public enum IrCompare { Eq, Ne, Lt, Le, Gt, Ge }
    public enum IrColumn { Id, A, B }   // orderable / comparable numeric columns

    /// <summary>
    /// One query step in a flat, JSON-friendly shape (a tagged record rather than a class hierarchy, so the
    /// corpus is trivially serializable and each field is independently shrinkable).
    /// </summary>
    public sealed record IrStep
    {
        public required IrStepKind Kind { get; init; }
        public IrColumn Column { get; init; }
        public IrCompare Op { get; init; }
        public int Value { get; init; }
        public bool Descending { get; init; }
        public int Count { get; init; }

        public static IrStep Where(IrColumn col, IrCompare op, int value) => new() { Kind = IrStepKind.Where, Column = col, Op = op, Value = value };
        public static IrStep OrderBy(IrColumn col, bool descending = false) => new() { Kind = IrStepKind.OrderBy, Column = col, Descending = descending };
        public static IrStep Skip(int count) => new() { Kind = IrStepKind.Skip, Count = count };
        public static IrStep Take(int count) => new() { Kind = IrStepKind.Take, Count = count };
        public static IrStep Distinct() => new() { Kind = IrStepKind.Distinct };

        public string Describe() => Kind switch
        {
            IrStepKind.Where => $"Where({Column}{OpSym(Op)}{Value})",
            IrStepKind.OrderBy => $"OrderBy({Column}{(Descending ? "-desc" : "")})",
            IrStepKind.Skip => $"Skip({Count})",
            IrStepKind.Take => $"Take({Count})",
            IrStepKind.Distinct => "Distinct",
            _ => Kind.ToString(),
        };

        private static string OpSym(IrCompare op) => op switch
        {
            IrCompare.Eq => "==", IrCompare.Ne => "!=", IrCompare.Lt => "<",
            IrCompare.Le => "<=", IrCompare.Gt => ">", IrCompare.Ge => ">=", _ => "?",
        };
    }
}
