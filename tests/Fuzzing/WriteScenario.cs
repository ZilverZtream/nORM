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
    /// A typed, serializable write scenario: a sequence of Insert/Update/Delete/Save operations over a single
    /// keyed entity. It is the durable reproduction contract for the write-path snapshot-diff oracle — the same
    /// scenario is applied to nORM and to an in-memory reference model, and the authoritative persisted state
    /// (read back via raw SQL) must equal the model. This is the CRUD-family slice of the fuzzing architecture.
    /// </summary>
    public sealed record WriteScenario
    {
        public required IReadOnlyList<WriteOp> Ops { get; init; }

        public string ToJson() => JsonSerializer.Serialize(this, FuzzRunManifest.ManifestJsonOptions);
        public static WriteScenario FromJson(string json) =>
            JsonSerializer.Deserialize<WriteScenario>(json, FuzzRunManifest.ManifestJsonOptions)
            ?? throw new InvalidOperationException("Empty WriteScenario JSON.");

        public string Describe() => string.Join(".", Ops.Select(o => o.Describe()));
    }

    public enum WriteOpKind { Insert, Update, Delete, Save }

    /// <summary>One write operation. <see cref="Value"/> is unused for Delete/Save.</summary>
    public sealed record WriteOp
    {
        public required WriteOpKind Kind { get; init; }
        public int Id { get; init; }
        public int Value { get; init; }

        public static WriteOp Insert(int id, int value) => new() { Kind = WriteOpKind.Insert, Id = id, Value = value };
        public static WriteOp Update(int id, int value) => new() { Kind = WriteOpKind.Update, Id = id, Value = value };
        public static WriteOp Delete(int id) => new() { Kind = WriteOpKind.Delete, Id = id };
        public static WriteOp Save() => new() { Kind = WriteOpKind.Save };

        public string Describe() => Kind switch
        {
            WriteOpKind.Insert => $"Ins({Id}={Value})",
            WriteOpKind.Update => $"Upd({Id}={Value})",
            WriteOpKind.Delete => $"Del({Id})",
            WriteOpKind.Save => "Save",
            _ => Kind.ToString(),
        };
    }

    /// <summary>The single keyed entity the write scenario operates over. Client-assigned key, one payload column.</summary>
    [Table("CrudItem")]
    public sealed class CrudItem
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
