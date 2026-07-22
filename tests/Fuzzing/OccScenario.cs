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
    /// A typed, serializable optimistic-concurrency scenario: several independent writers (each its own
    /// <see cref="nORM.Core.DbContext"/> over one shared database) interleave Load / Update / Save operations on a
    /// small set of seeded rows carrying a <c>[Timestamp]</c> token. It is the durable reproduction contract for
    /// the OCC slice of the write-path snapshot-diff oracle: a writer whose loaded token a concurrent writer has
    /// already advanced must have its save rejected (<see cref="nORM.Core.DbConcurrencyException"/>), never silently
    /// clobber the concurrent change. This is round-2's most bug-dense write-path area.
    /// </summary>
    public sealed record OccScenario
    {
        /// <summary>Number of independent writers (each a separate context over the shared connection).</summary>
        public required int WriterCount { get; init; }

        /// <summary>Rows seeded (id → initial value) before any op runs. OCC conflicts are exercised on updates.</summary>
        public required IReadOnlyList<int> SeededIds { get; init; }

        public required IReadOnlyList<OccOp> Ops { get; init; }

        public string ToJson() => JsonSerializer.Serialize(this, FuzzRunManifest.ManifestJsonOptions);
        public static OccScenario FromJson(string json) =>
            JsonSerializer.Deserialize<OccScenario>(json, FuzzRunManifest.ManifestJsonOptions)
            ?? throw new InvalidOperationException("Empty OccScenario JSON.");

        public string Describe() =>
            $"W{WriterCount}[{string.Join(",", SeededIds)}]:" + string.Join(".", Ops.Select(o => o.Describe()));
    }

    public enum OccOpKind { Load, Update, Save }

    /// <summary>One OCC operation performed by writer <see cref="Writer"/>. <see cref="Value"/> is used by Update.</summary>
    public sealed record OccOp
    {
        public required OccOpKind Kind { get; init; }
        public required int Writer { get; init; }
        public int Id { get; init; }
        public int Value { get; init; }

        public static OccOp Load(int writer, int id) => new() { Kind = OccOpKind.Load, Writer = writer, Id = id };
        public static OccOp Update(int writer, int id, int value) => new() { Kind = OccOpKind.Update, Writer = writer, Id = id, Value = value };
        public static OccOp Save(int writer) => new() { Kind = OccOpKind.Save, Writer = writer };

        public string Describe() => Kind switch
        {
            OccOpKind.Load => $"W{Writer}.Load({Id})",
            OccOpKind.Update => $"W{Writer}.Upd({Id}={Value})",
            OccOpKind.Save => $"W{Writer}.Save",
            _ => Kind.ToString(),
        };
    }

    /// <summary>The keyed entity the OCC scenario operates over: client-assigned key, one payload, a rowversion token.</summary>
    [Table("OccItem")]
    public sealed class OccItem
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
        [Timestamp] public byte[] Version { get; set; } = Array.Empty<byte>();
    }
}
