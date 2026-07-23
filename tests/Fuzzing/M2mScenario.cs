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
    /// A typed, serializable many-to-many write scenario: a sequence of LoadPost / Link / Unlink / Save
    /// operations over a small set of Posts and Tags related through a join table. It is the durable
    /// reproduction contract for the M2M slice of the write-path snapshot-diff oracle — the same scenario is
    /// applied to nORM (editing the tracked <c>Post.Tags</c> collection loaded via Include) and to an in-memory
    /// reference set of links, and the authoritative join-table state (raw SQL) must equal that set. This is the
    /// area where the m2m_write_sync silent-no-op bug lived, so a dropped add/remove is exactly what it catches.
    /// </summary>
    public sealed record M2mScenario
    {
        public required IReadOnlyList<int> PostIds { get; init; }
        public required IReadOnlyList<int> TagIds { get; init; }
        /// <summary>Join rows seeded before any op runs (the loaded snapshot the tracked collection diffs against).</summary>
        public required IReadOnlyList<M2mLink> InitialLinks { get; init; }
        public required IReadOnlyList<M2mOp> Ops { get; init; }

        public string ToJson() => JsonSerializer.Serialize(this, FuzzRunManifest.ManifestJsonOptions);
        public static M2mScenario FromJson(string json) =>
            JsonSerializer.Deserialize<M2mScenario>(json, FuzzRunManifest.ManifestJsonOptions)
            ?? throw new InvalidOperationException("Empty M2mScenario JSON.");

        public string Describe() =>
            $"P[{string.Join(",", PostIds)}]T[{string.Join(",", TagIds)}]" +
            $"L[{string.Join(",", InitialLinks.Select(l => $"{l.PostId}-{l.TagId}"))}]:" +
            string.Join(".", Ops.Select(o => o.Describe()));
    }

    public sealed record M2mLink
    {
        public required int PostId { get; init; }
        public required int TagId { get; init; }
    }

    public enum M2mOpKind { LoadPost, Link, Unlink, Save }

    /// <summary>One m2m op. <see cref="TagId"/> is unused for LoadPost/Save.</summary>
    public sealed record M2mOp
    {
        public required M2mOpKind Kind { get; init; }
        public int PostId { get; init; }
        public int TagId { get; init; }

        public static M2mOp LoadPost(int postId) => new() { Kind = M2mOpKind.LoadPost, PostId = postId };
        public static M2mOp Link(int postId, int tagId) => new() { Kind = M2mOpKind.Link, PostId = postId, TagId = tagId };
        public static M2mOp Unlink(int postId, int tagId) => new() { Kind = M2mOpKind.Unlink, PostId = postId, TagId = tagId };
        public static M2mOp Save() => new() { Kind = M2mOpKind.Save };

        public string Describe() => Kind switch
        {
            M2mOpKind.LoadPost => $"Load(P{PostId})",
            M2mOpKind.Link => $"Link(P{PostId}-T{TagId})",
            M2mOpKind.Unlink => $"Unlink(P{PostId}-T{TagId})",
            M2mOpKind.Save => "Save",
            _ => Kind.ToString(),
        };
    }

    [Table("M2mfPost")]
    public sealed class M2mfPost
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<M2mfTag> Tags { get; set; } = new();
    }

    [Table("M2mfTag")]
    public sealed class M2mfTag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }
}
