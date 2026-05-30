#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Options that control database scaffolding output.
    /// </summary>
    public sealed class ScaffoldOptions
    {
        /// <summary>
        /// Gets the optional table filter. Entries may be bare table names or
        /// schema-qualified names such as <c>dbo.Customer</c>. When empty, all
        /// discovered user tables are scaffolded. Null or blank filters are
        /// treated as empty.
        /// </summary>
        public IReadOnlyCollection<string> Tables { get; init; } = Array.Empty<string>();

        /// <summary>
        /// Gets a value indicating whether existing generated files may be
        /// overwritten. The default preserves the historical scaffolder behavior.
        /// </summary>
        public bool OverwriteFiles { get; init; } = true;

        /// <summary>
        /// Gets a value indicating whether scaffolding should fail when diagnostics
        /// are produced for schema features that cannot be emitted as runnable nORM
        /// model code.
        /// </summary>
        public bool FailOnWarnings { get; init; }

        /// <summary>
        /// Gets a value indicating whether provider-bound stored procedure/function
        /// wrapper methods should be generated on the scaffolded context. Generated
        /// wrappers call nORM's stored-procedure APIs and do not translate routine
        /// bodies across database providers.
        /// </summary>
        public bool EmitRoutineStubs { get; init; }

        /// <summary>
        /// Gets a value indicating whether provider-bound standalone sequence
        /// wrapper methods should be generated on the scaffolded context.
        /// Generated wrappers retrieve the next sequence value through provider
        /// SQL and do not translate sequence DDL across database providers.
        /// </summary>
        public bool EmitSequenceStubs { get; init; }

        /// <summary>
        /// Gets a value indicating whether views/materialized views and SQLite
        /// virtual tables should be emitted as query-only entity classes.
        /// Generated query-artifact entities are scaffolding
        /// bootstrap artifacts; nORM does not infer provider-neutral write semantics
        /// for them.
        /// </summary>
        public bool EmitViewEntities { get; init; }

        /// <summary>
        /// Gets a value indicating whether supported provider query artifacts
        /// (views/materialized views and SQLite virtual tables) should be emitted
        /// as query-only entity classes. This is the preferred API name;
        /// <see cref="EmitViewEntities"/> remains an equivalent compatibility alias.
        /// </summary>
        public bool EmitQueryArtifacts { get; init; }
    }
}
