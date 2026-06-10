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
        /// schema-qualified names such as <c>dbo.Customer</c>. Matching
        /// supported query artifacts such as views are emitted as read-only
        /// entities. When empty, all discovered user tables and ordinary
        /// views/materialized views are scaffolded; provider-specific virtual
        /// tables and synonyms remain opt-in query artifacts. Null or blank
        /// filters are treated as empty.
        /// </summary>
        public IReadOnlyCollection<string> Tables { get; init; } = Array.Empty<string>();

        /// <summary>
        /// Gets the optional schema filter. When empty, all discovered schemas are
        /// included unless <see cref="Tables"/> narrows the scaffold. When set,
        /// all discovered user tables and supported query artifacts in the
        /// listed schemas are scaffolded. Schema filters are unioned with
        /// explicit table filters. Null or blank schema entries are treated as
        /// empty.
        /// </summary>
        public IReadOnlyCollection<string> Schemas { get; init; } = Array.Empty<string>();

        /// <summary>
        /// Gets a value indicating whether generated <c>IQueryable&lt;T&gt;</c>
        /// context properties should use plural collection-style names. Entity
        /// class names remain based on the database object names regardless of
        /// this setting.
        /// </summary>
        public bool PluralizeQueryProperties { get; init; } = true;

        /// <summary>
        /// Gets a value indicating whether generated CLR entity, property,
        /// routine, and sequence names should preserve legal database object
        /// names instead of applying nORM's PascalCase naming convention.
        /// Invalid C# identifiers are still escaped or normalized so generated
        /// code always compiles.
        /// </summary>
        public bool UseDatabaseNames { get; init; }

        /// <summary>
        /// Gets a value indicating whether generated files should emit nullable
        /// reference type annotations and <c>#nullable enable</c>. The runtime
        /// API defaults to enabled for nullable-safe output; the CLI sets this
        /// from the target project's <c>Nullable</c> property when a project is
        /// supplied or inferred.
        /// </summary>
        public bool UseNullableReferenceTypes { get; init; } = true;

        /// <summary>
        /// Gets the optional namespace for the generated DbContext. When null
        /// or blank, the generated DbContext uses the entity namespace supplied
        /// to the scaffolder.
        /// </summary>
        public string? ContextNamespace { get; init; }

        /// <summary>
        /// Gets the optional absolute directory where the generated DbContext
        /// file should be written. When set, this cannot be combined with
        /// <see cref="ContextDirectory"/>. Entity files and warning reports
        /// remain in the scaffold output directory.
        /// </summary>
        public string? ContextOutputDirectory { get; init; }

        /// <summary>
        /// Gets the optional relative directory, below the scaffold output
        /// directory, where the generated DbContext file should be written.
        /// Entity files and warning reports remain in the root output
        /// directory. Absolute paths and parent-directory traversal are
        /// rejected.
        /// </summary>
        public string? ContextDirectory { get; init; }

        /// <summary>
        /// Gets a value indicating whether existing generated files may be
        /// overwritten. The default preserves the historical scaffolder behavior.
        /// </summary>
        public bool OverwriteFiles { get; init; } = true;

        /// <summary>
        /// Gets a value indicating whether scaffolding should validate and
        /// generate output in memory without creating, deleting, or overwriting
        /// files. This is intended for CI/preflight checks before a real
        /// scaffold run.
        /// </summary>
        public bool DryRun { get; init; }

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
        /// Gets a value indicating whether optional query artifacts such as
        /// SQLite virtual tables and SQL Server local table/view synonyms should
        /// be emitted as query-only entity classes. Ordinary views and
        /// materialized views are emitted by default. Generated query-artifact entities are scaffolding
        /// bootstrap artifacts; nORM does not infer provider-neutral write semantics
        /// for them.
        /// </summary>
        public bool EmitViewEntities { get; init; }

        /// <summary>
        /// Gets a value indicating whether optional provider query artifacts
        /// such as SQLite virtual tables and SQL Server local table/view synonyms
        /// should be emitted as query-only entity classes. Ordinary views and
        /// materialized views are emitted by default. This is the preferred API name;
        /// <see cref="EmitViewEntities"/> remains an equivalent compatibility alias.
        /// </summary>
        public bool EmitQueryArtifacts { get; init; }
    }
}
