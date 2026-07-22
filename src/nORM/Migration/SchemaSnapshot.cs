using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using RenameColumnAttr = nORM.Mapping.RenameColumnAttribute;

namespace nORM.Migration
{
    /// <summary>
    /// Represents the desired database schema state derived from code mappings (attributes or
    /// fluent configuration). This is NOT a live snapshot of an actual database - it reflects
    /// what the schema <em>should</em> look like based on the current entity model.
    /// Use <see cref="SchemaDiffer.Diff"/> to compare two snapshots and produce the migrations
    /// required to bring an old schema up to date with a new one.
    /// </summary>
    public class SchemaSnapshot
    {
        /// <summary>Tables captured in the snapshot.</summary>
        public List<TableSchema> Tables { get; init; } = new();
    }

    /// <summary>
    /// Describes the schema of a single table including its columns and foreign keys.
    /// </summary>
    public class TableSchema
    {
        /// <summary>Name of the table.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>
        /// True when the table is versioned by nORM-managed (trigger-emulated) temporal storage.
        /// Populated by <see cref="SchemaSnapshotBuilder.Build(DbContext)"/> when the context has
        /// temporal versioning enabled in an emulated storage mode. Migration generators mirror
        /// schema changes onto the table's <c>&lt;Name&gt;_History</c> companion in lock-step and
        /// re-emit the versioning triggers from the new schema. Provider-native temporal storage
        /// (e.g. SQL Server system-versioning) propagates schema changes itself and stays unflagged.
        /// </summary>
        public bool IsTemporal { get; set; }
        /// <summary>
        /// Name of the tenant-discriminator column when the entity is tenant-scoped; null otherwise.
        /// Populated by <see cref="SchemaSnapshotBuilder.Build(DbContext)"/>. Temporal migration DDL
        /// needs it because the regenerated versioning triggers scope their history-close statements
        /// to the same tenant.
        /// </summary>
        public string? TenantColumnName { get; set; }
        /// <summary>Columns defined on the table.</summary>
        public List<ColumnSchema> Columns { get; init; } = new();
        /// <summary>Foreign key constraints defined on this table.</summary>
        public List<ForeignKeySchema> ForeignKeys { get; init; } = new();
        /// <summary>CHECK constraints defined on this table.</summary>
        public List<CheckConstraintSchema> CheckConstraints { get; init; } = new();
        /// <summary>Provider-specific expression indexes defined on this table.</summary>
        public List<ExpressionIndexSchema> ExpressionIndexes { get; init; } = new();
    }

    /// <summary>
    /// Describes a table-level CHECK constraint.
    /// </summary>
    public class CheckConstraintSchema
    {
        /// <summary>Name of the CHECK constraint.</summary>
        public string ConstraintName { get; set; } = string.Empty;
        /// <summary>Provider SQL predicate inside the CHECK clause.</summary>
        public string Sql { get; set; } = string.Empty;
    }

    /// <summary>
    /// Describes an index whose key is a SQL expression rather than mapped columns.
    /// </summary>
    public class ExpressionIndexSchema
    {
        /// <summary>Database index name.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>Provider SQL expression used as the index key.</summary>
        public string ExpressionSql { get; set; } = string.Empty;
        /// <summary>True when the expression index enforces uniqueness.</summary>
        public bool IsUnique { get; set; }
        /// <summary>Optional provider SQL predicate for a filtered/partial expression index.</summary>
        public string? FilterSql { get; set; }
        /// <summary>Provider column names included as non-key covering columns where supported.</summary>
        public string[] IncludedColumnNames { get; set; } = Array.Empty<string>();
        /// <summary>Explicit provider null ordering for the expression key.</summary>
        public IndexNullSortOrder NullSortOrder { get; set; }
        /// <summary>True when a unique PostgreSQL expression index treats null values as equal.</summary>
        public bool NullsNotDistinct { get; set; }
    }

    /// <summary>
    /// Describes a foreign key constraint between a dependent (child) table and a principal (parent) table.
    /// </summary>
    public class ForeignKeySchema
    {
        /// <summary>Name of the constraint (e.g. "FK_Post_Blog_BlogId").</summary>
        public string ConstraintName { get; set; } = string.Empty;
        /// <summary>Columns on the dependent (child) table that hold the FK values. Order is significant.</summary>
        public string[] DependentColumns { get; set; } = Array.Empty<string>();
        /// <summary>The principal (parent) table being referenced.</summary>
        public string PrincipalTable { get; set; } = string.Empty;
        /// <summary>Columns on the principal table being referenced (usually the PK). Order matches DependentColumns.</summary>
        public string[] PrincipalColumns { get; set; } = Array.Empty<string>();
        /// <summary>
        /// Referential action on DELETE (NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT).
        /// "NO ACTION" is the default and causes no ON DELETE clause to be emitted.
        /// </summary>
        public string OnDelete { get; set; } = "NO ACTION";
        /// <summary>
        /// Referential action on UPDATE (NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT).
        /// "NO ACTION" is the default and causes no ON UPDATE clause to be emitted.
        /// </summary>
        public string OnUpdate { get; set; } = "NO ACTION";
    }

    /// <summary>
    /// Describes a column within a table schema snapshot.
    /// </summary>
    public class ColumnSchema
    {
        /// <summary>Name of the column.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>
        /// When non-null, indicates that this column was previously named <see cref="PreviousName"/>
        /// and the schema differ should emit a RENAME COLUMN operation instead of DROP + ADD.
        /// Populated by <see cref="SchemaSnapshotBuilder"/> when a property carries
        /// <c>[RenameColumn("oldName")]</c>.
        /// </summary>
        public string? PreviousName { get; set; }
        /// <summary>
        /// Full CLR type name of the column (e.g. <c>System.Int32</c>).
        /// An empty string is a recognizable placeholder meaning "unknown/unresolved type";
        /// callers that produce <see cref="ColumnSchema"/> instances should always populate this
        /// field - leaving it empty will cause all four SQL generators to fall back to their
        /// default type (e.g. NVARCHAR(MAX) for SQL Server) and may suppress spurious alter
        /// detections because two columns with an empty ClrType compare equal.
        /// </summary>
        // NOTE: the default is intentionally string.Empty (not null) so that null-safe string
        // comparisons in SchemaDiffer.Diff do not require extra null guards. If ClrType is empty
        // on a ColumnSchema produced by external code, treat it as a configuration concern.
        public string ClrType { get; set; } = string.Empty;
        /// <summary>
        /// Optional explicit provider store type (EF Core's <c>HasColumnType</c>, e.g. <c>decimal(18,2)</c>,
        /// <c>nvarchar(max)</c>, <c>jsonb</c>). When set, the SQL generators emit it verbatim instead of
        /// deriving the type from <see cref="ClrType"/> and the length/precision facets.
        /// </summary>
        public string? StoreType { get; set; }
        /// <summary>Optional maximum length for bounded text or binary columns.</summary>
        public int? MaxLength { get; set; }
        /// <summary>
        /// Optional Unicode text storage hint. <c>false</c> requests non-Unicode text where the provider distinguishes it;
        /// <c>true</c> requests Unicode text; <c>null</c> uses the provider default.
        /// </summary>
        public bool? IsUnicode { get; set; }
        /// <summary>True when bounded text or binary storage should use a fixed-length provider type.</summary>
        public bool IsFixedLength { get; set; }
        /// <summary>Optional decimal/numeric precision for providers that support fixed-precision decimals.</summary>
        public int? Precision { get; set; }
        /// <summary>Optional decimal/numeric scale for providers that support fixed-precision decimals.</summary>
        public int? Scale { get; set; }
        /// <summary>Indicates whether the column allows <c>null</c> values.</summary>
        public bool IsNullable { get; set; }
        /// <summary>True when the column is (part of) the table's primary key.</summary>
        public bool IsPrimaryKey { get; set; }
        /// <summary>True when the column has a UNIQUE index.</summary>
        public bool IsUnique { get; set; }
        /// <summary>Non-null means the column is covered by a named index.</summary>
        public string? IndexName { get; set; }
        /// <summary>Zero-based order of this column within a named composite index.</summary>
        public int? IndexOrder { get; set; }
        /// <summary>Named indexes this column participates in.</summary>
        public List<ColumnIndexSchema> Indexes { get; } = new();
        /// <summary>SQL literal default value for ADD COLUMN NOT NULL migrations (e.g. "''" or "0").</summary>
        public string? DefaultValue { get; set; }
        /// <summary>Optional provider default-constraint name associated with <see cref="DefaultValue"/>.</summary>
        public string? DefaultConstraintName { get; set; }
        /// <summary>Provider collation identifier applied to text comparison and ordering for this column.</summary>
        public string? Collation { get; set; }
        /// <summary>True when the column has identity/autoincrement semantics (e.g. [DatabaseGenerated(Identity)]).</summary>
        public bool IsIdentity { get; set; }
        /// <summary>
        /// When <see cref="IsIdentity"/>, true if the identity column HONORS an explicitly-supplied value
        /// (store-generated only when the value is default) — the store-generated-key convention (EF parity).
        /// Drives PostgreSQL <c>GENERATED BY DEFAULT AS IDENTITY</c> vs the strict <c>GENERATED ALWAYS</c>, and
        /// SQL Server's IDENTITY_INSERT wrap for explicit rows. False for a strict [DatabaseGenerated(Identity)].
        /// </summary>
        public bool IdentityByDefault { get; set; }
        /// <summary>Optional provider identity seed/start value.</summary>
        public long? IdentitySeed { get; set; }
        /// <summary>Optional provider identity increment value.</summary>
        public long? IdentityIncrement { get; set; }
        /// <summary>Provider SQL expression for a computed/generated column.</summary>
        public string? ComputedColumnSql { get; set; }
        /// <summary>True when a computed/generated column should be physically stored where supported.</summary>
        public bool IsStoredComputedColumn { get; set; }

        /// <summary>
        /// Optional human-readable column comment (EF Core's <c>HasComment</c>). Emitted by each provider's
        /// migration generator using its native mechanism: an inline block comment (SQLite), an inline
        /// <c>COMMENT</c> clause (MySQL), a <c>COMMENT ON COLUMN</c> statement (PostgreSQL), or
        /// <c>sp_addextendedproperty</c> (SQL Server). Null means no comment.
        /// </summary>
        public string? Comment { get; set; }

        /// <summary>
        /// Shallow copy of this column. Used when a table recreation needs the same column under a
        /// different <see cref="Name"/> (e.g. reverting a renamed column on the Down path).
        /// </summary>
        internal ColumnSchema Clone() => (ColumnSchema)MemberwiseClone();
    }

    /// <summary>
    /// Describes one index membership for a column.
    /// </summary>
    public class ColumnIndexSchema
    {
        /// <summary>Database index name.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>True when the named index enforces uniqueness.</summary>
        public bool IsUnique { get; set; }
        /// <summary>Zero-based order of this column within the named index.</summary>
        public int? Order { get; set; }
        /// <summary>True when this index key column is ordered descending.</summary>
        public bool IsDescending { get; set; }
        /// <summary>True when this column is an included, non-key column.</summary>
        public bool IsIncluded { get; set; }
        /// <summary>True when a unique PostgreSQL index treats null values as equal.</summary>
        public bool NullsNotDistinct { get; set; }
        /// <summary>Explicit provider null ordering for this index key column.</summary>
        public IndexNullSortOrder NullSortOrder { get; set; }
        /// <summary>Provider-specific filter predicate for a filtered/partial index.</summary>
        public string? FilterSql { get; set; }
    }

}
