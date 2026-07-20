using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Mapping;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Defines mapping metadata for a single entity type including table, key,
    /// property and relationship configuration.
    /// </summary>
    public interface IEntityTypeConfiguration
    {
        /// <summary>
        /// Gets the unqualified table name that the entity maps to.
        /// </summary>
        string? TableName { get; }

        /// <summary>
        /// Gets the optional database schema that contains <see cref="TableName"/>.
        /// </summary>
        string? SchemaName { get; }

        /// <summary>
        /// Gets the collection of properties that compose the primary key.
        /// </summary>
        IReadOnlyList<PropertyInfo> KeyProperties { get; }

        /// <summary>
        /// Gets the optional database primary-key constraint name.
        /// </summary>
        string? PrimaryKeyConstraintName { get; }

        /// <summary>
        /// Gets a value indicating whether the mapped type is query-only and
        /// should reject generated write operations.
        /// </summary>
        bool IsReadOnly { get; }

        /// <summary>
        /// Gets a value indicating whether the mapped type is keyless (a query type):
        /// never tracked and not savable, used for views and read models.
        /// </summary>
        bool IsKeyless { get; }

        /// <summary>
        /// Gets a mapping of property infos to explicit column names.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, string> ColumnNames { get; }

        /// <summary>
        /// Gets SQL default literals/functions configured for properties. These values are
        /// consumed by migration snapshots and do not change runtime insert/update column
        /// participation.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, string> DefaultValueSql { get; }

        /// <summary>
        /// Gets optional provider default-constraint names configured for properties.
        /// These names are provider DDL metadata; providers without named defaults may ignore them.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, string> DefaultValueConstraintNames { get; }

        /// <summary>
        /// Gets provider identity seed/increment metadata configured for database-generated properties.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, IdentityOptionsConfiguration> IdentityOptions { get; }

        /// <summary>
        /// Gets maximum length metadata configured for string or byte array properties.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, int> MaxLengths { get; }

        /// <summary>
        /// Gets the explicit required (non-nullable) settings configured via the fluent API, overriding the
        /// CLR/attribute-derived nullability when the schema is generated.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, bool> RequiredSettings { get; }

        /// <summary>
        /// Gets the store value-generation strategy configured via the fluent API (ValueGeneratedOnAdd /
        /// Never / OnAddOrUpdate), overriding the attribute-derived database-generated flag.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption> ValueGeneratedSettings { get; }

        /// <summary>
        /// Gets the properties marked as the entity's row-version / optimistic-concurrency token via the
        /// fluent API (IsRowVersion), equivalent to the <c>[Timestamp]</c> attribute.
        /// </summary>
        IReadOnlyCollection<PropertyInfo> RowVersionSettings { get; }

        /// <summary>
        /// Gets the explicit provider store types configured via the fluent API (HasColumnType), emitted
        /// verbatim by the migration SQL generators instead of the CLR-derived type.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, string> ColumnTypes { get; }

        /// <summary>
        /// Gets Unicode text metadata configured for string properties.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, bool> UnicodeSettings { get; }

        /// <summary>
        /// Gets fixed-length storage metadata configured for string or byte array properties.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, bool> FixedLengthSettings { get; }

        /// <summary>
        /// Gets decimal precision/scale metadata configured for properties.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, PrecisionConfiguration> Precisions { get; }

        /// <summary>
        /// Gets database collation names configured for string/comparable text properties.
        /// Collation names are provider identifiers and are emitted by migration generators.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, string> Collations { get; }

        /// <summary>
        /// Gets table-level CHECK constraints configured for migration snapshots.
        /// </summary>
        IReadOnlyList<CheckConstraintConfiguration> CheckConstraints { get; }

        /// <summary>
        /// Gets SQL expressions configured for database-computed/generated columns.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, ComputedColumnConfiguration> ComputedColumnSql { get; }

        /// <summary>
        /// Gets provider-specific expression indexes configured for migration snapshots.
        /// </summary>
        IReadOnlyList<ExpressionIndexConfiguration> ExpressionIndexes { get; }

        /// <summary>
        /// Gets the CLR type that this entity shares its table with, if any.
        /// </summary>
        Type? TableSplitWith { get; }

        /// <summary>
        /// Gets owned navigation properties configured for this entity.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, OwnedNavigation> OwnedNavigations { get; }

        /// <summary>
        /// Gets the shadow properties defined for the entity type.
        /// </summary>
        IReadOnlyDictionary<string, ShadowPropertyConfiguration> ShadowProperties { get; }

        /// <summary>
        /// Gets the relationship configurations defined for the entity type.
        /// </summary>
        IReadOnlyList<RelationshipConfiguration> Relationships { get; }

        /// <summary>Gets value converter configurations for this entity type.</summary>
        IReadOnlyList<ConverterConfiguration> Converters { get; }

        /// <summary>
        /// Gets owned collection navigation properties configured for this entity.
        /// Key is the collection navigation property; value describes the owned element type and configuration.
        /// </summary>
        IReadOnlyDictionary<PropertyInfo, OwnedCollectionNavigation> OwnedCollectionNavigations { get; }

        /// <summary>
        /// Gets the many-to-many relationship configurations defined for this entity type.
        /// </summary>
        IReadOnlyList<ManyToManyConfiguration> ManyToManyRelationships { get; }
    }

    /// <summary>
    /// Describes an owned navigation property and its optional configuration.
    /// </summary>
    /// <param name="OwnedType">CLR type of the owned entity.</param>
    /// <param name="Configuration">Optional configuration applied to the owned entity.</param>
    public record OwnedNavigation(Type OwnedType, IEntityTypeConfiguration? Configuration);

    /// <summary>
    /// Represents a shadow property that does not exist on the CLR type but is mapped to a database column.
    /// </summary>
    /// <param name="ClrType">The CLR type of the shadow property.</param>
    /// <param name="ColumnName">Optional column name override.</param>
    public record ShadowPropertyConfiguration(Type ClrType, string? ColumnName = null);

    /// <summary>
    /// Describes a table-level CHECK constraint configured through the fluent model.
    /// </summary>
    /// <param name="Name">Database constraint name.</param>
    /// <param name="Sql">Provider SQL predicate inside the CHECK constraint.</param>
    public record CheckConstraintConfiguration(string Name, string Sql);

    /// <summary>
    /// Describes a database-computed/generated column expression configured through the fluent model.
    /// </summary>
    /// <param name="Sql">Provider SQL expression used to compute the column.</param>
    /// <param name="Stored">Whether the generated value should be physically stored when the provider supports that choice.</param>
    public record ComputedColumnConfiguration(string Sql, bool Stored = false);

    /// <summary>
    /// Describes provider identity seed/increment metadata for migration generation.
    /// </summary>
    public record IdentityOptionsConfiguration(long Seed, long Increment);

    /// <summary>
    /// Describes decimal precision/scale metadata for migration generation.
    /// </summary>
    public record PrecisionConfiguration(int Precision, int? Scale = null);

    /// <summary>
    /// Describes a provider-specific index over a SQL expression rather than a mapped property.
    /// </summary>
    public record ExpressionIndexConfiguration(string Name, string ExpressionSql, bool IsUnique = false, string? FilterSql = null)
    {
        /// <summary>Provider column names included as non-key covering columns where supported.</summary>
        public string[] IncludedColumnNames { get; init; } = Array.Empty<string>();

        /// <summary>Explicit provider null ordering for the expression key.</summary>
        public IndexNullSortOrder NullSortOrder { get; init; } = IndexNullSortOrder.Default;

        /// <summary>True when a unique PostgreSQL expression index treats null values as equal.</summary>
        public bool NullsNotDistinct { get; init; }
    }

    /// <summary>
    /// Configuration details for a relationship between entities including navigation and key information.
    /// </summary>
    /// <param name="PrincipalNavigation">Navigation property on the principal entity.</param>
    /// <param name="DependentType">CLR type of the dependent entity.</param>
    /// <param name="DependentNavigation">Optional navigation property on the dependent entity.</param>
    /// <param name="PrincipalKey">Key property on the principal entity.</param>
    /// <param name="ForeignKey">Foreign key property on the dependent entity.</param>
    /// <param name="CascadeDelete">Whether dependent entities should be cascade deleted through the tracked object graph.</param>
    public record RelationshipConfiguration(PropertyInfo PrincipalNavigation, Type DependentType,
        PropertyInfo? DependentNavigation, PropertyInfo? PrincipalKey, PropertyInfo ForeignKey, bool CascadeDelete = true)
    {
        /// <summary>Ordered principal key properties for this relationship.</summary>
        public IReadOnlyList<PropertyInfo> PrincipalKeys { get; init; } =
            PrincipalKey is null ? Array.Empty<PropertyInfo>() : new[] { PrincipalKey };

        /// <summary>Ordered foreign key properties on the dependent entity.</summary>
        public IReadOnlyList<PropertyInfo> ForeignKeys { get; init; } = new[] { ForeignKey };

        /// <summary>Database referential action to emit for principal deletes.</summary>
        public ReferentialAction OnDelete { get; init; } = CascadeDelete ? ReferentialAction.Cascade : ReferentialAction.NoAction;

        /// <summary>Database referential action to emit for principal key updates.</summary>
        public ReferentialAction OnUpdate { get; init; } = ReferentialAction.NoAction;

        /// <summary>Optional database foreign key constraint name to preserve in migration snapshots.</summary>
        public string? ConstraintName { get; init; }

        /// <summary>Creates a relationship configuration backed by multiple key columns.</summary>
        public RelationshipConfiguration(
            PropertyInfo principalNavigation,
            Type dependentType,
            PropertyInfo? dependentNavigation,
            IReadOnlyList<PropertyInfo> principalKeys,
            IReadOnlyList<PropertyInfo> foreignKeys,
            bool cascadeDelete = true)
            : this(
                principalNavigation,
                dependentType,
                dependentNavigation,
                principalKeys is { Count: > 0 } ? principalKeys[0] : null,
                foreignKeys is { Count: > 0 } ? foreignKeys[0] : throw new ArgumentException("At least one foreign key property is required.", nameof(foreignKeys)),
                cascadeDelete)
        {
            if (principalKeys is null) throw new ArgumentNullException(nameof(principalKeys));
            if (foreignKeys is null) throw new ArgumentNullException(nameof(foreignKeys));
            if (principalKeys.Count == 0)
                throw new ArgumentException("At least one principal key property is required.", nameof(principalKeys));
            if (principalKeys.Count != foreignKeys.Count)
                throw new ArgumentException("Principal key and foreign key property counts must match.", nameof(foreignKeys));

            PrincipalKeys = principalKeys.ToArray();
            ForeignKeys = foreignKeys.ToArray();
        }

        /// <summary>Creates a relationship configuration backed by multiple key columns and explicit database referential actions.</summary>
        public RelationshipConfiguration(
            PropertyInfo principalNavigation,
            Type dependentType,
            PropertyInfo? dependentNavigation,
            IReadOnlyList<PropertyInfo> principalKeys,
            IReadOnlyList<PropertyInfo> foreignKeys,
            ReferentialAction onDelete,
            ReferentialAction onUpdate)
            : this(
                principalNavigation,
                dependentType,
                dependentNavigation,
                principalKeys,
                foreignKeys,
                cascadeDelete: onDelete == ReferentialAction.Cascade)
        {
            OnDelete = onDelete;
            OnUpdate = onUpdate;
        }
    }

    /// <summary>Associates a value converter with a mapped property.</summary>
    public record ConverterConfiguration(PropertyInfo Property, IValueConverter Converter);

    /// <summary>
    /// Describes an owned collection navigation property stored in a separate child table.
    /// </summary>
    /// <param name="OwnedType">CLR element type of the owned collection.</param>
    /// <param name="TableName">Name of the child table storing owned items.</param>
    /// <param name="ForeignKeyName">Column name in the child table referencing the owner's PK.</param>
    /// <param name="Configuration">Optional configuration applied to the owned element type.</param>
    public record OwnedCollectionNavigation(Type OwnedType, string TableName, string ForeignKeyName, IEntityTypeConfiguration? Configuration = null)
    {
        /// <summary>Optional schema containing the child table.</summary>
        public string? SchemaName { get; init; }
    }

    /// <summary>
    /// Configures a many-to-many relationship between two entity types via a join table.
    /// </summary>
    /// <param name="NavPropertyName">Name of the collection navigation property on this entity.</param>
    /// <param name="RelatedType">The CLR type on the other side of the relationship.</param>
    /// <param name="JoinTableName">Name of the join (bridge) table.</param>
    /// <param name="LeftFkColumn">Column in the join table referencing this entity's PK.</param>
    /// <param name="RightFkColumn">Column in the join table referencing the related entity's PK.</param>
    /// <param name="RelatedNavPropertyName">Optional nav property name on the related type (for the inverse side).</param>
    public record ManyToManyConfiguration(
        string NavPropertyName,
        Type RelatedType,
        string JoinTableName,
        string LeftFkColumn,
        string RightFkColumn,
        string? RelatedNavPropertyName = null)
    {
        /// <summary>Optional schema containing the join table.</summary>
        public string? JoinTableSchema { get; init; }

        /// <summary>Ordered join-table columns referencing this entity's key.</summary>
        public IReadOnlyList<string> LeftFkColumns { get; init; } = new[] { LeftFkColumn };

        /// <summary>Ordered join-table columns referencing the related entity's key.</summary>
        public IReadOnlyList<string> RightFkColumns { get; init; } = new[] { RightFkColumn };

        /// <summary>
        /// Optional ordered key properties on the left entity. When omitted, the entity primary key is used.
        /// </summary>
        public IReadOnlyList<PropertyInfo>? LeftKeyProperties { get; init; }

        /// <summary>
        /// Optional ordered key properties on the right entity. When omitted, the related entity primary key is used.
        /// </summary>
        public IReadOnlyList<PropertyInfo>? RightKeyProperties { get; init; }

        /// <summary>Database delete action for the FK from the join table to this entity.</summary>
        public ReferentialAction LeftOnDelete { get; init; } = ReferentialAction.NoAction;

        /// <summary>Database update action for the FK from the join table to this entity.</summary>
        public ReferentialAction LeftOnUpdate { get; init; } = ReferentialAction.NoAction;

        /// <summary>Database delete action for the FK from the join table to the related entity.</summary>
        public ReferentialAction RightOnDelete { get; init; } = ReferentialAction.NoAction;

        /// <summary>Database update action for the FK from the join table to the related entity.</summary>
        public ReferentialAction RightOnUpdate { get; init; } = ReferentialAction.NoAction;
    }
}
