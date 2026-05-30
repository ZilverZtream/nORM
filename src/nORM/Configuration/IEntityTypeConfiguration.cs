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
        /// Gets the collection of properties that compose the primary key.
        /// </summary>
        IReadOnlyList<PropertyInfo> KeyProperties { get; }

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
    public record OwnedCollectionNavigation(Type OwnedType, string TableName, string ForeignKeyName, IEntityTypeConfiguration? Configuration = null);

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
    }
}
