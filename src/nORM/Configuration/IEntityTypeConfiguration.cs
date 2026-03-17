using System;
using System.Collections.Generic;
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
        List<PropertyInfo> KeyProperties { get; }

        /// <summary>
        /// Gets a mapping of property infos to explicit column names.
        /// </summary>
        Dictionary<PropertyInfo, string> ColumnNames { get; }

        /// <summary>
        /// Gets the CLR type that this entity shares its table with, if any.
        /// </summary>
        Type? TableSplitWith { get; }

        /// <summary>
        /// Gets owned navigation properties configured for this entity.
        /// </summary>
        Dictionary<PropertyInfo, OwnedNavigation> OwnedNavigations { get; }

        /// <summary>
        /// Gets the shadow properties defined for the entity type.
        /// </summary>
        Dictionary<string, ShadowPropertyConfiguration> ShadowProperties { get; }

        /// <summary>
        /// Gets the relationship configurations defined for the entity type.
        /// </summary>
        List<RelationshipConfiguration> Relationships { get; }

        /// <summary>Gets value converter configurations for this entity type.</summary>
        IReadOnlyList<ConverterConfiguration> Converters { get; }

        /// <summary>
        /// Gets owned collection navigation properties configured for this entity.
        /// Key is the collection navigation property; value describes the owned element type and configuration.
        /// </summary>
        Dictionary<PropertyInfo, OwnedCollectionNavigation> OwnedCollectionNavigations { get; }

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
    /// <param name="CascadeDelete">Whether dependent entities should be cascade deleted.</param>
    public record RelationshipConfiguration(PropertyInfo PrincipalNavigation, Type DependentType,
        PropertyInfo? DependentNavigation, PropertyInfo? PrincipalKey, PropertyInfo ForeignKey, bool CascadeDelete = true);

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
        string? RelatedNavPropertyName = null);
}