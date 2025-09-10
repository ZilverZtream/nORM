using System;
using System.Collections.Generic;
using System.Reflection;

#nullable enable

namespace nORM.Configuration
{
    public interface IEntityTypeConfiguration
    {
        string? TableName { get; }
        List<PropertyInfo> KeyProperties { get; }
        Dictionary<PropertyInfo, string> ColumnNames { get; }
        Type? TableSplitWith { get; }
        Dictionary<PropertyInfo, OwnedNavigation> OwnedNavigations { get; }
        Dictionary<string, ShadowPropertyConfiguration> ShadowProperties { get; }
        List<RelationshipConfiguration> Relationships { get; }
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
}