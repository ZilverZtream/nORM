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

    public record OwnedNavigation(Type OwnedType, IEntityTypeConfiguration? Configuration);
    public record ShadowPropertyConfiguration(Type ClrType, string? ColumnName = null);
    public record RelationshipConfiguration(PropertyInfo PrincipalNavigation, Type DependentType,
        PropertyInfo? DependentNavigation, PropertyInfo? PrincipalKey, PropertyInfo ForeignKey);
}