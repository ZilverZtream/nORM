using System.Collections.Generic;
using System.Reflection;

#nullable enable

namespace nORM.Configuration
{
    public interface IEntityTypeConfiguration
    {
        string? TableName { get; }
        PropertyInfo? KeyProperty { get; }
        Dictionary<PropertyInfo, string> ColumnNames { get; }
    }
}