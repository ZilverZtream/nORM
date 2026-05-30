using System;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Marks an entity type as query-only. nORM can materialize rows for the
    /// type, but insert, update, delete, and tracked SaveChanges writes are
    /// rejected before SQL is generated.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public sealed class ReadOnlyEntityAttribute : Attribute
    {
    }
}
