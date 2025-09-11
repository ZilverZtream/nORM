using System;

namespace nORM.Mapping
{
    /// <summary>
    /// Marks a type as being owned by another entity, indicating its lifecycle is dependent on the owner.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class OwnedAttribute : Attribute
    {
    }
}
