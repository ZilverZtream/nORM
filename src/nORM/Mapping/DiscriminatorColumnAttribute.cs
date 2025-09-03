using System;

namespace nORM.Mapping
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DiscriminatorColumnAttribute : Attribute
    {
        public string PropertyName { get; }
        public DiscriminatorColumnAttribute(string propertyName)
            => PropertyName = propertyName;
    }
}
