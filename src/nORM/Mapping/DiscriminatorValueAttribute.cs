using System;

namespace nORM.Mapping
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DiscriminatorValueAttribute : Attribute
    {
        public object Value { get; }
        public DiscriminatorValueAttribute(object value)
            => Value = value;
    }
}
