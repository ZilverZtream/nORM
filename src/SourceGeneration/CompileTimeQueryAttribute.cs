using System;

namespace nORM.SourceGeneration
{
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class CompileTimeQueryAttribute : Attribute
    {
        public Type EntityType { get; }
        public CompileTimeQueryAttribute(Type entityType) => EntityType = entityType;
    }
}
