using System;

namespace nORM.SourceGeneration
{
    /// <summary>
    /// Indicates that a method should be populated by the source generator with
    /// a pre-compiled query for the specified entity type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
    public sealed class CompileTimeQueryAttribute : Attribute
    {
        /// <summary>The entity type that the generated query should materialize.</summary>
        public Type EntityType { get; }

        public CompileTimeQueryAttribute(Type entityType) => EntityType = entityType;
    }
}
