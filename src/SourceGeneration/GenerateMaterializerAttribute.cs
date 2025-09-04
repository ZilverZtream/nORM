using System;

namespace nORM.SourceGeneration
{
    /// <summary>
    /// Marks an entity type for which a materializer should be generated at
    /// compile time. The associated source generator will produce a method that
    /// can populate an instance of the decorated type directly from a
    /// <see cref="System.Data.Common.DbDataReader"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class GenerateMaterializerAttribute : Attribute
    {
    }
}
