#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        internal readonly record struct ScaffoldTable(string Name, string? Schema);

        internal readonly record struct ScaffoldSkippedObject(
            string? Schema,
            string Name,
            string Kind,
            string Detail,
            string? Comment);

        internal readonly record struct ScaffoldPrimaryKey(
            string EntityName,
            string[] PropertyNames,
            string? ConstraintName);

        internal readonly record struct ScaffoldUnsupportedFeature(
            string TableKey,
            string Kind,
            string Name,
            string Detail)
        {
            public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
        }
    }
}
