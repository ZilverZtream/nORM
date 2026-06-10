using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldUnsupportedFeatureInfo(
        string TableKey,
        string Kind,
        string Name,
        string Detail)
    {
        public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
    }
}
