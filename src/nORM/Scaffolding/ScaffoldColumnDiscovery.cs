#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static IReadOnlyDictionary<string, IReadOnlySet<string>> ToReadOnlySetDictionary(
            Dictionary<string, HashSet<string>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlySet<string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
    }
}
