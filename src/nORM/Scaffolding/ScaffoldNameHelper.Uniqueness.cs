#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldNameHelper
    {
        public static string MakeUniqueContextName(string contextName, IEnumerable<string> entityNames)
        {
            var existingNames = new HashSet<string>(entityNames, StringComparer.OrdinalIgnoreCase);
            if (!existingNames.Contains(contextName))
                return contextName;

            var preferred = contextName.EndsWith("Context", StringComparison.Ordinal)
                ? contextName
                : contextName + "Context";
            return MakeUnique(preferred, existingNames);
        }

        public static string MakeUnique(string baseName, HashSet<string> existingNames)
        {
            var candidate = EscapeCSharpIdentifier(string.IsNullOrWhiteSpace(baseName) ? "_" : baseName);
            var unique = candidate;
            var suffix = 2;
            while (existingNames.Contains(unique))
            {
                unique = candidate + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
                suffix++;
            }

            existingNames.Add(unique);
            return unique;
        }
    }
}
