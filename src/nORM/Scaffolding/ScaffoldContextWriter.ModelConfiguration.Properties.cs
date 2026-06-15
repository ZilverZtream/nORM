#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static IOrderedEnumerable<T> OrderPropertyConfigurations<T>(
            IReadOnlyList<T> configurations,
            Func<T, string> entityName,
            Func<T, string> propertyName)
            => configurations
                .OrderBy(entityName, StringComparer.Ordinal)
                .ThenBy(propertyName, StringComparer.Ordinal);
    }
}
