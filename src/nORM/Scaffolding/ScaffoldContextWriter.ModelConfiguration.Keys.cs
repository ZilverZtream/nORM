#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendPrimaryKeyConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextPrimaryKeyInfo> compositePrimaryKeys)
        {
            foreach (var key in compositePrimaryKeys.OrderBy(k => k.EntityName, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(key.EntityName);
                var constraintNameSuffix = string.IsNullOrWhiteSpace(key.ConstraintName)
                    ? string.Empty
                    : $", \"{EscapeStringLiteral(key.ConstraintName)}\"";
                sb.AppendLine($"            mb.Entity<{entity}>().HasKey({FormatScaffoldKeySelector("e", key.PropertyNames)}{constraintNameSuffix});");
            }
        }
    }
}
