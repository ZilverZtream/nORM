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
        private static void AppendExpressionIndexConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextExpressionIndexInfo> expressionIndexConfigurations)
        {
            foreach (var expressionIndex in expressionIndexConfigurations
                .OrderBy(i => i.EntityName, StringComparer.Ordinal)
                .ThenBy(i => i.Name, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(expressionIndex.EntityName);
                var name = EscapeStringLiteral(expressionIndex.Name);
                var expressionSql = EscapeStringLiteral(expressionIndex.ExpressionSql);
                var uniqueSuffix = expressionIndex.IsUnique ? ", isUnique: true" : string.Empty;
                var filterSuffix = string.IsNullOrWhiteSpace(expressionIndex.FilterSql) ? string.Empty : $", filterSql: \"{EscapeStringLiteral(expressionIndex.FilterSql)}\"";
                sb.AppendLine($"            mb.Entity<{entity}>().HasExpressionIndex(\"{name}\", \"{expressionSql}\"{uniqueSuffix}{filterSuffix});");
            }
        }
    }
}
