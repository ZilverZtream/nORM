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
        private static void AppendCheckConstraintConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextCheckConstraintInfo> checkConstraintConfigurations)
        {
            foreach (var check in checkConstraintConfigurations
                .OrderBy(c => c.EntityName, StringComparer.Ordinal)
                .ThenBy(c => c.Name, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(check.EntityName);
                var name = EscapeStringLiteral(check.Name);
                var sql = EscapeStringLiteral(check.Sql);
                sb.AppendLine($"            mb.Entity<{entity}>().HasCheckConstraint(\"{name}\", \"{sql}\");");
            }
        }
    }
}
