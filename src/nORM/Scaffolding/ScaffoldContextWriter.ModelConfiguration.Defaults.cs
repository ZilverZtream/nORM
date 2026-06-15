#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendDefaultValueConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextDefaultValueInfo> defaultValueConfigurations)
        {
            foreach (var defaultValue in OrderPropertyConfigurations(defaultValueConfigurations, static d => d.EntityName, static d => d.PropertyName))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(defaultValue.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(defaultValue.PropertyName);
                var sql = EscapeStringLiteral(defaultValue.DefaultValueSql);
                var constraintSuffix = string.IsNullOrWhiteSpace(defaultValue.ConstraintName)
                    ? string.Empty
                    : $", constraintName: \"{EscapeStringLiteral(defaultValue.ConstraintName!)}\"";
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasDefaultValueSql(\"{sql}\"{constraintSuffix});");
            }
        }
    }
}
