#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendComputedColumnConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextComputedColumnInfo> computedColumnConfigurations)
        {
            foreach (var computed in OrderPropertyConfigurations(computedColumnConfigurations, static c => c.EntityName, static c => c.PropertyName))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(computed.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(computed.PropertyName);
                var sql = EscapeStringLiteral(computed.Sql);
                var storedSuffix = computed.Stored ? ", stored: true" : string.Empty;
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasComputedColumnSql(\"{sql}\"{storedSuffix});");
            }
        }

        private static void AppendCollationConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextCollationInfo> collationConfigurations)
        {
            foreach (var collation in OrderPropertyConfigurations(collationConfigurations, static c => c.EntityName, static c => c.PropertyName))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(collation.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(collation.PropertyName);
                var value = EscapeStringLiteral(collation.Collation);
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasCollation(\"{value}\");");
            }
        }
    }
}
