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
        private static void AppendManyToManyConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldManyToManyJoinInfo> manyToManyJoins)
        {
            foreach (var join in manyToManyJoins
                .OrderBy(j => j.LeftEntityName, StringComparer.Ordinal)
                .ThenBy(j => j.RightEntityName, StringComparer.Ordinal)
                .ThenBy(j => j.LeftCollectionNavigationName, StringComparer.Ordinal)
                .ThenBy(j => j.RightCollectionNavigationName, StringComparer.Ordinal)
                .ThenBy(j => j.JoinTableKey, StringComparer.Ordinal))
            {
                AppendManyToManyConfiguration(sb, join);
            }
        }

        private static void AppendManyToManyConfiguration(StringBuilder sb, ScaffoldManyToManyJoinInfo join)
        {
            var left = ScaffoldNameHelper.EscapeCSharpIdentifier(join.LeftEntityName);
            var right = ScaffoldNameHelper.EscapeCSharpIdentifier(join.RightEntityName);
            var collection = ScaffoldNameHelper.EscapeCSharpIdentifier(join.LeftCollectionNavigationName);
            var inverseCollection = ScaffoldNameHelper.EscapeCSharpIdentifier(join.RightCollectionNavigationName);
            var joinTable = EscapeStringLiteral(join.JoinTableName);
            var joinSchema = join.JoinTableSchema is null ? null : EscapeStringLiteral(join.JoinTableSchema);
            var leftKey = FormatScaffoldKeySelector("p", join.LeftPrincipalKeyProperties);
            var rightKey = FormatScaffoldKeySelector("p", join.RightPrincipalKeyProperties);
            sb.AppendLine($"            mb.Entity<{left}>()");
            sb.AppendLine($"                .HasMany<{right}>(p => p.{collection})");
            sb.AppendLine($"                .WithMany(p => p.{inverseCollection})");
            sb.AppendLine($"                {FormatManyToManyUsingTableCall(join, joinTable, joinSchema, leftKey, rightKey)}");
        }
    }
}
