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

        private static string FormatManyToManyUsingTableCall(
            ScaffoldManyToManyJoinInfo join,
            string joinTable,
            string? joinSchema,
            string leftKey,
            string rightKey)
        {
            var schemaArgument = joinSchema is null ? string.Empty : $", schema: \"{joinSchema}\"";
            if (RequiresExplicitManyToManyReferentialActions(join))
                return FormatManyToManyUsingTableWithReferentialActions(join, joinTable, schemaArgument, leftKey, rightKey);

            if (joinSchema is null && !join.IsComposite && join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\");";

            if (joinSchema is null && join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)});";

            if (joinSchema is null && !join.IsComposite)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\", {leftKey}, {rightKey});";

            if (joinSchema is null)
                return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey});";

            if (!join.IsComposite && join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\", schema: \"{joinSchema}\");";

            if (join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, schema: \"{joinSchema}\");";

            if (!join.IsComposite)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\", {leftKey}, {rightKey}, schema: \"{joinSchema}\");";

            return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey}, schema: \"{joinSchema}\");";
        }

        private static string FormatManyToManyUsingTableWithReferentialActions(
            ScaffoldManyToManyJoinInfo join,
            string joinTable,
            string schemaArgument,
            string leftKey,
            string rightKey)
        {
            var leftOnDelete = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.LeftOnDelete);
            var leftOnUpdate = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.LeftOnUpdate);
            var rightOnDelete = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.RightOnDelete);
            var rightOnUpdate = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.RightOnUpdate);
            return join.UsesPrimaryKeys
                ? $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftOnDelete}, {leftOnUpdate}, {rightOnDelete}, {rightOnUpdate}{schemaArgument});"
                : $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey}, {leftOnDelete}, {leftOnUpdate}, {rightOnDelete}, {rightOnUpdate}{schemaArgument});";
        }

        private static bool RequiresExplicitManyToManyReferentialActions(ScaffoldManyToManyJoinInfo join)
            => !IsDefaultReferentialAction(join.LeftOnDelete)
               || !IsDefaultReferentialAction(join.LeftOnUpdate)
               || !IsDefaultReferentialAction(join.RightOnDelete)
               || !IsDefaultReferentialAction(join.RightOnUpdate);

        private static bool IsDefaultReferentialAction(string action)
            => ScaffoldReferentialAction.IsDefault(action);
    }
}
