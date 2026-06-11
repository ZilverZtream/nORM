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
        private static void AppendRelationshipConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextRelationshipInfo> relationships)
        {
            foreach (var relationship in relationships
                .OrderBy(r => r.PrincipalEntityName, StringComparer.Ordinal)
                .ThenBy(r => r.DependentEntityName, StringComparer.Ordinal)
                .ThenBy(r => r.CollectionNavigationName, StringComparer.Ordinal)
                .ThenBy(r => r.ReferenceNavigationName, StringComparer.Ordinal)
                .ThenBy(r => r.ForeignKeyPropertyNames[0], StringComparer.Ordinal))
            {
                var principal = ScaffoldNameHelper.EscapeCSharpIdentifier(relationship.PrincipalEntityName);
                var dependent = ScaffoldNameHelper.EscapeCSharpIdentifier(relationship.DependentEntityName);
                var collection = ScaffoldNameHelper.EscapeCSharpIdentifier(relationship.CollectionNavigationName);
                var reference = ScaffoldNameHelper.EscapeCSharpIdentifier(relationship.ReferenceNavigationName);
                var foreignKey = FormatScaffoldKeySelector("d", relationship.ForeignKeyPropertyNames);
                var principalKey = FormatScaffoldKeySelector("p", relationship.PrincipalKeyPropertyNames);
                sb.AppendLine($"            mb.Entity<{principal}>()");
                sb.AppendLine(relationship.IsUniqueDependentKey
                    ? $"                .HasOne(p => p.{collection})"
                    : $"                .HasMany(p => p.{collection})");
                sb.AppendLine($"                .WithOne(d => d.{reference})");
                if (IsLegacyCascadeShape(relationship))
                {
                    AppendLegacyRelationshipForeignKey(sb, relationship, foreignKey, principalKey);
                }
                else
                {
                    var constraintNameSuffix = string.IsNullOrWhiteSpace(relationship.ConstraintName)
                        ? string.Empty
                        : $", \"{EscapeStringLiteral(relationship.ConstraintName)}\"";
                    sb.AppendLine($"                .HasForeignKey({foreignKey}, {principalKey}, {FormatReferentialAction(relationship.OnDelete)}, {FormatReferentialAction(relationship.OnUpdate)}{constraintNameSuffix});");
                }
            }
        }

        private static void AppendLegacyRelationshipForeignKey(StringBuilder sb, ScaffoldContextRelationshipInfo relationship, string foreignKey, string principalKey)
        {
            var cascadeSuffix = relationship.CascadeDelete ? string.Empty : ", cascadeDelete: false";
            if (!string.IsNullOrWhiteSpace(relationship.ConstraintName))
            {
                var constraintName = EscapeStringLiteral(relationship.ConstraintName);
                cascadeSuffix = relationship.CascadeDelete
                    ? $", \"{constraintName}\""
                    : $", \"{constraintName}\", false";
            }

            sb.AppendLine($"                .HasForeignKey({foreignKey}, {principalKey}{cascadeSuffix});");
        }

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
            var leftOnDelete = FormatReferentialAction(join.LeftOnDelete);
            var leftOnUpdate = FormatReferentialAction(join.LeftOnUpdate);
            var rightOnDelete = FormatReferentialAction(join.RightOnDelete);
            var rightOnUpdate = FormatReferentialAction(join.RightOnUpdate);
            return join.UsesPrimaryKeys
                ? $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftOnDelete}, {leftOnUpdate}, {rightOnDelete}, {rightOnUpdate}{schemaArgument});"
                : $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey}, {leftOnDelete}, {leftOnUpdate}, {rightOnDelete}, {rightOnUpdate}{schemaArgument});";
        }

        private static bool IsLegacyCascadeShape(ScaffoldContextRelationshipInfo relationship)
            => (string.Equals(relationship.OnDelete, "CASCADE", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(relationship.OnDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase))
               && string.Equals(relationship.OnUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase);

        private static string FormatReferentialAction(string action)
            => NormalizeReferentialAction(action) switch
            {
                "CASCADE" => "ReferentialAction.Cascade",
                "SET NULL" => "ReferentialAction.SetNull",
                "RESTRICT" => "ReferentialAction.Restrict",
                "SET DEFAULT" => "ReferentialAction.SetDefault",
                _ => "ReferentialAction.NoAction"
            };

        private static bool RequiresExplicitManyToManyReferentialActions(ScaffoldManyToManyJoinInfo join)
            => !IsDefaultReferentialAction(join.LeftOnDelete)
               || !IsDefaultReferentialAction(join.LeftOnUpdate)
               || !IsDefaultReferentialAction(join.RightOnDelete)
               || !IsDefaultReferentialAction(join.RightOnUpdate);

        private static bool IsDefaultReferentialAction(string action)
            => string.Equals(NormalizeReferentialAction(action), "NO ACTION", StringComparison.OrdinalIgnoreCase);

        private static string NormalizeReferentialAction(string? action)
            => string.IsNullOrWhiteSpace(action)
                ? "NO ACTION"
                : action.Replace('_', ' ').Trim().ToUpperInvariant();

        private static string FormatScaffoldKeySelector(string parameterName, IReadOnlyList<string> propertyNames)
        {
            if (propertyNames.Count == 1)
                return $"{parameterName} => {parameterName}.{ScaffoldNameHelper.EscapeCSharpIdentifier(propertyNames[0])}";

            return $"{parameterName} => new {{ {string.Join(", ", propertyNames.Select(name => parameterName + "." + ScaffoldNameHelper.EscapeCSharpIdentifier(name)))} }}";
        }

        private static string FormatStringArrayLiteral(IReadOnlyList<string> values)
            => "new[] { " + string.Join(", ", values.Select(value => "\"" + EscapeStringLiteral(value) + "\"")) + " }";
    }
}
