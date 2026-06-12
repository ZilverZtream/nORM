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
