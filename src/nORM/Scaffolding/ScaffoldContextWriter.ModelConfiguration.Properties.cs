#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendDefaultValueConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextDefaultValueInfo> defaultValueConfigurations)
        {
            foreach (var defaultValue in defaultValueConfigurations
                .OrderBy(d => d.EntityName, StringComparer.Ordinal)
                .ThenBy(d => d.PropertyName, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(defaultValue.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(defaultValue.PropertyName);
                var sql = EscapeStringLiteral(defaultValue.DefaultValueSql);
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasDefaultValueSql(\"{sql}\");");
            }
        }

        private static void AppendIdentityOptionConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextIdentityOptionInfo> identityOptionConfigurations)
        {
            foreach (var identity in identityOptionConfigurations
                .OrderBy(i => i.EntityName, StringComparer.Ordinal)
                .ThenBy(i => i.PropertyName, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(identity.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(identity.PropertyName);
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasIdentityOptions({identity.Seed.ToString(CultureInfo.InvariantCulture)}, {identity.Increment.ToString(CultureInfo.InvariantCulture)});");
            }
        }

        private static void AppendPrecisionConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextPrecisionInfo> precisionConfigurations)
        {
            foreach (var precision in precisionConfigurations
                .OrderBy(p => p.EntityName, StringComparer.Ordinal)
                .ThenBy(p => p.PropertyName, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(precision.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(precision.PropertyName);
                var precisionValue = precision.Precision.ToString(CultureInfo.InvariantCulture);
                if (precision.Scale.HasValue)
                    sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasPrecision({precisionValue}, {precision.Scale.Value.ToString(CultureInfo.InvariantCulture)});");
                else
                    sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasPrecision({precisionValue});");
            }
        }

        private static void AppendColumnFacetConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextColumnFacetInfo> columnFacetConfigurations)
        {
            foreach (var facet in columnFacetConfigurations
                .OrderBy(f => f.EntityName, StringComparer.Ordinal)
                .ThenBy(f => f.PropertyName, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(facet.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(facet.PropertyName);
                var builder = $"mb.Entity<{entity}>().Property(e => e.{property})";
                if (facet.MaxLength.HasValue)
                    builder += $".HasMaxLength({facet.MaxLength.Value.ToString(CultureInfo.InvariantCulture)})";
                if (facet.IsUnicode.HasValue)
                    builder += $".IsUnicode({(facet.IsUnicode.Value ? "true" : "false")})";
                if (facet.IsFixedLength)
                    builder += ".IsFixedLength()";
                sb.AppendLine($"            {builder};");
            }
        }

        private static void AppendComputedColumnConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextComputedColumnInfo> computedColumnConfigurations)
        {
            foreach (var computed in computedColumnConfigurations
                .OrderBy(c => c.EntityName, StringComparer.Ordinal)
                .ThenBy(c => c.PropertyName, StringComparer.Ordinal))
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
            foreach (var collation in collationConfigurations
                .OrderBy(c => c.EntityName, StringComparer.Ordinal)
                .ThenBy(c => c.PropertyName, StringComparer.Ordinal))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(collation.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(collation.PropertyName);
                var value = EscapeStringLiteral(collation.Collation);
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasCollation(\"{value}\");");
            }
        }
    }
}
