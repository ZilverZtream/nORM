#nullable enable
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendIdentityOptionConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextIdentityOptionInfo> identityOptionConfigurations)
        {
            foreach (var identity in OrderPropertyConfigurations(identityOptionConfigurations, static i => i.EntityName, static i => i.PropertyName))
            {
                var entity = ScaffoldNameHelper.EscapeCSharpIdentifier(identity.EntityName);
                var property = ScaffoldNameHelper.EscapeCSharpIdentifier(identity.PropertyName);
                sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasIdentityOptions({identity.Seed.ToString(CultureInfo.InvariantCulture)}, {identity.Increment.ToString(CultureInfo.InvariantCulture)});");
            }
        }

        private static void AppendPrecisionConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextPrecisionInfo> precisionConfigurations)
        {
            foreach (var precision in OrderPropertyConfigurations(precisionConfigurations, static p => p.EntityName, static p => p.PropertyName))
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
    }
}
