#nullable enable
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendColumnFacetConfigurations(StringBuilder sb, IReadOnlyList<ScaffoldContextColumnFacetInfo> columnFacetConfigurations)
        {
            foreach (var facet in OrderPropertyConfigurations(columnFacetConfigurations, static f => f.EntityName, static f => f.PropertyName))
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
    }
}
