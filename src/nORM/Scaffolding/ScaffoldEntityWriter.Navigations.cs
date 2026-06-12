#nullable enable
using System;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityWriter
    {
        private static void AppendReferences(StringBuilder sb, System.Collections.Generic.IReadOnlyList<ScaffoldEntityReferenceInfo> references, bool useNullableReferenceTypes)
        {
            foreach (var reference in references
                .OrderBy(r => r.ReferenceNavigationName, StringComparer.Ordinal)
                .ThenBy(r => r.ForeignKeyPropertyName, StringComparer.Ordinal))
            {
                if (!reference.IsComposite)
                    sb.AppendLine($"    [ForeignKey(nameof({ScaffoldNameHelper.EscapeCSharpIdentifier(reference.ForeignKeyPropertyName)}))]");
                var nullableReferenceSuffix = useNullableReferenceTypes && !reference.IsRequired ? "?" : string.Empty;
                var initializer = useNullableReferenceTypes && reference.IsRequired ? " = default!;" : string.Empty;
                sb.AppendLine($"    public {ScaffoldNameHelper.EscapeCSharpIdentifier(reference.PrincipalEntityName)}{nullableReferenceSuffix} {ScaffoldNameHelper.EscapeCSharpIdentifier(reference.ReferenceNavigationName)} {{ get; set; }}{initializer}");
                sb.AppendLine();
            }
        }

        private static void AppendCollections(StringBuilder sb, System.Collections.Generic.IReadOnlyList<ScaffoldEntityCollectionInfo> collections, bool useNullableReferenceTypes)
        {
            foreach (var collection in collections
                .OrderBy(r => r.CollectionNavigationName, StringComparer.Ordinal)
                .ThenBy(r => r.ForeignKeyPropertyName, StringComparer.Ordinal))
            {
                if (collection.IsUniqueDependentKey)
                {
                    var nullableReferenceSuffix = useNullableReferenceTypes ? "?" : string.Empty;
                    sb.AppendLine($"    public {ScaffoldNameHelper.EscapeCSharpIdentifier(collection.DependentEntityName)}{nullableReferenceSuffix} {ScaffoldNameHelper.EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }}");
                }
                else
                {
                    sb.AppendLine($"    public List<{ScaffoldNameHelper.EscapeCSharpIdentifier(collection.DependentEntityName)}> {ScaffoldNameHelper.EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }} = new();");
                }

                sb.AppendLine();
            }
        }

        private static void AppendManyToManyCollections(StringBuilder sb, System.Collections.Generic.IReadOnlyList<ScaffoldEntityManyToManyNavigationInfo> manyToManyCollections)
        {
            foreach (var collection in manyToManyCollections
                .OrderBy(n => n.CollectionNavigationName, StringComparer.Ordinal)
                .ThenBy(n => n.TargetEntityName, StringComparer.Ordinal))
            {
                sb.AppendLine($"    public List<{ScaffoldNameHelper.EscapeCSharpIdentifier(collection.TargetEntityName)}> {ScaffoldNameHelper.EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }} = new();");
                sb.AppendLine();
            }
        }
    }
}
