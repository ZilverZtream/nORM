#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldRelationshipInfo(
        string DependentTableKey,
        string PrincipalTableKey,
        string DependentEntityName,
        string PrincipalEntityName,
        string ForeignKeyPropertyName,
        string PrincipalKeyPropertyName,
        string ReferenceNavigationName,
        string CollectionNavigationName,
        bool IsUniqueDependentKey,
        bool CascadeDelete,
        string OnDelete,
        string OnUpdate,
        string? ConstraintName)
    {
        public IReadOnlyList<string> ForeignKeyPropertyNames { get; init; } = new[] { ForeignKeyPropertyName };

        public IReadOnlyList<string> PrincipalKeyPropertyNames { get; init; } = new[] { PrincipalKeyPropertyName };

        public bool IsRequired { get; init; }

        public bool IsComposite => ForeignKeyPropertyNames.Count > 1 || PrincipalKeyPropertyNames.Count > 1;
    }
}
