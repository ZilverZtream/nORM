#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldContextInfo(
        string NamespaceName,
        string ContextName,
        IReadOnlyList<string> EntityNames,
        IReadOnlyList<ScaffoldContextRelationshipInfo> Relationships,
        IReadOnlyList<ScaffoldManyToManyJoinInfo> ManyToManyJoins,
        IReadOnlyList<ScaffoldRoutineStubInfo> RoutineStubs,
        IReadOnlyList<ScaffoldContextPrimaryKeyInfo> CompositePrimaryKeys,
        IReadOnlyList<ScaffoldContextDefaultValueInfo> DefaultValueConfigurations,
        IReadOnlyList<ScaffoldContextCheckConstraintInfo> CheckConstraintConfigurations,
        IReadOnlyList<ScaffoldContextComputedColumnInfo> ComputedColumnConfigurations,
        IReadOnlyList<ScaffoldContextExpressionIndexInfo> ExpressionIndexConfigurations,
        IReadOnlyList<ScaffoldContextCollationInfo> CollationConfigurations,
        IReadOnlyList<ScaffoldContextSequenceInfo> SequenceStubs,
        IReadOnlyList<ScaffoldContextIdentityOptionInfo> IdentityOptionConfigurations,
        IReadOnlyList<ScaffoldContextPrecisionInfo> PrecisionConfigurations,
        IReadOnlyList<ScaffoldContextColumnFacetInfo> ColumnFacetConfigurations,
        bool PluralizeQueryProperties,
        bool UseNullableReferenceTypes,
        string? EntityNamespaceName,
        bool UseDatabaseNames);

    internal readonly record struct ScaffoldContextRelationshipInfo(
        string DependentEntityName,
        string PrincipalEntityName,
        string ReferenceNavigationName,
        string CollectionNavigationName,
        bool IsUniqueDependentKey,
        bool CascadeDelete,
        string OnDelete,
        string OnUpdate,
        string? ConstraintName,
        IReadOnlyList<string> ForeignKeyPropertyNames,
        IReadOnlyList<string> PrincipalKeyPropertyNames);

    internal readonly record struct ScaffoldContextPrimaryKeyInfo(
        string EntityName,
        string[] PropertyNames,
        string? ConstraintName);

    internal readonly record struct ScaffoldContextDefaultValueInfo(
        string EntityName,
        string PropertyName,
        string DefaultValueSql);

    internal readonly record struct ScaffoldContextIdentityOptionInfo(
        string EntityName,
        string PropertyName,
        long Seed,
        long Increment);

    internal readonly record struct ScaffoldContextPrecisionInfo(
        string EntityName,
        string PropertyName,
        int Precision,
        int? Scale);

    internal readonly record struct ScaffoldContextColumnFacetInfo(
        string EntityName,
        string PropertyName,
        int? MaxLength,
        bool? IsUnicode,
        bool IsFixedLength);

    internal readonly record struct ScaffoldContextCheckConstraintInfo(
        string EntityName,
        string Name,
        string Sql);

    internal readonly record struct ScaffoldContextComputedColumnInfo(
        string EntityName,
        string PropertyName,
        string Sql,
        bool Stored);

    internal readonly record struct ScaffoldContextExpressionIndexInfo(
        string EntityName,
        string Name,
        string ExpressionSql,
        bool IsUnique,
        string? FilterSql);

    internal readonly record struct ScaffoldContextCollationInfo(
        string EntityName,
        string PropertyName,
        string Collation);

    internal readonly record struct ScaffoldContextSequenceInfo(
        string? Schema,
        string Name,
        string Detail,
        string? Comment);
}
