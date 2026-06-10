#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldCompositeForeignKeyDiagnosticInfo(
        string ConstraintName,
        string DependentTable,
        string[] DependentColumns,
        string PrincipalTable,
        string[] PrincipalColumns,
        IReadOnlyDictionary<string, object?> Metadata);

    internal readonly record struct ScaffoldPossibleJoinTableDiagnosticInfo(
        string TableKey,
        string[] PrincipalTables,
        string[] ConstraintNames,
        string[] Reasons,
        IReadOnlyDictionary<string, object?> Metadata);
}
