namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldForeignKeyInfo(
        string? DependentSchema,
        string DependentTable,
        string DependentColumn,
        string? PrincipalSchema,
        string PrincipalTable,
        string PrincipalColumn,
        string ConstraintName,
        int ColumnCount,
        string OnDelete = "NO ACTION",
        string OnUpdate = "NO ACTION",
        bool IsSyntheticConstraintName = false);
}
