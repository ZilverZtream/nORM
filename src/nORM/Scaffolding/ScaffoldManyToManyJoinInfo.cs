#nullable enable

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldManyToManyJoinInfo(
        string JoinTableKey,
        string LeftTableKey,
        string RightTableKey,
        string JoinTableName,
        string? JoinTableSchema,
        string LeftEntityName,
        string RightEntityName,
        string[] LeftForeignKeyColumns,
        string[] RightForeignKeyColumns,
        string[] LeftPrincipalKeyProperties,
        string[] RightPrincipalKeyProperties,
        string LeftOnDelete,
        string LeftOnUpdate,
        string RightOnDelete,
        string RightOnUpdate,
        bool UsesPrimaryKeys,
        string LeftCollectionNavigationName,
        string RightCollectionNavigationName)
    {
        public string LeftForeignKeyColumn => LeftForeignKeyColumns[0];

        public string RightForeignKeyColumn => RightForeignKeyColumns[0];

        public bool IsComposite => LeftForeignKeyColumns.Length > 1 || RightForeignKeyColumns.Length > 1;
    }
}
