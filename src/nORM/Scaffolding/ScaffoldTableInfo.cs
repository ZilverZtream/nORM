namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldTableInfo(string Name, string? Schema, string Kind = "Table");
}
