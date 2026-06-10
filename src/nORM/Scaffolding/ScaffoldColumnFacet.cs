#nullable enable

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldColumnFacet(
        int? MaxLength,
        bool? IsUnicode,
        bool IsFixedLength);
}
