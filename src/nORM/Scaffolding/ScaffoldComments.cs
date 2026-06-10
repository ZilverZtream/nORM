#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldComments(
        string? TableComment,
        IReadOnlyDictionary<string, string> ColumnComments);
}
