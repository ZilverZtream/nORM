#nullable enable
using System.Data.Common;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldEntityFileSetRequest(
        DbConnection Connection,
        DatabaseProvider Provider,
        string OutputDirectory,
        string NamespaceName,
        ScaffoldModelDiscoveryResult Discovery,
        ScaffoldModelComposition Composition,
        ScaffoldOptions Options);
}
