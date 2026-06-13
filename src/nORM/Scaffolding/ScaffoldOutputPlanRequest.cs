#nullable enable
using System.Data.Common;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldOutputPlanRequest(
        DbConnection Connection,
        DatabaseProvider Provider,
        string OutputDirectory,
        string ContextOutputDirectory,
        string NamespaceName,
        string ContextNamespace,
        string SafeContextName,
        ScaffoldModelDiscoveryResult Discovery,
        ScaffoldModelComposition Composition,
        ScaffoldOptions Options,
        ObjectPool<StringBuilder> StringBuilderPool);
}
