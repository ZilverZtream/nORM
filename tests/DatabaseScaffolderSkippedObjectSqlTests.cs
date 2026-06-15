#nullable enable

using System;
using System.Reflection;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void SqlServerSkippedObjectSql_AssemblesFunctionFragmentsWithKeywordBoundaries()
    {
        var method = typeof(ScaffoldSqlServerSkippedObjectDiscovery)
            .GetMethod("GetSkippedObjectSql", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(nameof(ScaffoldSqlServerSkippedObjectDiscovery), "GetSkippedObjectSql");

        var sql = (string)method.Invoke(null, null)!;

        Assert.Contains("FROM sys.objects o", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE o.is_ms_shipped = 0", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("))FROM", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("oWHERE", sql, StringComparison.OrdinalIgnoreCase);
    }
}
