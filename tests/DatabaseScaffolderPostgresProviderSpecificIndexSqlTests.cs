#nullable enable

using System;
using System.Reflection;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void PostgresProviderSpecificBtreeOptionSql_AssemblesFragmentsWithKeywordBoundaries()
    {
        var field = typeof(ScaffoldPostgresProviderSpecificIndexFeatureDiscovery)
            .GetField("ProviderSpecificBtreeOptionSql", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingFieldException(nameof(ScaffoldPostgresProviderSpecificIndexFeatureDiscovery), "ProviderSpecificBtreeOptionSql");

        var sql = (string)field.GetValue(null)!;

        Assert.Contains("FROM pg_index ix", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE ix.indisprimary = false", sql, StringComparison.Ordinal);
        Assert.Contains("'; hasNullsNotDistinct='", sql, StringComparison.Ordinal);
        Assert.Contains("'; indexSql=' || pg_get_indexdef(ix.indexrelid))::text AS Detail", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DetailFROM", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("amWHERE", sql, StringComparison.OrdinalIgnoreCase);
    }
}
