using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class GlobalFiltersTests : TestBase
{
    private class SoftEntity
    {
        public int Id { get; set; }
        public bool IsDeleted { get; set; }
    }

    public static IEnumerable<object[]> Providers()
    {
        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
            yield return new object[] { provider };
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Global_filter_is_applied_to_queries(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        var options = new DbContextOptions();
        options.AddGlobalFilter<SoftEntity>(e => !e.IsDeleted);
        using var ctx = new DbContext(connection, provider, options);

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SoftEntity (Id INTEGER PRIMARY KEY, IsDeleted INTEGER);" +
                              "INSERT INTO SoftEntity (Id, IsDeleted) VALUES (1, 0), (2, 1);";
            cmd.ExecuteNonQuery();
        }

        var results = ctx.Query<SoftEntity>().ToList();

        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }
}
