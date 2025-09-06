using System.Linq;
using System.Collections.Generic;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class JoinTests : TestBase
{
    private class Order
    {
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
    }

    private class JoinDto
    {
        public decimal Amount { get; set; }
    }

    private class SelfJoinDto
    {
        public decimal OuterAmount { get; set; }
        public decimal InnerAmount { get; set; }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_on_join_projection_translates(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Order, JoinDto>(q =>
            q.Join(q, o => o.Id, o2 => o2.Id, (o, o2) => new JoinDto { Amount = o2.Amount })
             .Where(x => x.Amount > 100),
            connection, provider);

        Assert.Contains(provider.Escape("Amount"), sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Self_join_projection_maps_properties_to_correct_aliases(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Order, SelfJoinDto>(q =>
            q.Join(q, o => o.Id, o2 => o2.Id, (o, o2) => new SelfJoinDto
            {
                OuterAmount = o.Amount,
                InnerAmount = o2.Amount
            })
            .Where(x => x.InnerAmount > 0 && x.OuterAmount > 0),
            connection, provider);

        var col = provider.Escape("Amount");
        var outerAlias = provider.Escape("T0");
        var innerAlias = provider.Escape("T1");
        Assert.Contains($"{outerAlias}.{col}", sql);
        Assert.Contains($"{innerAlias}.{col}", sql);
    }

    public static IEnumerable<object[]> Providers()
    {
        yield return new object[] { ProviderKind.Sqlite };
        yield return new object[] { ProviderKind.SqlServer };
        yield return new object[] { ProviderKind.MySql };
    }
}
