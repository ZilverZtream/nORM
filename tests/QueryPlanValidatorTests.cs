using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

public class QueryPlanValidatorTests
{
    private static QueryPlan CreatePlan(string sql, Dictionary<string, object> parameters)
        => new(sql, parameters, new List<string>(), Materializer, typeof(int), false, false, false, string.Empty, new List<IncludePlan>(), null, Array.Empty<string>(), true, TimeSpan.FromSeconds(30));

    private static Task<object> Materializer(DbDataReader _, CancellationToken __) => Task.FromResult<object>(0);

    public static IEnumerable<object[]> Providers()
    {
        yield return new object[] { new SqlServerProvider() };
        yield return new object[] { new SqliteProvider() };
        yield return new object[] { new PostgresProvider() };
        yield return new object[] { new MySqlProvider() };
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Throws_when_sql_exceeds(DatabaseProvider provider)
    {
        if (provider.MaxSqlLength == int.MaxValue)
            return;

        var sql = new string('x', provider.MaxSqlLength + 1);
        var plan = CreatePlan(sql, new Dictionary<string, object>());
        Assert.Throws<InvalidOperationException>(() => QueryPlanValidator.Validate(plan, provider));
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Throws_when_too_many_params(DatabaseProvider provider)
    {
        var paramCount = provider.MaxParameters + 1;
        var parameters = new Dictionary<string, object>(paramCount);
        for (int i = 0; i < paramCount; i++)
            parameters[$"{provider.ParamPrefix}p{i}"] = i;

        var plan = CreatePlan("SELECT 1", parameters);
        Assert.Throws<InvalidOperationException>(() => QueryPlanValidator.Validate(plan, provider));
    }
}
