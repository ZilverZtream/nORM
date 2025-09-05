using System;
using System.Linq;
using System.Reflection;
using nORM.Core;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

public class WindowFunctionParameterTests : TestBase
{
    private class Item
    {
        public int Id { get; set; }
    }

    [Fact]
    public void Offset_parameter_does_not_collide_with_existing_parameter()
    {
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;
        using var ctx = new DbContext(connection, provider);
        var translator = new QueryTranslator(ctx);

        // Pre-populate parameter dictionary with an existing entry
        var pmField = typeof(QueryTranslator).GetField("_parameterManager", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var pm = (ParameterManager)pmField.GetValue(translator)!;
        var existingName = provider.ParamPrefix + "p0";
        pm.Parameters[existingName] = 999;
        pm.Index = 0; // Force next parameter to start from p0

        // Build query that introduces a window function offset parameter
        var query = ctx.Query<Item>()
            .WithLag(i => i.Id, 1, (i, prev) => new { i.Id, Prev = prev });

        var plan = translator.Translate(query.Expression);

        Assert.Equal(2, plan.Parameters.Count); // original + offset
        Assert.Equal(999, plan.Parameters[existingName]); // original value preserved
        Assert.Contains(plan.Parameters, kv => kv.Key != existingName && kv.Value.Equals(1));
    }
}
