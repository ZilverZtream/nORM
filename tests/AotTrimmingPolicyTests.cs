using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using nORM.Core;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public class AotTrimmingPolicyTests
{
    [Fact]
    public void Dynamic_table_query_is_annotated_for_aot_and_trimming()
    {
        var method = typeof(DbContext).GetMethod(nameof(DbContext.Query), new[] { typeof(string) })
            ?? throw new MissingMethodException(nameof(DbContext), nameof(DbContext.Query));

        Assert.NotNull(method.GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(method.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
    }

    [Fact]
    public void Dynamic_entity_generator_is_annotated_for_aot_and_trimming()
    {
        Assert.NotNull(typeof(DynamicEntityTypeGenerator).GetCustomAttribute<RequiresDynamicCodeAttribute>());
        Assert.NotNull(typeof(DynamicEntityTypeGenerator).GetCustomAttribute<RequiresUnreferencedCodeAttribute>());

        var methods = typeof(DynamicEntityTypeGenerator)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m.Name is nameof(DynamicEntityTypeGenerator.GenerateEntityType) or nameof(DynamicEntityTypeGenerator.GenerateEntityTypeAsync))
            .ToArray();

        Assert.NotEmpty(methods);
        foreach (var method in methods)
        {
            Assert.NotNull(method.GetCustomAttribute<RequiresDynamicCodeAttribute>());
            Assert.NotNull(method.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
        }
    }
}
