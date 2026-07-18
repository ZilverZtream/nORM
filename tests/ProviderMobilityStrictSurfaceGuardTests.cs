using System;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class ProviderMobilityStrictSurfaceGuardTests
{
    [Fact]
    public void Provider_bound_public_DbContext_surface_is_explicitly_classified()
    {
        var providerBound = typeof(DbContext)
            .GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(IsProviderBoundMember)
            .Select(static member => member.MemberType + ":" + member.Name)
            .Distinct()
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();

        var expected = new[]
        {
            "Method:ApplyNativeTenantPolicyAsync",
            "Method:ApplyProviderNativeTemporalBootstrapAsync",
            "Method:CreateCompiledQueryCommandAsync",
            "Method:CreateSavepointAsync",
            "Method:DropNativeTenantPolicyAsync",
            "Method:ExecuteCompiledQueryListAsync",
            "Method:ExecuteStoredProcedureAsAsyncEnumerable",
            "Method:ExecuteStoredProcedureAsync",
            "Method:ExecuteStoredProcedureNonQueryAsync",
            "Method:ExecuteStoredProcedureNonQueryWithOutputAsync",
            "Method:ExecuteStoredProcedureWithOutputAsync",
            "Method:FromSqlInterpolatedAsync",
            "Method:FromSqlRawAsync",
            "Method:GenerateNativeTenantPolicySql",
            "Method:GenerateProviderNativeTemporalBootstrapSql",
            "Method:GetCompiledQueryMaterializer",
            "Method:InsertAsync",
            "Method:Query",
            "Method:QueryUnchangedAsync",
            "Method:QueryUnchangedInterpolatedAsync",
            "Method:RollbackToSavepointAsync",
            "Method:SqlQueryInterpolatedAsync",
            "Method:SqlQueryRawAsync",
            "Method:UpdateAsync",
            "Method:DeleteAsync",
            "Property:Connection",
            "Property:Provider"
        }.OrderBy(static value => value, StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, providerBound);
    }

    [Fact]
    public void Provider_bound_public_transaction_facades_are_explicitly_classified()
    {
        Assert.Equal(
            new[]
            {
                "Method:ExecuteSqlInterpolated",
                "Method:ExecuteSqlInterpolatedAsync",
                "Method:ExecuteSqlRaw",
                "Method:ExecuteSqlRawAsync",
                "Method:GetDbConnection",
                "Method:UseTransaction",
                "Property:CurrentTransaction"
            },
            ProviderBoundMembers(typeof(DatabaseFacade)));

        Assert.Equal(
            new[] { "Property:Transaction" },
            ProviderBoundMembers(typeof(DbContextTransaction)));
    }

    private static string[] ProviderBoundMembers(Type type)
        => type.GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(IsProviderBoundMember)
            .Select(static member => member.MemberType + ":" + member.Name)
            .Distinct()
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();

    private static bool IsProviderBoundMember(MemberInfo member)
        => member switch
        {
            PropertyInfo property => IsProviderBoundType(property.PropertyType) ||
                                     IsProviderBoundName(property.Name),
            MethodInfo method when method.IsSpecialName => false,
            MethodInfo method when method.DeclaringType == typeof(DbContextTransaction)
                && method.Name.Contains("Savepoint", StringComparison.Ordinal) => false,
            MethodInfo method => IsProviderBoundType(method.ReturnType) ||
                                 method.GetParameters().Any(static parameter => IsProviderBoundType(parameter.ParameterType)) ||
                                 IsDynamicTableQuery(method) ||
                                 IsProviderBoundName(method.Name),
            _ => false
        };

    private static bool IsProviderBoundType(Type type)
    {
        var unwrapped = Nullable.GetUnderlyingType(type) ?? type;
        if (unwrapped.IsGenericType)
            unwrapped = unwrapped.GetGenericArguments()[0];

        return typeof(DbConnection).IsAssignableFrom(unwrapped) ||
               typeof(DbCommand).IsAssignableFrom(unwrapped) ||
               typeof(DbTransaction).IsAssignableFrom(unwrapped) ||
               typeof(DatabaseProvider).IsAssignableFrom(unwrapped);
    }

    private static bool IsProviderBoundName(string name)
        => name.Contains("Sql", StringComparison.Ordinal) ||
           name.Contains("QueryUnchanged", StringComparison.Ordinal) ||
           name.Contains("StoredProcedure", StringComparison.Ordinal) ||
           name.Contains("NativeTenant", StringComparison.Ordinal) ||
           name.Contains("ProviderNative", StringComparison.Ordinal) ||
           name.Contains("CompiledQueryCommand", StringComparison.Ordinal) ||
           name.Contains("CompiledQueryMaterializer", StringComparison.Ordinal) ||
           name.Contains("Savepoint", StringComparison.Ordinal);

    private static bool IsDynamicTableQuery(MethodInfo method)
        => method.Name == "Query" &&
           method.GetParameters() is { Length: 1 } parameters &&
           parameters[0].ParameterType == typeof(string);
}
