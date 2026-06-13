#nullable enable

using System;
using System.IO;
using System.Text;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void ScaffoldContext_WithSqlServerTableValuedFunction_EmitsSelectWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "GetRevenueRows",
            "SQL Server table-valued function; parameters=1; outputParameters=0; callShape=table-valued-function; parameterModes=@tenantId:IN:int; dataType=TABLE; resultColumns=Id:int:0|Name:nvarchar(40):0");

        Assert.Contains("Executes provider-bound table-valued function `dbo.GetRevenueRows`", code);
        Assert.Contains("public sealed class GetRevenueRowsParameters", code);
        Assert.Contains("public int? tenantId { get; init; }", code);
        Assert.Contains("public sealed class GetRevenueRowsResult", code);
        Assert.Contains("public int Id { get; set; }", code);
        Assert.Contains("public string Name { get; set; } = default!;", code);
        Assert.Contains("var args = parameters is null ? System.Array.Empty<object>() : new object[] { (object?)parameters.tenantId ?? System.DBNull.Value };", code);
        Assert.Contains("if (args.Length != 1)", code);
        Assert.Contains("Function `dbo.GetRevenueRows` was scaffolded with 1 input parameters", code);
        Assert.Contains("Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenueRows\")", code);
        Assert.Contains("SELECT * FROM ", code);
        Assert.Contains("QueryUnchangedAsync<TResult>", code);
        Assert.Contains("Task<List<GetRevenueRowsResult>> GetRevenueRowsAsync(GetRevenueRowsParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("QueryUnchangedAsync<GetRevenueRowsResult>", code);
        Assert.Contains("IAsyncEnumerable<TResult> StreamGetRevenueRowsAsync<TResult>", code);
        Assert.Contains("IAsyncEnumerable<GetRevenueRowsResult> StreamGetRevenueRowsAsync(GetRevenueRowsParameters? parameters = null", code);
        Assert.Contains("QueryUnchangedStreamAsync<TResult>", code);
        Assert.Contains("QueryUnchangedStreamAsync<GetRevenueRowsResult>", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>(\"dbo.GetRevenueRows\"", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_tvf_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithSqlServerScalarFunction_EmitsValueProjectionWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "CalculateRisk",
            "SQL Server scalar function; parameters=1; outputParameters=1; callShape=scalar-function; parameterModes=@customerId:IN:int,return:RETURN:int; dataType=int");

        Assert.Contains("Executes provider-bound scalar function `dbo.CalculateRisk`", code);
        Assert.Contains("public sealed class CalculateRiskParameters", code);
        Assert.Contains("public int? customerId { get; init; }", code);
        Assert.Contains("private sealed class CalculateRiskValueResult<TValue>", code);
        Assert.Contains("Task<TValue?> CalculateRiskValueAsync<TValue>(CalculateRiskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("QueryUnchangedAsync<CalculateRiskValueResult<TValue>>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args)", code);
        Assert.Contains("return rows.Count == 0 ? default : rows[0].Value;", code);
        Assert.Contains("SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.Contains("QueryUnchangedAsync<TResult>", code);
        Assert.DoesNotContain("WithOutputAsync", code);
        Assert.True(
            code.IndexOf("Executes provider-bound scalar function `dbo.CalculateRisk`", StringComparison.Ordinal) <
            code.IndexOf("public Task<List<TResult>> CalculateRiskAsync<TResult>", StringComparison.Ordinal));
        Assert.True(
            code.IndexOf("public Task<List<TResult>> CalculateRiskAsync<TResult>", StringComparison.Ordinal) <
            code.IndexOf("private sealed class CalculateRiskValueResult<TValue>", StringComparison.Ordinal));

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_scalar_fn_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithPostgresFunction_EmitsSelectInvocationWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate_risk",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=customer_id:IN:integer; dataType=integer");

        Assert.Contains("Executes provider-bound function `public.calculate_risk`", code);
        Assert.Contains("public int? customer_id { get; init; }", code);
        Assert.Contains("Task<TValue?> CalculateRiskValueAsync<TValue>(CalculateRiskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>", code);
    }

    [Fact]
    public void ScaffoldContext_WithPostgresFunctionAdvancedParameterTypes_EmitsTypedParameterDto()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate_campaign",
            "PostgreSQL function; parameters=6; outputParameters=0; parameterModes=tenant_id:IN:integer,duration:IN:interval,customer_ids:IN:ARRAY (_int4),labels:IN:ARRAY (_text),note:IN:USER-DEFINED (citext),trace_id:IN:USER-DEFINED (uuid); dataType=integer");

        Assert.Contains("using System;", code);
        Assert.Contains("public int? tenant_id { get; init; }", code);
        Assert.Contains("public TimeSpan? duration { get; init; }", code);
        Assert.Contains("public int[]? customer_ids { get; init; }", code);
        Assert.Contains("public string[]? labels { get; init; }", code);
        Assert.Contains("public string? note { get; init; }", code);
        Assert.Contains("public Guid? trace_id { get; init; }", code);
        Assert.Contains("if (args.Length != 6)", code);
        Assert.Contains("var casts = new[] { \"integer\", \"interval\", \"integer[]\", \"text[]\", \"citext\", \"uuid\" };", code);
        Assert.Contains("Provider.ParamPrefix + \"p\" + i + \"::\" + casts[i]", code);
        Assert.DoesNotContain("public object? duration { get; init; }", code);
        Assert.DoesNotContain("public object? customer_ids { get; init; }", code);
        Assert.DoesNotContain("public object? trace_id { get; init; }", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_pg_routine_types_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithPostgresFunctionParameterizedArrayTypes_EmitsTypedParameterDto()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "score_campaign",
            "PostgreSQL function; parameters=2; outputParameters=0; parameterModes=ratings:IN:ARRAY (numeric(10,2)),labels:IN:ARRAY (varchar(32)); dataType=integer");

        Assert.Contains("public decimal[]? ratings { get; init; }", code);
        Assert.Contains("public string[]? labels { get; init; }", code);
        Assert.Contains("var casts = new[] { \"numeric[]\", \"character varying[]\" };", code);
        Assert.DoesNotContain("public object? ratings { get; init; }", code);
        Assert.DoesNotContain("public object? labels { get; init; }", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_pg_param_array_types_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithFunctionParametersThatCannotBecomeDto_UsesPositionalArguments()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate odd",
            "PostgreSQL function; parameters=2; outputParameters=0; parameterModes=tenant-id:IN:integer,search text:IN:text; dataType=integer");

        Assert.DoesNotContain("public sealed class CalculateOddParameters", code);
        Assert.Contains("Task<TValue?> CalculateOddValueAsync<TValue>(object?[]? arguments = null, CancellationToken ct = default)", code);
        Assert.Contains("var args = arguments is null ? System.Array.Empty<object>() : System.Array.ConvertAll(arguments, value => (object)(value ?? System.DBNull.Value));", code);
        Assert.Contains("if (args.Length != 2)", code);
        Assert.Contains("Function `public.calculate odd` was scaffolded with 2 input parameters", code);
        Assert.Contains("pass exactly 2 arguments in scaffolded order", code);
        Assert.DoesNotContain("var args = System.Array.Empty<object>();", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_positional_function_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithColonDelimitedFunctionParameterNames_ParsesModeTokenBeforePositionalFallback()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate odd",
            "PostgreSQL function; parameters=2; outputParameters=0; parameterModes=tenant:id:IN:integer,search:text:IN:text; dataType=integer");

        Assert.DoesNotContain("public sealed class CalculateOddParameters", code);
        Assert.Contains("Task<TValue?> CalculateOddValueAsync<TValue>(object?[]? arguments = null, CancellationToken ct = default)", code);
        Assert.Contains("if (args.Length != 2)", code);
        Assert.Contains("var casts = new[] { \"integer\", \"text\" };", code);
        Assert.Contains("Parameters discovered at scaffold time: tenant:id IN integer, search:text IN text", code);
        Assert.DoesNotContain("public object? tenant", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_colon_function_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithModeLikeTextInsideFunctionParameterName_UsesTerminalModeToken()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate odd",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=tenant:IN:name:IN:integer; dataType=integer");

        Assert.DoesNotContain("public sealed class CalculateOddParameters", code);
        Assert.Contains("Task<TValue?> CalculateOddValueAsync<TValue>(object?[]? arguments = null, CancellationToken ct = default)", code);
        Assert.Contains("if (args.Length != 1)", code);
        Assert.Contains("var casts = new[] { \"integer\" };", code);
        Assert.Contains("Parameters discovered at scaffold time: tenant:IN:name IN integer", code);
        Assert.DoesNotContain("public object? tenant", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_mode_text_function_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithSemicolonDelimitedFunctionParameterNames_PreservesGeneratedMetadataFields()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate odd",
            "PostgreSQL function; parameters=2; outputParameters=0; parameterModes=tenant;id:IN:integer,search;text:IN:text; dataType=integer");

        Assert.DoesNotContain("public sealed class CalculateOddParameters", code);
        Assert.Contains("Task<TValue?> CalculateOddValueAsync<TValue>(object?[]? arguments = null, CancellationToken ct = default)", code);
        Assert.Contains("if (args.Length != 2)", code);
        Assert.Contains("var casts = new[] { \"integer\", \"text\" };", code);
        Assert.Contains("Parameters discovered at scaffold time: tenant;id IN integer, search;text IN text", code);
        Assert.DoesNotContain("var args = System.Array.Empty<object>();", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_semicolon_function_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithAllowListedKeyTextInsideFunctionParameterName_KeepsRoutineFieldsIntact()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate odd",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=tenant; dataType=retained:IN:integer; dataType=integer");

        Assert.DoesNotContain("public sealed class CalculateOddParameters", code);
        Assert.Contains("Task<TValue?> CalculateOddValueAsync<TValue>(object?[]? arguments = null, CancellationToken ct = default)", code);
        Assert.Contains("if (args.Length != 1)", code);
        Assert.Contains("var casts = new[] { \"integer\" };", code);
        Assert.Contains("Parameters discovered at scaffold time: tenant; dataType=retained IN integer", code);
        Assert.DoesNotContain("var args = System.Array.Empty<object>();", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_key_text_function_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithCommaDelimitedFunctionParameterNames_SplitsOnlyCompleteParameters()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate odd",
            "PostgreSQL function; parameters=2; outputParameters=0; parameterModes=tenant,id:IN:integer,search,text:IN:text; dataType=integer");

        Assert.DoesNotContain("public sealed class CalculateOddParameters", code);
        Assert.Contains("Task<TValue?> CalculateOddValueAsync<TValue>(object?[]? arguments = null, CancellationToken ct = default)", code);
        Assert.Contains("if (args.Length != 2)", code);
        Assert.Contains("var casts = new[] { \"integer\", \"text\" };", code);
        Assert.Contains("Parameters discovered at scaffold time: tenant,id IN integer, search,text IN text", code);
        Assert.DoesNotContain("var args = System.Array.Empty<object>();", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_comma_function_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithStoredProcedureParametersThatCannotBecomeDto_UsesDictionaryArguments()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "import odd",
            "SQL Server stored procedure; parameters=2; outputParameters=0; parameterModes=@tenant-id:IN:int,@search text:IN:nvarchar(64)");

        Assert.DoesNotContain("public sealed class ImportOddParameters", code);
        Assert.Contains("Task<List<TResult>> ImportOddAsync<TResult>(IReadOnlyDictionary<string, object?>? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("IAsyncEnumerable<TResult> StreamImportOddAsync<TResult>(IReadOnlyDictionary<string, object?>? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"import odd\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"import odd\")))", code);
        Assert.Contains("ExecuteStoredProcedureAsAsyncEnumerable<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"import odd\"), ct, RequireScaffoldedRoutineParameters(parameters, 2, Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"import odd\")))", code);
        Assert.Contains("pass exactly {expectedInputCount} dictionary entries using the provider parameter names", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_dictionary_routine_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithPostgresTableFunction_EmitsSelectStarInvocationWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "customer_orders",
            "PostgreSQL function; parameters=3; outputParameters=2; parameterModes=customer_id:IN:integer,Id:OUT:integer,Name:OUT:text; dataType=record");

        Assert.Contains("Executes provider-bound function `public.customer_orders`", code);
        Assert.Contains("public sealed class CustomerOrdersResult", code);
        Assert.Contains("public int Id { get; set; }", code);
        Assert.Contains("public string Name { get; set; } = default!;", code);
        Assert.Contains("Task<List<CustomerOrdersResult>> CustomerOrdersAsync(CustomerOrdersParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", code);
        Assert.Contains("QueryUnchangedAsync<CustomerOrdersResult>", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>", code);
    }

    [Fact]
    public void ScaffoldContext_WithResultColumnNamesContainingColon_PreservesNameBeforeTypeMapping()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "GetOrderLines",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenantId:IN:int; resultColumns=Order:Id:int:0|Line:Name:nvarchar(40):1");

        Assert.Contains("public sealed class GetOrderLinesResult", code);
        Assert.Contains("public int OrderId { get; set; }", code);
        Assert.Contains("public string? LineName { get; set; }", code);
        Assert.DoesNotContain("public object? Order", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_colon_result_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithResultColumnNamesContainingSemicolon_PreservesMetadataFields()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "GetOrderLines",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenantId:IN:int; resultColumns=Order;Id:int:0|Line;Name:nvarchar(40):1");

        Assert.Contains("public sealed class GetOrderLinesResult", code);
        Assert.Contains("public int OrderId { get; set; }", code);
        Assert.Contains("public string? LineName { get; set; }", code);
        Assert.DoesNotContain("public object? Order", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_semicolon_result_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithResultColumnNamesContainingPipe_SplitsOnlyCompleteColumns()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "GetOrderLines",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenantId:IN:int; resultColumns=Order|Id:int:0|Line|Name:nvarchar(40):1");

        Assert.Contains("public sealed class GetOrderLinesResult", code);
        Assert.Contains("public int OrderId { get; set; }", code);
        Assert.Contains("public string? LineName { get; set; }", code);
        Assert.DoesNotContain("public object? Order", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_pipe_result_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithPostgresSetReturningScalarFunction_EmitsValueProjectionWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "customer_ids",
            "PostgreSQL function; parameters=1; outputParameters=0; callShape=table-valued-function; parameterModes=tenant_id:IN:integer; dataType=integer");

        Assert.Contains("Executes provider-bound function `public.customer_ids`", code);
        Assert.Contains("public sealed class CustomerIdsResult", code);
        Assert.Contains("public int Value { get; set; }", code);
        Assert.Contains("Task<List<CustomerIdsResult>> CustomerIdsAsync", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.Contains("return QueryUnchangedAsync<CustomerIdsResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.Contains("QueryUnchangedStreamAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.Contains("QueryUnchangedStreamAsync<CustomerIdsResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.Contains("IAsyncEnumerable<TResult> StreamCustomerIdsAsync<TResult>", code);
        Assert.DoesNotContain("QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", code);
    }

    [Fact]
    public void ScaffoldContext_WithMySqlFunction_EmitsSelectInvocationWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            null,
            "calculate_risk",
            "MySQL FUNCTION; parameters=1; outputParameters=0; parameterModes=customer_id:IN:int; dataType=int");

        Assert.Contains("Executes provider-bound FUNCTION `calculate_risk`", code);
        Assert.Contains("Task<TValue?> CalculateRiskValueAsync<TValue>(CalculateRiskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>", code);
    }

    [Fact]
    public void ScaffoldContext_WithMySqlUnsignedRoutineParameters_EmitsUnsignedClrTypes()
    {
        var code = InvokeScaffoldContextWithRoutine(
            null,
            "calculate_unsigned_risk",
            "MySQL FUNCTION; parameters=4; outputParameters=0; parameterModes=customer_id:IN:int unsigned,max_id:IN:bigint unsigned,rank:IN:smallint unsigned,flag:IN:tinyint unsigned; dataType=int");

        Assert.Contains("public uint? customer_id { get; init; }", code);
        Assert.Contains("public ulong? max_id { get; init; }", code);
        Assert.Contains("public ushort? rank { get; init; }", code);
        Assert.Contains("public byte? flag { get; init; }", code);
        Assert.DoesNotContain("public int? customer_id { get; init; }", code);
        Assert.DoesNotContain("public long? max_id { get; init; }", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_mysql_unsigned_routine_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
