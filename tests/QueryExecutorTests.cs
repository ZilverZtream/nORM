using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies the synchronous query execution path in <see cref="QueryExecutor"/>.
///
/// Issue 2 fix: the sync path must NOT use GetAwaiter().GetResult() on async
/// methods.  Instead it uses truly synchronous ADO.NET calls (ExecuteReader /
/// reader.Read()) and synchronous materializers.
///
/// Tests here verify:
/// 1. Synchronous IEnumerable enumeration (GetEnumerator path) returns correct data.
/// 2. GroupJoin is handled synchronously without async materializer calls.
/// 3. The structural check: QueryExecutor.Materialize and MaterializeGroupJoin no
///    longer reference GetAwaiter().GetResult() — verified by inspecting the IL
///    of the compiled assembly (or by absence of Task-returning delegates being
///    invoked synchronously).
/// </summary>
public class QueryExecutorTests
{
    // ── entity types ──────────────────────────────────────────────────────────

    [Table("ExecOrder")]
    private class ExecOrder
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public int CustomerId { get; set; }
    }

    [Table("ExecCustomer")]
    private class ExecCustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static DbContext CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        cn.Execute("CREATE TABLE ExecOrder (Id INTEGER PRIMARY KEY, Name TEXT, Amount REAL, CustomerId INTEGER)");
        cn.Execute("CREATE TABLE ExecCustomer (Id INTEGER PRIMARY KEY, Name TEXT)");
        cn.Execute("INSERT INTO ExecCustomer VALUES (1,'Alice'), (2,'Bob')");
        cn.Execute("INSERT INTO ExecOrder VALUES (1,'Widget',9.99,1),(2,'Gadget',49.99,1),(3,'Doohickey',4.99,2)");
        return new DbContext(cn, new SqliteProvider());
    }

    // ── synchronous execution tests ───────────────────────────────────────────

    [Fact]
    public void Sync_enumeration_returns_all_rows()
    {
        // IEnumerable.GetEnumerator() triggers ExecuteSync which calls Materialize
        // (the truly-synchronous path, not the async path).
        using var ctx = CreateContext();
        var results = ctx.Query<ExecOrder>().ToList();   // ToList() uses IEnumerable, not async

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void Sync_enumeration_with_where_filters_correctly()
    {
        using var ctx = CreateContext();
        var results = ctx.Query<ExecOrder>()
            .Where(o => o.CustomerId == 1)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(1, r.CustomerId));
    }

    [Fact]
    public void Sync_enumeration_with_orderby_works()
    {
        using var ctx = CreateContext();
        var results = ctx.Query<ExecOrder>()
            .OrderBy(o => o.Amount)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Doohickey", results[0].Name);
        Assert.Equal("Widget", results[1].Name);
        Assert.Equal("Gadget", results[2].Name);
    }

    [Fact]
    public async Task Async_path_also_returns_correct_results()
    {
        // Confirm the async path still works and produces the same results.
        using var ctx = CreateContext();
        var results = await ctx.Query<ExecOrder>()
            .Where(o => o.Amount > 5)
            .ToListAsync();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void Sync_first_returns_single_entity()
    {
        using var ctx = CreateContext();
        var result = ctx.Query<ExecOrder>().First();
        Assert.NotNull(result);
    }

    [Fact]
    public void Sync_firstordefault_returns_null_when_no_rows()
    {
        using var ctx = CreateContext();
        var result = ctx.Query<ExecOrder>()
            .Where(o => o.Id == 9999)
            .FirstOrDefault();
        Assert.Null(result);
    }

    // ── structural check: no GetAwaiter().GetResult() in sync path ────────────

    [Fact]
    public void Materialize_sync_path_does_not_use_GetAwaiter_GetResult()
    {
        // Read the IL of QueryExecutor.Materialize via reflection and assert that
        // the method body does NOT call GetAwaiter() / GetResult() on a Task.
        //
        // We check for the presence of calls to Task.GetAwaiter() in the method's
        // IL bytes.  GetAwaiter().GetResult() on a Task always emits a callvirt to
        // System.Runtime.CompilerServices.TaskAwaiter.GetResult (or similar) after
        // getting the awaiter.  Its token appears in the raw IL.
        //
        // This test provides a regression guard: if someone re-introduces
        // .GetAwaiter().GetResult() it will fail immediately.

        var queryExecutorType = typeof(DbContext).Assembly
            .GetType("nORM.Query.QueryExecutor", throwOnError: true)!;

        var materializeMethod = queryExecutorType.GetMethod(
            "Materialize",
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;

        Assert.NotNull(materializeMethod);

        var ilBytes = materializeMethod.GetMethodBody()?.GetILAsByteArray();
        Assert.NotNull(ilBytes);

        // We look for the string "GetAwaiter" in the method's referenced tokens.
        // The simplest portable approach: check that "GetAwaiter" does NOT appear
        // in the names of methods called from within Materialize.
        bool callsGetAwaiter = DoesMethodCallGetAwaiter(materializeMethod);
        Assert.False(callsGetAwaiter,
            "QueryExecutor.Materialize must not use .GetAwaiter().GetResult() " +
            "on async methods — this causes thread-pool starvation / deadlocks.");
    }

    [Fact]
    public void MaterializeGroupJoin_sync_path_does_not_use_GetAwaiter_GetResult()
    {
        var queryExecutorType = typeof(DbContext).Assembly
            .GetType("nORM.Query.QueryExecutor", throwOnError: true)!;

        // MaterializeGroupJoin is a private method
        var method = queryExecutorType.GetMethod(
            "MaterializeGroupJoin",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        Assert.NotNull(method);

        bool callsGetAwaiter = DoesMethodCallGetAwaiter(method);
        Assert.False(callsGetAwaiter,
            "QueryExecutor.MaterializeGroupJoin must not use .GetAwaiter().GetResult() " +
            "on async methods.");
    }

    /// <summary>
    /// Inspects the IL metadata tokens of <paramref name="method"/> and checks
    /// whether any of the called methods is named "GetAwaiter".  This is a
    /// lightweight structural test that does not require running any code.
    /// </summary>
    private static bool DoesMethodCallGetAwaiter(MethodInfo method)
    {
        var body = method.GetMethodBody();
        if (body == null) return false;

        var module = method.Module;
        var il = body.GetILAsByteArray();
        if (il == null) return false;

        int i = 0;
        while (i < il.Length)
        {
            byte op = il[i++];

            // Extended opcode (0xFE prefix)
            if (op == 0xFE && i < il.Length)
            {
                i++; // skip second byte of extended opcode
            }

            // call / callvirt / ldftn = opcodes that reference method tokens
            // call    = 0x28, callvirt = 0x6F, ldftn = 0xFE 0x06, ldvirtftn = 0xFE 0x07
            // We check 0x28 (call) and 0x6F (callvirt)
            int opcode = il[i - 1];
            if ((opcode == 0x28 || opcode == 0x6F) && i + 3 < il.Length)
            {
                int token = il[i] | (il[i + 1] << 8) | (il[i + 2] << 16) | (il[i + 3] << 24);
                i += 4;
                try
                {
                    var callee = module.ResolveMethod(token);
                    if (callee != null && callee.Name == "GetAwaiter")
                        return true;
                }
                catch
                {
                    // Ignore resolution failures (generic contexts, etc.)
                }
            }
            else
            {
                // Skip operand bytes for other opcodes as needed —
                // for our purposes we accept a few false negatives from
                // branch / load instructions; we only care about call sites.
            }
        }

        return false;
    }
}

// ── tiny SQLite helper ─────────────────────────────────────────────────────────
file static class SqliteExtensions
{
    public static void Execute(this Microsoft.Data.Sqlite.SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
