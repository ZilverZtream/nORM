using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

namespace nORM.Tests;

public class CompiledJoinDiagnosticTest
{
    private readonly ITestOutputHelper _output;
    public CompiledJoinDiagnosticTest(ITestOutputHelper output) => _output = output;

    [Table("DiagUser")]
    public class DiagUser
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("DiagOrder")]
    public class DiagOrder
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
        public string ProductName { get; set; } = "";
    }

    [Fact]
    public async Task Compiled_vs_NonCompiled_Join_SQL()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE DiagUser (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
            CREATE TABLE DiagOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, UserId INTEGER NOT NULL, Amount REAL NOT NULL, ProductName TEXT NOT NULL);
            INSERT INTO DiagUser (Name) VALUES ('Alice'), ('Bob');
            INSERT INTO DiagOrder (UserId, Amount, ProductName) VALUES (1, 200.0, 'Widget'), (2, 50.0, 'Gadget');
        ";
        cmd.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Non-compiled: capture SQL via interceptor
        var interceptor = new SqlCapture();
        ctx.Options.CommandInterceptors.Add(interceptor);

        var nonCompiled = await NormAsyncExtensions.ToListAsync(
            ctx.Query<DiagUser>()
                .Join(ctx.Query<DiagOrder>(), u => u.Id, o => o.UserId,
                      (u, o) => new { u.Name, o.Amount, o.ProductName })
                .Where(x => x.Amount > 100)
                .Take(50));

        var nonCompiledSql = interceptor.LastSql;
        _output.WriteLine($"Non-compiled SQL: {nonCompiledSql}");
        _output.WriteLine($"Non-compiled result count: {nonCompiled.Count}");

        // Compiled
        var compiled = Norm.CompileQuery<DbContext, int, object>((c, amount) =>
            c.Query<DiagUser>()
                .Join(c.Query<DiagOrder>(), u => u.Id, o => o.UserId,
                      (u, o) => new { u.Name, o.Amount, o.ProductName })
                .Where(x => x.Amount > amount)
                .Take(50));

        interceptor.LastSql = "";
        var compiledResult = await compiled(ctx, 100);
        var compiledSql = interceptor.LastSql;
        _output.WriteLine($"Compiled SQL: {compiledSql}");
        _output.WriteLine($"Compiled result count: {compiledResult.Count}");

        Assert.True(nonCompiled.Count > 0);
        Assert.True(compiledResult.Count > 0);

        // Perf comparison: run each 100 times
        // Warmup
        for (int i = 0; i < 10; i++)
        {
            await NormAsyncExtensions.ToListAsync(
                ctx.Query<DiagUser>()
                    .Join(ctx.Query<DiagOrder>(), u => u.Id, o => o.UserId,
                          (u, o) => new { u.Name, o.Amount, o.ProductName })
                    .Where(x => x.Amount > 100)
                    .Take(50));
            await compiled(ctx, 100);
        }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 100; i++)
        {
            await NormAsyncExtensions.ToListAsync(
                ctx.Query<DiagUser>()
                    .Join(ctx.Query<DiagOrder>(), u => u.Id, o => o.UserId,
                          (u, o) => new { u.Name, o.Amount, o.ProductName })
                    .Where(x => x.Amount > 100)
                    .Take(50));
        }
        var nonCompiledTime = sw.Elapsed;
        _output.WriteLine($"Non-compiled 100x: {nonCompiledTime.TotalMilliseconds:F2}ms ({nonCompiledTime.TotalMilliseconds / 100:F3}ms/op)");

        sw.Restart();
        for (int i = 0; i < 100; i++)
        {
            await compiled(ctx, 100);
        }
        var compiledTime = sw.Elapsed;
        _output.WriteLine($"Compiled 100x: {compiledTime.TotalMilliseconds:F2}ms ({compiledTime.TotalMilliseconds / 100:F3}ms/op)");
    }

    private class SqlCapture : IDbCommandInterceptor
    {
        public string LastSql { get; set; } = "";
        public Task<InterceptionResult<int>> NonQueryExecutingAsync(System.Data.Common.DbCommand command, DbContext context, System.Threading.CancellationToken ct)
        { LastSql = command.CommandText; return Task.FromResult(InterceptionResult<int>.Continue()); }
        public Task NonQueryExecutedAsync(System.Data.Common.DbCommand command, DbContext context, int result, TimeSpan duration, System.Threading.CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(System.Data.Common.DbCommand command, DbContext context, System.Threading.CancellationToken ct)
        { LastSql = command.CommandText; return Task.FromResult(InterceptionResult<object?>.Continue()); }
        public Task ScalarExecutedAsync(System.Data.Common.DbCommand command, DbContext context, object? result, TimeSpan duration, System.Threading.CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<System.Data.Common.DbDataReader>> ReaderExecutingAsync(System.Data.Common.DbCommand command, DbContext context, System.Threading.CancellationToken ct)
        { LastSql = command.CommandText; return Task.FromResult(InterceptionResult<System.Data.Common.DbDataReader>.Continue()); }
        public Task ReaderExecutedAsync(System.Data.Common.DbCommand command, DbContext context, System.Data.Common.DbDataReader reader, TimeSpan duration, System.Threading.CancellationToken ct) => Task.CompletedTask;
        public Task CommandFailedAsync(System.Data.Common.DbCommand command, DbContext context, Exception exception, System.Threading.CancellationToken ct) => Task.CompletedTask;
    }
}
