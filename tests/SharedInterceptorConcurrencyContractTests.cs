using System;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the shared-interceptor concurrency contract: ONE interceptor instance registered in a
/// shared options object serves multiple contexts saving concurrently, and every callback
/// receives the context that actually executed the command — the interceptor pipeline holds no
/// per-command framework state that could cross wires between contexts. (The shipped
/// interceptor surface is stateless by design: callbacks receive the command and context per
/// call; user-held state is the user's concern.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SharedInterceptorConcurrencyContractTests
{
    [Table("SicRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private sealed class ContextRecordingInterceptor : BaseDbCommandInterceptor
    {
        public readonly ConcurrentBag<(DbContext Ctx, string Sql)> Writes = new();
        public ContextRecordingInterceptor() : base(NullLogger.Instance) { }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            Record(command, context);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public override InterceptionResult<int> NonQueryExecuting(DbCommand command, DbContext context)
        {
            Record(command, context);
            return InterceptionResult<int>.Continue();
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            Record(command, context);
            return base.ReaderExecuting(command, context);
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            Record(command, context);
            return base.ReaderExecutingAsync(command, context, ct);
        }

        private void Record(DbCommand command, DbContext context)
        {
            if (command.CommandText.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
                Writes.Add((context, command.CommandText));
        }
    }

    [Fact]
    public async Task One_interceptor_instance_serves_concurrent_contexts_with_the_right_context_argument()
    {
        var spy = new ContextRecordingInterceptor();

        DbContext MakeCtx(out SqliteConnection cn)
        {
            cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE SicRow_Test (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL)";
                cmd.ExecuteNonQuery();
            }
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
            opts.CommandInterceptors.Add(spy);
            return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        }

        var ctxA = MakeCtx(out var cnA);
        var ctxB = MakeCtx(out var cnB);
        await using (ctxA)
        await using (ctxB)
        using (cnA)
        using (cnB)
        {
            const int perContext = 25;
            var saveA = Task.Run(async () =>
            {
                for (var i = 1; i <= perContext; i++)
                {
                    ctxA.Add(new Row { Id = i, V = i });
                    await ctxA.SaveChangesAsync();
                }
            });
            var saveB = Task.Run(async () =>
            {
                for (var i = 1; i <= perContext; i++)
                {
                    ctxB.Add(new Row { Id = i, V = -i });
                    await ctxB.SaveChangesAsync();
                }
            });
            await Task.WhenAll(saveA, saveB);

            var byCtx = spy.Writes.GroupBy(w => w.Ctx).ToDictionary(g => g.Key, g => g.Count());
            Assert.Equal(2, byCtx.Count);
            Assert.True(byCtx.Values.All(c => c >= perContext),
                $"expected >= {perContext} intercepted writes per context, got [{string.Join(",", byCtx.Values)}]");

            // Both databases hold exactly their own writes — no cross-context interference.
            Assert.Equal(perContext, await ctxA.Query<Row>().CountAsync());
            Assert.Equal(perContext, await ctxB.Query<Row>().CountAsync());
            Assert.Equal(perContext, (await ctxA.Query<Row>().ToListAsync()).Count(r => r.V > 0));
            Assert.Equal(perContext, (await ctxB.Query<Row>().ToListAsync()).Count(r => r.V < 0));
        }
    }
}
