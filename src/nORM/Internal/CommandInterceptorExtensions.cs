using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Enterprise;

#nullable enable

namespace nORM.Internal
{
    internal static class CommandInterceptorExtensions
    {
        public static async Task<int> ExecuteNonQueryWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteNonQueryAsync(ct);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.NonQueryExecutingAsync(command, ctx, ct);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.NonQueryExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = await command.ExecuteNonQueryAsync(ct);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.NonQueryExecutedAsync(command, ctx, result, sw.Elapsed, ct);
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct);
                }
                throw;
            }
        }

        public static async Task<object?> ExecuteScalarWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteScalarAsync(ct);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ScalarExecutingAsync(command, ctx, ct);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ScalarExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = await command.ExecuteScalarAsync(ct);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ScalarExecutedAsync(command, ctx, result, sw.Elapsed, ct);
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct);
                }
                throw;
            }
        }

        public static async Task<DbDataReader> ExecuteReaderWithInterceptionAsync(this DbCommand command, DbContext ctx, CommandBehavior behavior, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteReaderAsync(behavior, ct);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ReaderExecutingAsync(command, ctx, ct);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ReaderExecutedAsync(command, ctx, interception.Result!, TimeSpan.Zero, ct);
                    return interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var reader = await command.ExecuteReaderAsync(behavior, ct);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ReaderExecutedAsync(command, ctx, reader, sw.Elapsed, ct);
                }
                return reader;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct);
                }
                throw;
            }
        }
    }
}
