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
                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.NonQueryExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.NonQueryExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.NonQueryExecutedAsync(command, ctx, result, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        public static int ExecuteNonQueryWithInterception(this DbCommand command, DbContext ctx)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return command.ExecuteNonQuery();
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.NonQueryExecutingAsync(command, ctx, CancellationToken.None).GetAwaiter().GetResult();
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.NonQueryExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult();
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = command.ExecuteNonQuery();
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.NonQueryExecutedAsync(command, ctx, result, sw.Elapsed, CancellationToken.None).GetAwaiter().GetResult();
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.CommandFailedAsync(command, ctx, ex, CancellationToken.None).GetAwaiter().GetResult();
                }
                throw;
            }
        }

        public static async Task<object?> ExecuteScalarWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ScalarExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ScalarExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ScalarExecutedAsync(command, ctx, result, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        public static object? ExecuteScalarWithInterception(this DbCommand command, DbContext ctx)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return command.ExecuteScalar();
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.ScalarExecutingAsync(command, ctx, CancellationToken.None).GetAwaiter().GetResult();
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.ScalarExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult();
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = command.ExecuteScalar();
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.ScalarExecutedAsync(command, ctx, result, sw.Elapsed, CancellationToken.None).GetAwaiter().GetResult();
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.CommandFailedAsync(command, ctx, ex, CancellationToken.None).GetAwaiter().GetResult();
                }
                throw;
            }
        }

        public static async Task<DbDataReader> ExecuteReaderWithInterceptionAsync(this DbCommand command, DbContext ctx, CommandBehavior behavior, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ReaderExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ReaderExecutedAsync(command, ctx, interception.Result!, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var reader = await command.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ReaderExecutedAsync(command, ctx, reader, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return reader;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        public static DbDataReader ExecuteReaderWithInterception(this DbCommand command, DbContext ctx, CommandBehavior behavior)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return command.ExecuteReader(behavior);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.ReaderExecutingAsync(command, ctx, CancellationToken.None).GetAwaiter().GetResult();
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.ReaderExecutedAsync(command, ctx, interception.Result!, TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult();
                    return interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var reader = command.ExecuteReader(behavior);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.ReaderExecutedAsync(command, ctx, reader, sw.Elapsed, CancellationToken.None).GetAwaiter().GetResult();
                }
                return reader;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.CommandFailedAsync(command, ctx, ex, CancellationToken.None).GetAwaiter().GetResult();
                }
                throw;
            }
        }
    }
}
